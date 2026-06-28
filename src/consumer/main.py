
import signal
import time
from datetime import datetime

from pydantic import ValidationError

from src.consumer.config import settings
from src.consumer.db import DatabaseManager
from src.consumer.kafka_client import KafkaConsumerClient, KafkaProducerClient
from src.consumer.models import BlockModel, TokenTransferModel, TransactionModel
from src.consumer.redis_client import RedisManager

from src.consumer.logger import logger

running = True


def signal_handler(sig, frame):
    global running
    logger.info("Shutdown signal received. Starting graceful shutdown.")
    running = False


def build_whale_alert_payload(tx: TransactionModel) -> dict:
    return {
        "hash": tx.hash,
        "timestamp": tx.timestamp.isoformat(),
        "from_address": tx.from_address,
        "to_address": tx.to_address,
        "value": tx.value,
        "value_eth": tx.value / 10**18,
    }


def publish_whale_alerts(producer: KafkaProducerClient, transactions: list[TransactionModel]) -> int:
    whale_transactions = [
        tx for tx in transactions
        if tx.value >= settings.WHALE_THRESHOLD_WEI
    ]

    for tx in whale_transactions:
        producer.send_message(
            settings.WHALE_ALERTS_TOPIC,
            tx.hash,
            build_whale_alert_payload(tx),
        )

    if whale_transactions:
        producer.flush()
        logger.info("Published %s whale alert(s)", len(whale_transactions))

    return len(whale_transactions)

def build_token_event_payload(token_tx: TokenTransferModel) -> dict:
    return {
        "address": token_tx.token_address,
        "timestamp": token_tx.timestamp.isoformat() if token_tx.timestamp else None,
        "from_address": token_tx.from_address,
        "to_address": token_tx.to_address,
        "value": token_tx.value,
        "block_number": token_tx.block_number,
        "transaction_hash": token_tx.transaction_hash
    }

def publish_token_events(producer: KafkaProducerClient, token_transfers: list[TokenTransferModel]) -> int:
    for tx in token_transfers:
        producer.send_message(
            settings.TOKEN_EVENTS_TOPIC,
            tx.token_address,
            build_token_event_payload(tx)
        )
    if token_transfers:
        producer.flush()
        logger.info("Published %s token transfer event(s)", len(token_transfers))
    return len(token_transfers)


def flush_batches(
    db_manager: DatabaseManager,
    kafka_client: KafkaConsumerClient,
    producer_client: KafkaProducerClient,
    tx_batch: list[TransactionModel],
    token_batch: list[TokenTransferModel],
    reason: str = "batch",
) -> None:
    conn = db_manager.get_connection()
    try:
        conn.autocommit = False

        if tx_batch:
            db_manager.insert_transactions_batch(conn, tx_batch)
        if token_batch:
            db_manager.insert_token_transfers_batch(conn, token_batch)

        conn.commit()
        publish_whale_alerts(producer_client, tx_batch)
        publish_token_events(producer_client, token_batch)
        kafka_client.commit()

        logger.info(
            "%s commit complete: transactions=%s token_transfers=%s",
            reason,
            len(tx_batch),
            len(token_batch),
        )
        tx_batch.clear()
        token_batch.clear()
    except Exception:
        conn.rollback()
        logger.exception("%s commit failed; Kafka offsets were not committed", reason)
        raise
    finally:
        db_manager.release_connection(conn)


def enrich_token_timestamp(token_model: TokenTransferModel, redis_manager: RedisManager) -> bool:
    if token_model.timestamp is not None:
        return True

    cached_time = redis_manager.get_block_timestamp(token_model.block_number)
    retries = 0
    while not cached_time and retries < 10:
        time.sleep(0.2)
        cached_time = redis_manager.get_block_timestamp(token_model.block_number)
        retries += 1

    if not cached_time:
        logger.warning("Missing timestamp for block %s; skipping token transfer", token_model.block_number)
        return False

    token_model.timestamp = datetime.fromisoformat(cached_time)
    return True


def main():
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting Ethereum Kafka consumer")

    try:
        db_manager = DatabaseManager()
    except Exception:
        logger.critical("Failed to initialize database manager", exc_info=True)
        return

    kafka_client = KafkaConsumerClient()
    producer_client = KafkaProducerClient()
    kafka_client.subscribe()

    try:
        redis_manager = RedisManager()
    except Exception:
        logger.critical("Failed to initialize Redis manager", exc_info=True)
        producer_client.close()
        kafka_client.close()
        db_manager.close_all()
        return

    tx_batch: list[TransactionModel] = []
    token_batch: list[TokenTransferModel] = []

    try:
        while running:
            topic, data = kafka_client.poll(timeout=1.0)

            if data is not None:
                try:
                    if topic == "blocks":
                        block_model = BlockModel(**data)
                        redis_manager.cache_block_timestamp(
                            block_model.number,
                            block_model.timestamp.isoformat(),
                        )
                    elif topic == "transactions":
                        tx_batch.append(TransactionModel(**data))
                    elif topic == "token_transfers":
                        token_model = TokenTransferModel(**data)
                        if enrich_token_timestamp(token_model, redis_manager):
                            token_batch.append(token_model)
                except ValidationError:
                    logger.exception("Message validation failed")
                    continue

            if len(tx_batch) + len(token_batch) >= settings.BATCH_SIZE:
                try:
                    flush_batches(db_manager, kafka_client, producer_client, tx_batch, token_batch)
                except Exception:
                    logger.error("Batch flush failed; records will remain buffered for retry")

    except KeyboardInterrupt:
        pass
    finally:
        if tx_batch or token_batch:
            try:
                flush_batches(
                    db_manager,
                    kafka_client,
                    producer_client,
                    tx_batch,
                    token_batch,
                    reason="shutdown",
                )
            except Exception:
                logger.exception("Failed to flush remaining records during shutdown")

        try:
            producer_client.close()
        except Exception:
            logger.exception("Failed to close Kafka producer cleanly")
        finally:
            kafka_client.close()
            db_manager.close_all()
        logger.info("Consumer shutdown complete")


if __name__ == "__main__":
    main()
