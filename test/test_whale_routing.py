import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock

from src.consumer.config import settings
from src.consumer.main import flush_batches, publish_whale_alerts


def make_tx(hash_value, value):
    return SimpleNamespace(
        hash=hash_value,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        from_address="0xfrom",
        to_address="0xto",
        value=value,
        gas_price=1,
    )


class WhaleRoutingTest(unittest.TestCase):
    def test_publish_whale_alerts_only_sends_transactions_at_threshold(self):
        producer = Mock()
        small_tx = make_tx("small", settings.WHALE_THRESHOLD_WEI - 1)
        whale_tx = make_tx("whale", settings.WHALE_THRESHOLD_WEI)

        published = publish_whale_alerts(producer, [small_tx, whale_tx])

        self.assertEqual(published, 1)
        producer.send_message.assert_called_once()
        topic, key, payload = producer.send_message.call_args.args
        self.assertEqual(topic, settings.WHALE_ALERTS_TOPIC)
        self.assertEqual(key, "whale")
        self.assertEqual(payload["value_eth"], 100)
        producer.flush.assert_called_once()

    def test_flush_batches_commits_offsets_after_db_commit_and_alert_flush(self):
        db_manager = Mock()
        kafka_client = Mock()
        producer = Mock()
        conn = Mock()
        db_manager.get_connection.return_value = conn
        tx_batch = [make_tx("whale", settings.WHALE_THRESHOLD_WEI)]
        token_batch = []

        flush_batches(db_manager, kafka_client, producer, tx_batch, token_batch)

        db_manager.insert_transactions_batch.assert_called_once()
        conn.commit.assert_called_once()
        producer.flush.assert_called_once()
        kafka_client.commit.assert_called_once()
        self.assertEqual(tx_batch, [])
        db_manager.release_connection.assert_called_once_with(conn)

    def test_flush_batches_does_not_commit_offsets_when_alert_publish_fails(self):
        db_manager = Mock()
        kafka_client = Mock()
        producer = Mock()
        producer.send_message.side_effect = RuntimeError("producer down")
        conn = Mock()
        db_manager.get_connection.return_value = conn
        tx_batch = [make_tx("whale", settings.WHALE_THRESHOLD_WEI)]

        with self.assertRaises(RuntimeError):
            flush_batches(db_manager, kafka_client, producer, tx_batch, [])

        conn.commit.assert_called_once()
        kafka_client.commit.assert_not_called()
        db_manager.release_connection.assert_called_once_with(conn)

    def test_producer_client_raises_error_on_delivery_failure(self):
        from src.consumer.kafka_client import KafkaProducerClient
        from unittest.mock import patch
        
        with patch('src.consumer.kafka_client.Producer') as mock_producer_class:
            mock_producer = mock_producer_class.return_value
            
            def mock_produce(topic, key, value, on_delivery):
                class DummyError:
                    def __str__(self):
                        return "delivery failed"
                on_delivery(DummyError(), None)
                
            mock_producer.produce.side_effect = mock_produce
            mock_producer.flush.return_value = 0
            
            client = KafkaProducerClient()
            client.send_message("test-topic", "test-key", {"data": "test"})
            
            with self.assertRaises(RuntimeError) as ctx:
                client.flush()
            
            self.assertIn("Kafka message delivery failed: delivery failed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
