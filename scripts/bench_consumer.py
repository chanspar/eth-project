"""
C (Consumer Throughput) 벤치마크 — 3가지 모드 비교
실행: uv run python scripts/bench_consumer.py
사전 조건: docker-compose (Kafka, PostgreSQL) 가 떠 있어야 합니다.
"""

import json, time, hashlib, random, logging, math
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka.admin import AdminClient
from psycopg2 import pool
from psycopg2.extras import execute_values
from pydantic import ValidationError
from src.consumer.models import TransactionModel, TokenTransferModel

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ── 설정 ──
KAFKA_BROKER = "localhost:9094"
POSTGRES_DSN = "postgresql://user:password@localhost:5432/eth_data"
BENCH_TOPIC  = "bench-test"
NUM_MESSAGES = 10_000
TARGET_T     = 1_500


# ============================================================
# 1. 가짜 데이터 생성 & Kafka에 밀어넣기
# ============================================================
def make_fake(i: int) -> tuple[str, dict]:
    h = hashlib.sha256(f"{i}_{time.time()}".encode()).hexdigest()
    if i % 2 == 0:
        return f"{BENCH_TOPIC}-tx", {
            "hash": f"0x{h}", "block_timestamp": int(time.time()),
            "from_address": f"0x{'a'*40}", "to_address": f"0x{'b'*40}",
            "value": random.randint(0, 10**18), "gas_price": random.randint(10**9, 10**11),
        }
    else:
        return f"{BENCH_TOPIC}-token", {
            "transaction_hash": f"0x{h}", "log_index": i % 500,
            "block_timestamp": int(time.time()), "block_number": 20_000_000 + i,
            "token_address": f"0x{'c'*40}", "from_address": f"0x{'d'*40}",
            "to_address": f"0x{'e'*40}", "value": random.randint(0, 10**18),
        }

def produce_test_data():
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})
    for i in range(NUM_MESSAGES):
        topic, data = make_fake(i)
        producer.produce(topic, json.dumps(data).encode())
    producer.flush()
    logger.info(f"📦 {NUM_MESSAGES:,}건 produce 완료")


# ============================================================
# 2. Consumer 벤치마크
#    - use_db=False → Kafka poll + Pydantic만 (DB 없음)
#    - use_db=True  → Kafka poll + Pydantic + DB INSERT
#    - batch_size   → 1이면 단건 INSERT, 100이면 배치 INSERT
# ============================================================
def bench(use_db: bool, batch_size: int) -> float:
    db = pool.ThreadedConnectionPool(1, 10, dsn=POSTGRES_DSN) if use_db else None

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": f"bench-db{use_db}-batch{batch_size}-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([f"{BENCH_TOPIC}-tx", f"{BENCH_TOPIC}-token"])

    tx_buf, token_buf = [], []
    count = empty = 0
    start = time.perf_counter()

    while count < NUM_MESSAGES:
        msg = consumer.poll(1.0)
        if msg is None:
            empty += 1
            if empty > 5: break
            continue
        if msg.error():
            continue

        empty = 0
        raw = json.loads(msg.value().decode())

        try:
            if msg.topic().endswith("-tx"):
                tx_buf.append(TransactionModel(**raw))
            else:
                token_buf.append(TokenTransferModel(**raw))
            count += 1
        except ValidationError:
            continue

        # DB 사용 시: 배치 사이즈 도달하면 flush
        if use_db and (len(tx_buf) + len(token_buf) >= batch_size):
            _flush(db, tx_buf, token_buf)
            consumer.commit()
            tx_buf.clear(); token_buf.clear()

    # 잔여 flush
    if use_db and (tx_buf or token_buf):
        _flush(db, tx_buf, token_buf)

    elapsed = time.perf_counter() - start
    consumer.close()
    if db: db.closeall()
    return count / elapsed if elapsed > 0 else 0


def _flush(db, tx_buf, token_buf):
    conn = db.getconn()
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if tx_buf:
                execute_values(cur, """
                    INSERT INTO transactions (hash, timestamp, from_address, to_address, value, gas_price)
                    VALUES %s ON CONFLICT (hash, timestamp) DO NOTHING
                """, [(t.hash, t.timestamp, t.from_address, t.to_address, t.value, t.gas_price) for t in tx_buf])
            if token_buf:
                execute_values(cur, """
                    INSERT INTO token_transfers (transaction_hash, log_index, timestamp, token_address, from_address, to_address, value)
                    VALUES %s ON CONFLICT (transaction_hash, log_index, timestamp) DO NOTHING
                """, [(t.transaction_hash, t.log_index, t.timestamp, t.token_address, t.from_address, t.to_address, t.value) for t in token_buf])
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"DB INSERT 실패: {e}")
    finally:
        db.putconn(conn)


# ============================================================
# 3. 실행: 3가지 모드 비교
# ============================================================
if __name__ == "__main__":
    results = {}

    tests = [
        ("Pydantic만 (DB 없음)",    False, 1),
        ("DB 단건 INSERT (batch=1)", True,  1),
        ("DB 배치 INSERT (batch=100)", True, 100),
    ]

    for name, use_db, batch_size in tests:
        produce_test_data()
        logger.info(f"⏱️  테스트: {name}")
        results[name] = bench(use_db, batch_size)
        logger.info(f"   → {results[name]:,.0f} msg/s\n")

    # 벤치마크 임시 토픽 자동 삭제
    logger.info("🧹 테스트 완료: 임시 벤치마크 토픽 삭제 중...")
    admin = AdminClient({"bootstrap.servers": KAFKA_BROKER})
    fs = admin.delete_topics([f"{BENCH_TOPIC}-tx", f"{BENCH_TOPIC}-token"])
    for topic, f in fs.items():
        try:
            f.result()
            logger.info(f"   ↳ 토픽 '{topic}' 삭제 성공")
        except Exception as e:
            logger.warning(f"   ↳ 토픽 '{topic}' 삭제 실패: {e}")
    print()

    # 결과 비교
    logger.info("=" * 58)
    logger.info(f"  {'모드':<28} {'처리량':>10}  {'T/C':>6}  {'파티션':>4}")
    logger.info("-" * 58)
    for name, throughput in results.items():
        partitions = max(1, math.ceil(TARGET_T / throughput)) if throughput > 0 else "N/A"
        ratio = f"{TARGET_T / throughput:.2f}" if throughput > 0 else "N/A"
        logger.info(f"  {name:<28} {throughput:>8,.0f}/s  {ratio:>6}  {partitions:>4}")
    logger.info("=" * 58)
    logger.info(f"  목표 T = {TARGET_T:,} msg/s")
    logger.info(f"  🎯 실제 C = DB 배치 INSERT 결과 기준으로 파티션 수 결정")
