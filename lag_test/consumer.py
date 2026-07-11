import time
import json
import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, KafkaError

# =====================================================================
# 설정 제어 스위치
# =====================================================================
# True  : 건별 INSERT + TCP 딜레이 시뮬레이션 (초당 ~60건 처리 -> Lag 우상향)
# False : 벌크 INSERT (execute_values) 시뮬레이션 (초당 수천 건 처리 -> Lag 0 수렴)
SLOW_MODE = False  

BATCH_SIZE = 100
DB_DSN = "dbname=lag_db user=user password=password host=localhost port=5432"
KAFKA_CONF = {
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'lag-test-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False  # 수동 오프셋 커밋으로 제어
}

def init_db():
    """테스트용 DB 테이블 생성"""
    conn = psycopg2.connect(DB_DSN)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS test_transactions (
            hash VARCHAR(66) PRIMARY KEY,
            timestamp INT,
            from_address VARCHAR(42),
            to_address VARCHAR(42),
            value NUMERIC,
            gas_price NUMERIC
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Postgres [test_transactions] 테이블 준비 완료")

def main():
    try:
        init_db()
    except Exception as e:
        print(f"❌ DB 연결 실패: {e}\nDocker Compose가 작동 중인지, 포트 5432가 열려있는지 확인해주세요.")
        return

    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(['transactions'])
    
    mode_str = "SLOW MODE (건별 INSERT)" if SLOW_MODE else "FAST BATCH MODE (execute_values)"
    print(f"🎯 Consumer 시작 완료. 모드: [{mode_str}]")
    
    conn = psycopg2.connect(DB_DSN)
    batch = []
    processed_count = 0
    total_processed = 0
    start_time = time.time()
    
    try:
        while True:
            # 0.5초 대기하며 메시지 수신
            msg = consumer.poll(0.5)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ 카프카 에러: {msg.error()}")
                    break
            
            data = json.loads(msg.value().decode('utf-8'))
            
            if SLOW_MODE:
                # -----------------------------------------------------------
                # 1. 건별 INSERT 시뮬레이션
                # -----------------------------------------------------------
                cur = conn.cursor()
                cur.execute("""
                    INSERT INTO test_transactions (hash, timestamp, from_address, to_address, value, gas_price)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (hash) DO NOTHING;
                """, (data['hash'], data['timestamp'], data['from_address'], data['to_address'], data['value'], data['gas_price']))
                conn.commit()
                cur.close()
                
                # TCP 연결 왕복 + 파싱 시간(약 15ms)의 물리적 오버헤드를 sleep으로 재현
                time.sleep(0.015)
                
                processed_count += 1
                total_processed += 1
                
                # 100건마다 오프셋 커밋
                if processed_count % 100 == 0:
                    consumer.commit(asynchronous=False)
                    
            else:
                # -----------------------------------------------------------
                # 2. 배치 INSERT (execute_values) 시뮬레이션
                # -----------------------------------------------------------
                batch.append((
                    data['hash'], data['timestamp'], data['from_address'], data['to_address'], data['value'], data['gas_price']
                ))
                
                if len(batch) >= BATCH_SIZE:
                    cur = conn.cursor()
                    execute_values(cur, """
                        INSERT INTO test_transactions (hash, timestamp, from_address, to_address, value, gas_price)
                        VALUES %s
                        ON CONFLICT (hash) DO NOTHING;
                    """, batch)
                    conn.commit()
                    cur.close()
                    
                    # 배치 단위 완료 후 한 번만 커밋
                    consumer.commit(asynchronous=False)
                    processed_count += len(batch)
                    total_processed += len(batch)
                    batch.clear()
            
            # 3초마다 소비 속도 측정
            now = time.time()
            if now - start_time >= 3:
                duration = now - start_time
                speed = processed_count / duration
                print(f"📥 [Consumer] Total: {total_processed} rec. Speed: {speed:.2f} rec/sec")
                processed_count = 0
                start_time = now

    except KeyboardInterrupt:
        print("\n🛑 사용자에 의해 컨슈머가 중지되었습니다.")
    finally:
        # 종료 전 배치에 남은 잔여 데이터 삽입
        if batch and not SLOW_MODE:
            cur = conn.cursor()
            execute_values(cur, """
                INSERT INTO test_transactions (hash, timestamp, from_address, to_address, value, gas_price)
                VALUES %s
                ON CONFLICT (hash) DO NOTHING;
            """, batch)
            conn.commit()
            cur.close()
            consumer.commit(asynchronous=False)
            print(f"💾 잔여 데이터 {len(batch)}건 적재 완료.")
            
        consumer.close()
        conn.close()
        print("Consumer 연결이 정상적으로 해제되었습니다.")

if __name__ == "__main__":
    main()
