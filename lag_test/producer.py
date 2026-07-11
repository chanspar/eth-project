import time
import json
import uuid
from confluent_kafka import Producer

# 로컬 Docker Kafka 외부 포트인 9094로 연결합니다.
conf = {
    'bootstrap.servers': 'localhost:9094',
    'client.id': 'lag-test-producer'
}

producer = Producer(conf)
topic = 'transactions'

print("🚀 Starting simulated ETH Transaction Producer (Target: 750 tx/sec)...")
messages_sent = 0
total_sent = 0
start_time = time.time()

try:
    while True:
        loop_start = time.time()
        
        # 0.1초 동안 75건을 보내어 초당 750건(TPS)을 맞춥니다.
        for _ in range(75):
            payload = {
                "hash": "0x" + uuid.uuid4().hex,
                "timestamp": int(time.time()),
                "from_address": "0x" + uuid.uuid4().hex[:40],
                "to_address": "0x" + uuid.uuid4().hex[:40],
                "value": 1250000000000000000,  # 1.25 ETH (Wei)
                "gas_price": 18000000000       # 18 Gwei
            }
            # 비동기로 메시지 큐에 삽입
            producer.produce(topic, value=json.dumps(payload).encode('utf-8'))
            messages_sent += 1
            total_sent += 1

        # 카프카 딜리버리 콜백 이벤트 트리거
        producer.poll(0)

        # 0.1초 주기에 맞춰 대기 시간을 계산합니다.
        elapsed = time.time() - loop_start
        sleep_time = 0.1 - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

        # 3초마다 현재 속도를 출력합니다.
        now = time.time()
        if now - start_time >= 3:
            duration = now - start_time
            tps = messages_sent / duration
            print(f"📈 [Producer] Sent {total_sent} total messages. Current TPS: {tps:.2f}")
            messages_sent = 0
            start_time = now

except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    # 큐에 남아 있는 메시지 전송 보장
    producer.flush()
    print("Producer stopped.")
