from confluent_kafka import Consumer
import json

consumer = Consumer({
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'user-event-consumerv2',
    'auto.offset.reset': 'earliest'
})

consumer.subscribe(['user-events'])
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print('Error:', msg.error())
        else:
            key = msg.key().decode('utf-8') if msg.key() else None
            data = json.loads(msg.value().decode('utf-8'))
            print(f"[파티션{msg.partition()}] 유저={key}, 이벤트={data}")
except KeyboardInterrupt:
    print("\n컨슈머 종료 중...")
finally:
    # 종료 시 컨슈머를 안전하게 닫아 서버(카프카) 리소스를 정리합니다.
    consumer.close()
