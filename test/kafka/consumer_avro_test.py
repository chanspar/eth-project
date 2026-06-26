import io
from confluent_kafka import Consumer
from fastavro import schemaless_reader, parse_schema

# 1. 수신 데이터를 해석할 Avro 스키마 정의 (프로듀서와 일치해야 함)
schema_dict = {
    "type": "record",
    "name": "UserEvent",
    "fields": [
        {"name": "user_id", "type": "string"},
        {"name": "action", "type": "string"},
        {"name": "amount", "type": "int"}
    ]
}
parsed_schema = parse_schema(schema_dict)

# 2. 컨슈머 초기화 (호스트 PC 포트인 9094 사용)
consumer = Consumer({
    'bootstrap.servers': 'localhost:9094',
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest'
})

# 3. 토픽 구독
consumer.subscribe(['user-events-avro'])

print("Avro 컨슈머 시작... 메시지를 기다리는 중 (종료하려면 Ctrl+C)")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print('Error:', msg.error())
        else:
            # 4. Key 디코딩 (문자열)
            key = msg.key().decode('utf-8') if msg.key() else None
            
            # 5. Value 역직렬화 (Avro 바이너리 -> 파이썬 딕셔너리)
            bytes_io = io.BytesIO(msg.value())
            data = schemaless_reader(bytes_io, parsed_schema)
            
            print(f"[파티션{msg.partition()}] 유저={key}, 이벤트={data}")

except KeyboardInterrupt:
    print("\n컨슈머 종료 중...")
finally:
    consumer.close()
