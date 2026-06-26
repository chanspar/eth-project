import io
from confluent_kafka import Producer
from fastavro import schemaless_writer, parse_schema

# 1. Avro 스키마 정의
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

# 2. 프로듀서 초기화 (호스트 PC 포트인 9094 사용)
producer = Producer({'bootstrap.servers': 'localhost:9094'})

events = [
    {"user_id": "101", "action": "결제", "amount": 50000},
    {"user_id": "102", "action": "주문", "amount": 30000},
    {"user_id": "101", "action": "취소", "amount": 50000},
    {"user_id": "101", "action": "환불", "amount": 50000}
]

for event in events:
    # 3. Avro 바이너리로 직렬화 (메시지 크기를 줄이기 위해 스키마 헤더를 제외한 schemaless_writer 사용)
    bytes_io = io.BytesIO()
    schemaless_writer(bytes_io, parsed_schema, event)
    serialized_data = bytes_io.getvalue()
    
    # 4. 전송
    producer.produce(
        topic="user-events-avro",
        key=event['user_id'].encode('utf-8'),
        value=serialized_data
    )

producer.flush()
print("Avro 메시지 발행 완료!")
