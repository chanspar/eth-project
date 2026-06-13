from confluent_kafka import Producer
import json

producer = Producer({'bootstrap.servers': 'localhost:9094'})

events = [
	{"user_id": "101", "action": "결제", "amount": 50000},
	{"user_id": "102", "action": "주문", "amount": 30000},
	{"user_id": "101", "action": "취소", "amount": 50000},
	{"user_id": "101", "action": "환불", "amount": 50000}
]

for event in events:
	producer.produce(
		topic="user-events",
		key=event['user_id'].encode('utf-8'),
		value=json.dumps(event).encode('utf-8')
	)

producer.flush()
print("발행 완")
