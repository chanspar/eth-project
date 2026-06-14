import logging
import json
from confluent_kafka import Consumer, KafkaError, KafkaException
from .config import settings

logger = logging.getLogger(__name__)

class KafkaConsumerClient:
    def __init__(self):
        conf = {
            'bootstrap.servers': settings.KAFKA_BROKER,
            'group.id': settings.CONSUMER_GROUP_ID,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # 수동 커밋 강제 (멱등성 보장)
        }
        self.consumer = Consumer(conf)
        # blocks 토픽을 추가하여 블록 생성 시간을 읽어옵니다.
        self.topics = ["blocks", "transactions", "token_transfers"]

    def subscribe(self):
        self.consumer.subscribe(self.topics)
        logger.info(f"토픽 구독 완료: {self.topics}")

    def poll(self, timeout=1.0):
        msg = self.consumer.poll(timeout=timeout)
        if msg is None:
            return None, None
            
        err = msg.error()
        if err is not None:
            if err.code() == KafkaError._PARTITION_EOF:
                return None, None # 토픽, 데이터라 None, None
            else:
                logger.error(f"카프카 에러: {err}")
                raise KafkaException(err)
                
        try:
            raw_val = msg.value()
            if raw_val is None:
                # 카프카의 Tombstone 메시지(value가 없는 삭제 요청 등) 처리
                return msg.topic(), None
                
            val = raw_val.decode('utf-8')
            data = json.loads(val)
            return msg.topic(), data
        except Exception as e:
            logger.error(f"메시지 파싱 에러: {e}")
            return None, None

    def commit(self):
        self.consumer.commit()

    def close(self):
        self.consumer.close()
        logger.info("Kafka Consumer 연결 종료 완료")
