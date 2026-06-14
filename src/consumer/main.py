import signal
import logging
import time
from datetime import datetime
from src.consumer.config import settings
from src.consumer.db import DatabaseManager
from src.consumer.kafka_client import KafkaConsumerClient
from src.consumer.redis_client import RedisManager
from src.consumer.models import BlockModel, TransactionModel, TokenTransferModel
from pydantic import ValidationError

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

running = True

def signal_handler(sig, frame):
    global running
    logger.info("종료 시그널 수신. 우아한 종료(Graceful Shutdown)를 시작합니다...")
    running = False

def main():
    signal.signal(signal.SIGINT, signal_handler) # Ctrl + C 시그널 핸들러
    # 리눅스 서버나 도커(Docker) 같은 시스템이 프로세스를 종료할 때 얌전히 꺼지라고 보내는 표준 종료 신호(예: 리눅스의 kill 명령어)입니다.
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("시니어 레벨 Kafka Consumer 애플리케이션 시작")

    # 1. DB 매니저 초기화 (Connection Pool)
    try:
        db_manager = DatabaseManager()
    except Exception as e:
        logger.critical("DB 매니저 초기화 실패. 종료합니다.")
        return

    # 2. Kafka 클라이언트 초기화 및 구독
    kafka_client = KafkaConsumerClient()
    kafka_client.subscribe()

    # 3. Redis 매니저 초기화 (캐싱용)
    try:
        redis_manager = RedisManager()
    except Exception as e:
        logger.critical("Redis 매니저 초기화 실패. 종료합니다.")
        return

    # 인메모리 배치 버퍼
    tx_batch = []
    token_batch = []
    
    try:
        while running:
            # 1초 대기하며 카프카에서 메시지 폴링
            topic, data = kafka_client.poll(timeout=1.0)
            
            if data is not None:
                # Pydantic 모델을 이용한 데이터 검증 및 형변환
                try:
                    if topic == "blocks":
                        block_model = BlockModel(**data)
                        # Redis에 타임스탬프 캐싱 (블록번호 -> 시간문자열)
                        redis_manager.cache_block_timestamp(block_model.number, block_model.timestamp.isoformat())
                    elif topic == "transactions":
                        tx_model = TransactionModel(**data)
                        tx_batch.append(tx_model)
                    elif topic == "token_transfers":
                        token_model = TokenTransferModel(**data)
                        
                        # 타임스탬프 결측치 보완 로직 (Redis 캐시 활용)
                        if token_model.timestamp is None:
                            cached_time = redis_manager.get_block_timestamp(token_model.block_number)
                            # 카프카 토픽 간의 도착 지연(Race Condition)을 막기 위해 잠시 대기(최대 2초)
                            retries = 0
                            while not cached_time and retries < 10:
                                time.sleep(0.2)
                                cached_time = redis_manager.get_block_timestamp(token_model.block_number)
                                retries += 1
                                
                            if cached_time:
                                token_model.timestamp = datetime.fromisoformat(cached_time)
                            else:
                                logger.warning(f"블록 {token_model.block_number}의 시간을 찾을 수 없어 스킵합니다.")
                                continue # 시간 없는 데이터는 버림
                                
                        token_batch.append(token_model)
                except ValidationError as e:
                    logger.error(f"데이터 검증 실패 (Pydantic 에러): {e}")
                    # 실무에서는 여기서 실패한 데이터를 Dead Letter Queue(DLQ)로 전송합니다.
                    continue

            total_batch_size = len(tx_batch) + len(token_batch)
            
            # 설정한 배치 크기(BATCH_SIZE)에 도달하면 일괄 DB 저장 후 Kafka 커밋 수행
            if total_batch_size >= settings.BATCH_SIZE:
                conn = db_manager.get_connection()
                try:
                    conn.autocommit = False # 트랜잭션 수동 제어
                    
                    # execute_values를 사용한 초고속 배치 인서트
                    if tx_batch:
                        db_manager.insert_transactions_batch(conn, tx_batch)
                    if token_batch:
                        db_manager.insert_token_transfers_batch(conn, token_batch)
                    
                    # 1. DB 트랜잭션 확정
                    conn.commit()
                    # 2. DB 성공 시에만 카프카 오프셋 확정 (At-Least-Once 보장)
                    kafka_client.commit()
                    
                    logger.info(f"배치 커밋 완료: 트랜잭션 {len(tx_batch)}건, 토큰 {len(token_batch)}건")
                    
                    # 버퍼 비우기
                    tx_batch.clear()
                    token_batch.clear()
                except Exception as e:
                    conn.rollback()
                    logger.error(f"DB 배치 저장 실패. 롤백 수행: {e}")
                finally:
                    # 풀에 커넥션 반환
                    db_manager.release_connection(conn)

    except KeyboardInterrupt:
        pass
    finally:
        # 종료 시점에 버퍼에 남아있는 자투리 데이터 처리
        if tx_batch or token_batch:
            conn = db_manager.get_connection()
            try:
                conn.autocommit = False
                if tx_batch:
                    db_manager.insert_transactions_batch(conn, tx_batch)
                if token_batch:
                    db_manager.insert_token_transfers_batch(conn, token_batch)
                conn.commit()
                kafka_client.commit()
                logger.info(f"종료 전 남은 데이터 커밋 완료: 트랜잭션 {len(tx_batch)}건, 토큰 {len(token_batch)}건")
            except Exception as e:
                conn.rollback()
                logger.error(f"종료 전 자투리 커밋 실패: {e}")
            finally:
                db_manager.release_connection(conn)

        # 리소스 안전 해제
        kafka_client.close()
        db_manager.close_all()
        logger.info("모든 리소스 정리 완료. 애플리케이션이 안전하게 종료되었습니다.")

if __name__ == "__main__":
    main()
