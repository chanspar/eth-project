import logging
from psycopg2 import pool
from psycopg2.extras import execute_values
from .config import settings

logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        try:
            self.connection_pool = pool.ThreadedConnectionPool(
                minconn=settings.DB_POOL_MIN_CONN,
                maxconn=settings.DB_POOL_MAX_CONN,
                dsn=settings.POSTGRES_DSN
            )
            logger.info("PostgreSQL 커넥션 풀 초기화 완료")
        except Exception as e:
            logger.error(f"PostgreSQL 커넥션 풀 생성 실패: {e}")
            raise e

    def get_connection(self):
        """
        풀에서 DB 커넥션을 가져오는 메서드
        만약 설정한 maxconn 개수보다 많은 요청이 들어오면, 가장 오랫동안 사용하지 않은 커넥션을 반납시킬지, 기다리게 할지 설정 필요
        (여기서는 getconn()이 대기하다가 반납된 커넥션을 가져오도록 설정해둠)
        """
        return self.connection_pool.getconn()

    def release_connection(self, conn):
        """
        사용이 끝난 커넥션을 풀에 반납
        """
        self.connection_pool.putconn(conn)

    def close_all(self):
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("PostgreSQL 커넥션 풀 종료 완료")

    def insert_transactions_batch(self, conn, transactions: list):
        """execute_values를 이용한 초고속 배치 UPSERT"""
        if not transactions:
            return
        
        query = """
            INSERT INTO transactions (hash, timestamp, from_address, to_address, value, gas_price)
            VALUES %s
            ON CONFLICT (hash, timestamp) DO NOTHING;
        """

        # 1000개의 Pydantic 객체(tx)에서 필요한 알맹이만 쏙쏙 빼서 
        # (해시, 시간, 보낸사람...) 형태의 박스(튜플)로 포장합니다.
        # 이게 %s에 들어감
        values = [(
            tx.hash, tx.timestamp, tx.from_address, tx.to_address, tx.value, tx.gas_price
        ) for tx in transactions]

        with conn.cursor() as cursor:
            # execute_values(실행할 커서, 실행할 쿼리 문자열, 전달할 값들의 리스트)
            execute_values(cursor, query, values)

    def insert_token_transfers_batch(self, conn, transfers: list):
        """execute_values를 이용한 초고속 배치 UPSERT"""
        if not transfers:
            return
            
        query = """
            INSERT INTO token_transfers (transaction_hash, log_index, timestamp, token_address, from_address, to_address, value)
            VALUES %s
            ON CONFLICT (transaction_hash, log_index, timestamp) DO NOTHING;
        """
        values = [(
            tf.transaction_hash, tf.log_index, tf.timestamp, tf.token_address, tf.from_address, tf.to_address, tf.value
        ) for tf in transfers]

        with conn.cursor() as cursor:
            execute_values(cursor, query, values)
