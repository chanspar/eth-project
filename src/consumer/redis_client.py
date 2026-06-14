import redis
import logging
from .config import settings

logger = logging.getLogger(__name__)

class RedisManager:
    def __init__(self):
        try:
            self.client = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True # 바이트 대신 문자열로 자동 디코딩
            )
            self.client.ping()
            logger.info("Redis 연결 성공 (블록 시간 캐싱용)")
        except Exception as e:
            logger.error(f"Redis 연결 실패: {e}")
            raise e

    def cache_block_timestamp(self, block_number: int, timestamp: str):
        # 1일(86400초) 후 자동 만료되도록 설정해 메모리 누수를 방지합니다.
        self.client.setex(f"block_time:{block_number}", 86400, timestamp)

    def get_block_timestamp(self, block_number: int) -> str | None:
        return self.client.get(f"block_time:{block_number}")
