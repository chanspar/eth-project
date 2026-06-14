import redis
import logging
from typing import Optional
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
        # 카프카 파티션 딜레이를 고려해 10분(600초) 후 자동 만료되도록 설정 (메모리 절약)
        self.client.setex(f"block_time:{block_number}", 600, timestamp)

    def get_block_timestamp(self, block_number: int) -> Optional[str]:
        result = self.client.get(f"block_time:{block_number}")
        if result is None:
            return None
        return str(result)
