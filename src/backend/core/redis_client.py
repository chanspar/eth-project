import redis.asyncio as redis
from typing import Optional
from src.backend.core.config import settings
from src.backend.core.logger import logger

class RedisManager:
    def __init__(self):
        self.redis: Optional[redis.Redis] = None

    async def connect(self):
        try:
            self.redis = redis.Redis(
                host=settings.REDIS_HOST,
                port=settings.REDIS_PORT,
                db=settings.REDIS_DB,
                decode_responses=True # 문자열로 자동 디코딩
            )
            # 연결 테스트
            await self.redis.ping()
            logger.info(f"Connected to Redis at {settings.REDIS_HOST}:{settings.REDIS_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis = None

    async def disconnect(self):
        if self.redis:
            await self.redis.aclose() # type: ignore
            logger.info("Disconnected from Redis")

redis_manager = RedisManager()
