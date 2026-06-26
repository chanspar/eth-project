import asyncio
import json
from src.backend.core.exceptions import DatabaseFetchError
from src.backend.core.logger import logger
from src.backend.models.schemas import TrendingTokensResponse, TrendingToken, TokenSearchResponse
from src.backend.repositories.token_repo import TokenRepository
from src.backend.core.redis_client import redis_manager

class TokenService:
    """
    ERC20 토큰의 트렌드 지표 수집 및 통계 처리를 담당하는 비즈니스 서비스 클래스입니다.
    """
    _trending_lock = asyncio.Lock()  # Cache Stampede 방지용 락

    def __init__(self, repo: TokenRepository):
        """
        TokenService 인스턴스를 초기화합니다.
        
        Args:
            repo (TokenRepository): 토큰 데이터베이스 접근 리포지토리 객체
        """
        self.repo = repo
        
    async def get_trending_tokens(self, limit: int, hours: int) -> TrendingTokensResponse:
        """
        최근 특정 시간(hours) 동안 이체 빈도가 가장 높은 인기 토큰 통계 목록을 가져옵니다.
        Redis 캐싱 및 Double-Checked Locking을 통해 DB 부하를 최소화합니다.
        """
        cache_key = f"trending_tokens:{hours}h:limit_{limit}"

        try:
            # 1. 락 없이 캐시 우선 확인
            if redis_manager.redis:
                cached_data = await redis_manager.redis.get(cache_key)
                if cached_data:
                    return TrendingTokensResponse(**json.loads(cached_data))

            # 2. 캐시 미스 시 락 획득 (Cache Stampede 방지)
            async with self._trending_lock:
                # 3. 락 획득 후 캐시 재확인 (Double-Checked Locking)
                if redis_manager.redis:
                    cached_data = await redis_manager.redis.get(cache_key)
                    if cached_data:
                        return TrendingTokensResponse(**json.loads(cached_data))

                # 4. 여전히 캐시가 없다면 DB에서 무거운 집계 쿼리 실행
                rows = await self.repo.get_trending_tokens(hours=hours, limit=limit)
                tokens = []
                for row in rows:
                    tokens.append(TrendingToken(
                        address=row['address'],
                        symbol=row['symbol'],
                        name=row['name'],
                        transfer_count=row['transfer_count']
                    ))
                
                response = TrendingTokensResponse(trending_tokens=tokens)

                # 5. DB 조회 결과를 Redis에 저장 (TTL: 60초)
                if redis_manager.redis:
                    await redis_manager.redis.set(
                        cache_key,
                        response.model_dump_json(),
                        ex=60
                    )

                return response

        except Exception as e:
            logger.error(f"Failed to fetch trending tokens: {e}")
            raise DatabaseFetchError("Database query failed")

    async def get_token_trends(self, address: str, bucket_width: str, limit: int) -> list:
        try:
            rows = await self.repo.get_token_trends_by_address(address, bucket_width, limit)
            from src.backend.models.schemas import TokenTrendPoint
            trends = []
            for row in rows:
                trends.append(TokenTrendPoint(
                    time_bucket=row['bucket'],
                    transfer_count=row['transfer_count'],
                    total_value=float(row['total_value']) if row['total_value'] else 0.0
                ))
            return trends
        except Exception as e:
            logger.error(f"Failed to fetch token trends for {address}: {e}")
            raise DatabaseFetchError("Database query failed")

    async def get_all_tokens(self, limit: int, offset: int, prefix: str = None) -> list[TokenSearchResponse]:
        try:
            rows = await self.repo.get_all_tokens(limit=limit, offset=offset, prefix=prefix)
            tokens = []
            for row in rows:
                tokens.append(TokenSearchResponse(
                    address=row['address'],
                    symbol=row['symbol'],
                    name=row['name'],
                    decimals=row['decimals']
                ))
            return tokens
        except Exception as e:
            logger.error(f"Failed to fetch all tokens: {e}")
            raise DatabaseFetchError("Database query failed")

