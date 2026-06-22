from src.backend.core.exceptions import DatabaseFetchError
from src.backend.core.logger import logger
from src.backend.models.schemas import GasMetricsResponse
from src.backend.repositories.gas_repo import GasRepository
from src.backend.core.redis_client import redis_manager

import asyncio

_gas_metrics_lock = asyncio.Lock()

class GasService:
    """
    이더리움 네트워크의 가스비(Gas Price) 관련 실시간 분석 및 지표 변환 작업을 담당하는 비즈니스 서비스 클래스입니다.
    """
    def __init__(self, repo: GasRepository):
        """
        GasService 인스턴스를 초기화합니다.
        
        Args:
            repo (GasRepository): 가스 데이터베이스 접근 리포지토리 객체
        """
        self.repo = repo
        
    async def get_gas_metrics(self) -> GasMetricsResponse:
        """
        최근 5분간 이더리움 평균 가스 가격 데이터를 가져와 Gwei 단위로 변환 및 포맷팅하여 반환합니다.
        
        Returns:
            GasMetricsResponse: 소수점 둘째 자리까지 반올림된 Gwei 단위의 가스비 지표 응답 DTO
            
        Raises:
            DatabaseFetchError: 데이터베이스 쿼리 혹은 변환 처리 중 오류 발생 시 예외 발생
        """
        try:
            cache_key = "cache:gas_metrics:5min"
            
            # 1. 1차 Redis 캐시 확인 (빠른 반환)
            if redis_manager.redis:
                cached_data = await redis_manager.redis.get(cache_key)
                if cached_data is not None:
                    return GasMetricsResponse(average_gas_price_gwei=float(cached_data))
            
            # 2. 캐시 Stampede(동시 접근 폭주) 방지를 위한 Lock 획득 및 Double-Checked Locking
            async with _gas_metrics_lock:
                # Lock을 대기하는 동안 다른 요청이 캐시를 채워넣었을 수 있으므로 2차 확인
                if redis_manager.redis:
                    cached_data = await redis_manager.redis.get(cache_key)
                    if cached_data is not None:
                        return GasMetricsResponse(average_gas_price_gwei=float(cached_data))
                
                # 3. 진짜 캐시 미스인 경우 최초 1명만 DB 쿼리 실행
                avg_wei = await self.repo.get_average_gas_price_last_5_minutes()
                avg_gwei = round(avg_wei / 1e9, 2)
                
                # 4. 조회 결과를 Redis에 캐시 저장 (TTL: 60초)
                if redis_manager.redis:
                    await redis_manager.redis.set(cache_key, avg_gwei, ex=60)
                    
                return GasMetricsResponse(average_gas_price_gwei=avg_gwei)
        except Exception as e:
            logger.error(f"Failed to fetch gas metrics: {e}")
            raise DatabaseFetchError("Database query failed")
