from fastapi import HTTPException
import logging
from src.backend.repositories.gas_repo import GasRepository
from src.backend.models.schemas import GasMetricsResponse

logger = logging.getLogger(__name__)

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
            HTTPException: 데이터베이스 쿼리 혹은 변환 처리 중 오류 발생 시 500 상태 코드 반환
        """
        try:
            avg_wei = await self.repo.get_average_gas_price_last_5_minutes()
            avg_gwei = avg_wei / 1e9
            return GasMetricsResponse(average_gas_price_gwei=round(avg_gwei, 2))
        except Exception as e:
            logger.error(f"Failed to fetch gas metrics: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

