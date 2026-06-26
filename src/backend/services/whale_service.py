from src.backend.core.exceptions import DatabaseFetchError
from src.backend.core.logger import logger
from src.backend.models.schemas import WhaleListResponse, WhaleTransaction
from src.backend.repositories.whale_repo import WhaleRepository

class WhaleService:
    """
    이더리움 네트워크의 대규모 자금 이체(고래 거래)를 탐지하고 관리하는 비즈니스 서비스 클래스입니다.
    """
    WHALE_THRESHOLD_ETH = 100
    WHALE_THRESHOLD_WEI = WHALE_THRESHOLD_ETH * (10 ** 18)

    def __init__(self, repo: WhaleRepository):
        """
        WhaleService 인스턴스를 초기화합니다.
        
        Args:
            repo (WhaleRepository): 고래 데이터베이스 접근 리포지토리 객체
        """
        self.repo = repo
        
    async def get_recent_whales(self, limit: int) -> WhaleListResponse:
        """
        설정된 임계값(예: 100 ETH) 이상의 큰 금액이 이체된 최근 고래 거래 내역 목록을 포맷팅하여 가져옵니다.
        
        Args:
            limit (int): 반환할 거래 내역 최대 개수
            
        Returns:
            WhaleListResponse: 고래 트랜잭션 정보 및 지갑 라벨링 메타데이터를 포함한 응답 DTO
            
        Raises:
            DatabaseFetchError: 데이터베이스 쿼리 혹은 포맷팅 처리 중 오류 발생 시 예외 발생
        """
        try:
            rows = await self.repo.get_recent_whales(self.WHALE_THRESHOLD_WEI, limit)
            whales = []
            for row in rows:
                whales.append(WhaleTransaction(
                    hash=row['hash'],
                    timestamp=row['timestamp'],
                    from_address=row['from_address'],
                    to_address=row['to_address'],
                    value_eth=float(row['value']) / 1e18,
                    from_label=row.get('from_label'),
                    to_label=row.get('to_label'),
                    from_category=row.get('from_category'),
                    to_category=row.get('to_category')
                ))

            return WhaleListResponse(whales=whales)
        except Exception as e:
            logger.error(f"Failed to fetch recent whales: {e}")
            raise DatabaseFetchError("Database query failed")

