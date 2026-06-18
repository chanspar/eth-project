from fastapi import HTTPException
import logging
from src.backend.repositories.token_repo import TokenRepository
from src.backend.models.schemas import TrendingTokensResponse, TrendingToken

logger = logging.getLogger(__name__)

class TokenService:
    """
    ERC20 토큰의 트렌드 지표 수집 및 통계 처리를 담당하는 비즈니스 서비스 클래스입니다.
    """
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
        
        Args:
            limit (int): 반환할 인기 토큰 목록의 최대 개수
            hours (int): 트렌드를 분석할 기준 시간
            
        Returns:
            TrendingTokensResponse: 정렬된 인기 토큰 정보 응답 DTO
            
        Raises:
            HTTPException: 데이터베이스 쿼리 혹은 분석 처리 중 오류 발생 시 500 상태 코드 반환
        """
        try:
            rows = await self.repo.get_trending_tokens(hours=hours, limit=limit)
            tokens = []
            for row in rows:
                tokens.append(TrendingToken(
                    address=row['address'],
                    symbol=row['symbol'],
                    name=row['name'],
                    transfer_count=row['transfer_count']
                ))
            return TrendingTokensResponse(trending_tokens=tokens)
        except Exception as e:
            logger.error(f"Failed to fetch trending tokens: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

