from fastapi import HTTPException
import logging
from src.backend.repositories.token_repo import TokenRepository
from src.backend.models.schemas import TrendingTokensResponse, TrendingToken

logger = logging.getLogger(__name__)

class TokenService:
    def __init__(self, repo: TokenRepository):
        self.repo = repo
        
    async def get_trending_tokens(self, limit: int, hours: int) -> TrendingTokensResponse:
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
