from fastapi import HTTPException
import logging
from src.backend.repositories.whale_repo import WhaleRepository
from src.backend.models.schemas import WhaleListResponse, WhaleTransaction

logger = logging.getLogger(__name__)

class WhaleService:
    WHALE_THRESHOLD_ETH = 100
    WHALE_THRESHOLD_WEI = WHALE_THRESHOLD_ETH * (10 ** 18)

    def __init__(self, repo: WhaleRepository):
        self.repo = repo
        
    async def get_recent_whales(self, limit: int) -> WhaleListResponse:
        try:
            rows = await self.repo.get_recent_whales(self.WHALE_THRESHOLD_WEI, limit)
            whales = []
            for row in rows:
                whales.append(WhaleTransaction(
                    hash=row['hash'],
                    timestamp=row['timestamp'],
                    from_address=row['from_address'],
                    to_address=row['to_address'],
                    value_eth=float(row['value']) / 1e18
                ))
            return WhaleListResponse(whales=whales)
        except Exception as e:
            logger.error(f"Failed to fetch recent whales: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")
