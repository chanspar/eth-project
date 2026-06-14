from fastapi import HTTPException
import logging
from src.backend.repositories.gas_repo import GasRepository
from src.backend.models.schemas import GasMetricsResponse

logger = logging.getLogger(__name__)

class GasService:
    def __init__(self, repo: GasRepository):
        self.repo = repo
        
    async def get_gas_metrics(self) -> GasMetricsResponse:
        try:
            avg_wei = await self.repo.get_average_gas_price_last_5_minutes()
            avg_gwei = avg_wei / 1e9
            return GasMetricsResponse(average_gas_price_gwei=round(avg_gwei, 2))
        except Exception as e:
            logger.error(f"Failed to fetch gas metrics: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")
