import asyncpg
from fastapi import APIRouter, Depends
from src.backend.core.db import get_pool
from src.backend.repositories.gas_repo import GasRepository
from src.backend.services.gas_service import GasService
from src.backend.models.schemas import GasMetricsResponse

router = APIRouter()

def get_gas_service(pool: asyncpg.Pool = Depends(get_pool)) -> GasService:
    repo = GasRepository(pool)
    return GasService(repo)

@router.get("/gas", response_model=GasMetricsResponse)
async def get_gas_metrics(service: GasService = Depends(get_gas_service)):
    """
    최근 5분간의 평균 가스비(Gwei)를 계산하여 반환합니다.
    """
    return await service.get_gas_metrics()
