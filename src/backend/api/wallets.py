import asyncpg
from fastapi import APIRouter, Depends
from src.backend.core.db import get_db
from src.backend.repositories.wallet_repo import WalletRepository
from src.backend.services.wallet_service import WalletService
from src.backend.models.schemas import WalletHistoryResponse

router = APIRouter()

def get_wallet_service(conn: asyncpg.Connection = Depends(get_db)) -> WalletService:
    repo = WalletRepository(conn)
    return WalletService(repo)

@router.get("/{address}/history", response_model=WalletHistoryResponse)
async def get_wallet_history(address: str, limit: int = 10, service: WalletService = Depends(get_wallet_service)):
    """
    특정 지갑 주소의 최근 이더리움 송수신 및 토큰 송수신 내역을 조회합니다.
    """
    return await service.get_wallet_history(address=address, limit=limit)
