import asyncpg
from fastapi import APIRouter, Depends
from src.backend.core.db import get_db
from src.backend.repositories.token_repo import TokenRepository
from src.backend.services.token_service import TokenService
from src.backend.models.schemas import TrendingTokensResponse

router = APIRouter()

def get_token_service(conn: asyncpg.Connection = Depends(get_db)) -> TokenService:
    repo = TokenRepository(conn)
    return TokenService(repo)

@router.get("/trending", response_model=TrendingTokensResponse)
async def get_trending_tokens(limit: int = 10, hours: int = 1, service: TokenService = Depends(get_token_service)):
    """
    최근 N시간 동안 가장 전송 건수가 많은 핫 토큰 랭킹을 반환합니다.
    """
    return await service.get_trending_tokens(limit=limit, hours=hours)
