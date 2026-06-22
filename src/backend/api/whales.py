import asyncpg
from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect

from src.backend.core.db import get_db
from src.backend.core.ws_manager import manager
from src.backend.models.schemas import WhaleListResponse
from src.backend.repositories.whale_repo import WhaleRepository
from src.backend.services.whale_service import WhaleService

router = APIRouter()
ws_router = APIRouter()


def get_whale_service(conn: asyncpg.Connection = Depends(get_db)) -> WhaleService:
    repo = WhaleRepository(conn)
    return WhaleService(repo)


@router.get("", response_model=WhaleListResponse)
async def get_recent_whales(limit: int = 50, service: WhaleService = Depends(get_whale_service)):
    """
    최근 설정된 임계값 이상의 대규모 자금 이체(고래 거래) 내역 목록을 조회합니다.
    """
    return await service.get_recent_whales(limit=limit)


@ws_router.websocket("/whales")
async def whale_websocket(websocket: WebSocket):
    """
    실시간 고래 거래 알림(Kafka whale-alerts 토픽) 브로드캐스트 전송을 위한 웹소켓 연결을 유지합니다.
    """
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
