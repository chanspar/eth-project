import asyncpg
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Depends
from src.backend.core.db import get_db
from src.backend.core.ws_manager import manager
from src.backend.repositories.whale_repo import WhaleRepository
from src.backend.services.whale_service import WhaleService
from src.backend.models.schemas import WhaleListResponse

router = APIRouter()
ws_router = APIRouter()

def get_whale_service(conn: asyncpg.Connection = Depends(get_db)) -> WhaleService:
    repo = WhaleRepository(conn)
    return WhaleService(repo)

@router.get("/", response_model=WhaleListResponse)
async def get_recent_whales(limit: int = 10, service: WhaleService = Depends(get_whale_service)):
    """
    최근 발생한 100 ETH 이상 고래 이체 내역을 반환합니다.
    """
    return await service.get_recent_whales(limit=limit)

@ws_router.websocket("/whales")
async def whale_websocket(websocket: WebSocket):
    """
    고래 이체 내역 실시간 스트리밍 (웹소켓)
    PostgreSQL의 LISTEN/NOTIFY를 통해 이벤트 기반으로 동작합니다.
    """
    await manager.connect(websocket)
    try:
        while True:
            # 클라이언트 연결 유지를 위한 루프 (실제 데이터 전송은 manager.broadcast에서 발생)
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

