import asyncio
import logging
from typing import List
from fastapi import WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)

class ConnectionManager:
    def __init__(self, max_connections: int = 1000, ping_interval: int = 30):
        self.active_connections: List[WebSocket] = []
        self.max_connections = max_connections
        self.ping_interval = ping_interval
        self._ping_task = None

    async def connect(self, websocket: WebSocket):
        # 1. 최대 연결 수 초과 시 즉시 거부 (웹소켓 규격코드 1008)
        if len(self.active_connections) >= self.max_connections:
            logger.warning(f"WebSocket connection rejected: Max limit ({self.max_connections}) reached.")
            await websocket.close(code=1008, reason="Max connections limit reached")
            raise WebSocketDisconnect(code=1008, reason="Max connections limit reached")

        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket client connected. Total: {len(self.active_connections)}")

        # 2. 최초 연결 발생 시 백그라운드 하트비트 루프 구동
        if self._ping_task is None or self._ping_task.done():
            self._ping_task = asyncio.create_task(self._ping_loop())

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Total: {len(self.active_connections)}")

    async def _send_to_connection(self, connection: WebSocket, message: dict):
        try:
            await connection.send_json(message)
        except Exception as e:
            logger.error(f"Failed to send message to websocket client: {e}")
            self.disconnect(connection)
            try:
                await connection.close()
            except Exception:
                pass

    async def broadcast(self, message: dict):
        tasks = [
            self._send_to_connection(connection, message) 
            for connection in list(self.active_connections)
        ]
        if tasks:
            await asyncio.gather(*tasks)

    async def _ping_loop(self):
        logger.info("WebSocket heartbeat loop started.")
        try:
            while True:
                await asyncio.sleep(self.ping_interval)
                if not self.active_connections:
                    continue
                # 모든 활성 클라이언트에 애플리케이션 레벨 핑 전송 (데드 소켓 자동 정리 유도)
                await self.broadcast({"type": "ping"})
        except asyncio.CancelledError:
            logger.info("WebSocket heartbeat loop cancelled.")
        except Exception as e:
            logger.error(f"Error in heartbeat loop: {e}")

    async def stop_ping_loop(self):
        if self._ping_task and not self._ping_task.done():
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass
            logger.info("WebSocket heartbeat task safely cancelled.")

manager = ConnectionManager()
