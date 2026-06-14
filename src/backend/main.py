from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from src.backend.core.db import init_db_pool, close_db_pool, load_tokens_from_csv
from src.backend.api import gas, whales, tokens, wallets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    pool = await init_db_pool()
    app.state.pool = pool
    await load_tokens_from_csv(pool)
    yield
    # Shutdown
    await close_db_pool()

app = FastAPI(
    title="Ethereum Real-time Dashboard API",
    description="카프카와 TimescaleDB를 활용한 실시간 이더리움 대시보드 백엔드 API",
    version="1.0.0",
    lifespan=lifespan
)

# CORS 설정 (프론트엔드 연동을 위해)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 라우터 등록
app.include_router(gas.router, prefix="/api/v1/metrics")
app.include_router(whales.router, prefix="/api/v1/whales")
app.include_router(tokens.router, prefix="/api/v1/tokens")
app.include_router(wallets.router, prefix="/api/v1/wallets")
app.include_router(whales.ws_router, prefix="/ws")

@app.get("/")
async def root():
    return {"message": "Ethereum Dashboard API is running."}
