import os
import asyncpg
import aiofiles
import csv
import json
import logging
import asyncio
from typing import Optional, AsyncGenerator
from fastapi import Request
from src.backend.core.config import settings
from src.backend.core.ws_manager import manager

pool: Optional[asyncpg.Pool] = None
listener_conn: Optional[asyncpg.Connection] = None

def handle_whale_notification(connection, pid, channel, payload):
    """
    PostgreSQL pg_notify 콜백 함수입니다.
    이벤트 루프에 broadcast 태스크를 예약합니다.
    """
    try:
        data = json.loads(payload)
        # Wei to ETH 변환
        value_eth = float(data.get('value', 0)) / 1e18
        
        whale_msg = {
            "hash": data.get('hash'),
            "timestamp": data.get('timestamp'),
            "from_address": data.get('from_address'),
            "to_address": data.get('to_address'),
            "value_eth": value_eth
        }
        
        asyncio.create_task(manager.broadcast(whale_msg))
    except Exception as e:
        logging.error(f"Error processing notification: {e}")

async def init_db_pool() -> asyncpg.Pool:
    global pool
    global listener_conn
    
    pool = await asyncpg.create_pool(
        settings.POSTGRES_DSN, 
        min_size=settings.DB_POOL_MIN_CONN, 
        max_size=settings.DB_POOL_MAX_CONN
    )
    logging.info("PostgreSQL 연결 풀(Pool)이 생성되었습니다.")
    
    # Trigger Setup for Listen/Notify (100 ETH = 10^20 Wei)
    setup_trigger_query = """
    CREATE OR REPLACE FUNCTION notify_whale_event() RETURNS TRIGGER AS $$
    BEGIN
        IF NEW.value >= 100000000000000000000 THEN
            PERFORM pg_notify('whale_events', row_to_json(NEW)::text);
        END IF;
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'whale_trigger') THEN
            CREATE TRIGGER whale_trigger
            AFTER INSERT ON transactions
            FOR EACH ROW EXECUTE PROCEDURE notify_whale_event();
        END IF;
    END
    $$;
    """
    
    async with pool.acquire() as conn:
        await conn.execute(setup_trigger_query)
        logging.info("Whale 트랜잭션 감지 트리거가 세팅되었습니다.")
        
    # Set up dedicated connection for LISTEN
    listener_conn = await asyncpg.connect(settings.POSTGRES_DSN)
    await listener_conn.add_listener('whale_events', handle_whale_notification)
    logging.info("PostgreSQL LISTEN ('whale_events') 가 활성화되었습니다.")
    
    return pool

async def close_db_pool():
    global pool
    global listener_conn
    
    if listener_conn:
        await listener_conn.close()
        logging.info("PostgreSQL LISTEN 커넥션이 종료되었습니다.")
        
    if pool:
        await pool.close()
        logging.info("PostgreSQL 연결 풀이 종료되었습니다.")

async def get_db(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """FastAPI Dependency Injection을 위한 DB 커넥션 제공자"""
    async with request.app.state.pool.acquire() as conn:
        yield conn

async def load_tokens_from_csv(pool: asyncpg.Pool):
    """
    서버 시작 시 src/data/top1000_erc20_tokens.csv 파일을 읽어
    tokens 테이블에 초기 데이터를 적재합니다. (Upsert)
    """
    csv_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "top1000_erc20_tokens.csv")
    if not os.path.exists(csv_path):
        logging.warning(f"토큰 메타데이터 파일을 찾을 수 없습니다: {csv_path}")
        return

    logging.info("토큰 메타데이터(CSV)를 비동기적으로 DB에 적재합니다...")
    
    # aiofiles를 사용한 Non-blocking I/O
    async with aiofiles.open(csv_path, mode='r', encoding='utf-8') as f:
        content = await f.read()
        
    reader = csv.DictReader(content.splitlines())
    records = []
    for row in reader:
        address = row['address'].lower()
        symbol = row['symbol']
        name = row['name']
        decimals = int(row['decimals']) if row['decimals'].isdigit() else 18
        records.append((address, symbol, name, decimals))

    query = """
    INSERT INTO tokens (address, symbol, name, decimals)
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (address) DO UPDATE
    SET symbol = EXCLUDED.symbol,
        name = EXCLUDED.name,
        decimals = EXCLUDED.decimals;
    """

    async with pool.acquire() as conn:
        await conn.executemany(query, records)
        logging.info(f"총 {len(records)}개의 토큰 메타데이터가 적재/업데이트 되었습니다.")

