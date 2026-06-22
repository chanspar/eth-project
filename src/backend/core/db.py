import csv
import logging
import os
from typing import AsyncGenerator, Optional

import aiofiles
import asyncpg
from fastapi import Request

from src.backend.core.config import settings

pool: Optional[asyncpg.Pool] = None


async def init_db_pool() -> asyncpg.Pool:
    """
    PostgreSQL 커넥션 풀을 초기화하고 전역 변수 'pool'에 저장합니다.
    
    설정 파일(settings)의 DSN 정보 및 커넥션 풀 크기 제한(DB_POOL_MIN_CONN, DB_POOL_MAX_CONN)을
    기반으로 asyncpg 커넥션 풀을 생성하고 로깅합니다.
    
    Returns:
        asyncpg.Pool: 생성된 PostgreSQL 커넥션 풀 객체
    """
    global pool

    pool = await asyncpg.create_pool(
        settings.POSTGRES_DSN,
        min_size=settings.DB_POOL_MIN_CONN,
        max_size=settings.DB_POOL_MAX_CONN,
    )
    logging.info(f"DB 커넥션 풀 생성 완료. (최대 {pool.get_max_size()}개)")

    return pool


async def close_db_pool():
    """
    전역 'pool' 변수에 저장되어 활성화된 PostgreSQL 커넥션 풀을 닫습니다.
    
    서버 종료 시 Lifespan 이벤트에서 안전하게 커넥션 리소스를 회수하고
    전역 'pool'을 None으로 초기화합니다.
    """
    global pool

    if pool:
        await pool.close()
        pool = None
        logging.info("DB 커넥션 풀이 안전하게 종료되었습니다.")


async def get_db(request: Request) -> AsyncGenerator[asyncpg.Connection, None]:
    """
    FastAPI Dependency용 헬퍼 함수입니다.
    
    FastAPI 애플리케이션 상태(app.state.pool)에 등록된 커넥션 풀로부터
    비동기 데이터베이스 커넥션을 획득하여 yield하고, 라우터 작업이 완료되면
    안전하게 커넥션을 반환(close/release)합니다.
    
    Args:
        request (Request): FastAPI HTTP Request 객체
        
    Yields:
        asyncpg.Connection: 획득한 비동기 DB 커넥션
    """
    async with request.app.state.pool.acquire() as conn:
        yield conn

def get_pool(request: Request) -> asyncpg.Pool:
    """
    FastAPI 애플리케이션에 등록된 커넥션 풀 자체를 반환합니다.
    캐시 확인 전 불필요하게 커넥션을 미리 점유(acquire)하는 것을 방지할 때 유용합니다.
    """
    return request.app.state.pool


async def load_tokens_from_csv(pool: asyncpg.Pool):
    """
    CSV 파일(top1000_erc20_tokens.csv)로부터 토큰 정보를 읽어와 DB의 tokens 테이블에 일괄 로드합니다.
    
    1. 로컬 데이터 경로에 파일이 존재하는지 검증합니다.
    2. CSV 파일을 읽어서 토큰의 주소(소문자 처리), 심볼, 이름, 소수점 자릿수(decimals) 정보를 파싱합니다.
    3. INSERT INTO ... ON CONFLICT DO UPDATE DML 구문을 사용해 중복(conflict) 주소가 발생할 경우,
    토큰 심볼, 이름, 소수점 정보를 업데이트합니다.
    
    Args:
        pool (asyncpg.Pool): DB에 쿼리를 실행할 커넥션 풀 객체
    """
    csv_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "data",
        "top1000_erc20_tokens.csv",
    )
    if not os.path.exists(csv_path):
        logging.warning("Token metadata CSV not found: %s", csv_path)
        return

    logging.info("Loading token metadata CSV into database")

    async with aiofiles.open(csv_path, mode="r", encoding="utf-8-sig") as f:
        content = await f.read()

    reader = csv.DictReader(content.splitlines())
    records = []
    for row in reader:
        address = row["address"].lower()
        symbol = row["symbol"]
        name = row["name"]
        decimals = int(row["decimals"]) if row["decimals"].isdigit() else 18
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
        logging.info("Loaded/updated %s token metadata rows", len(records))


async def load_known_label_from_csv(pool: asyncpg.Pool):
    """
    CSV 파일(known_labels.csv)로부터 알려진 지갑 주소 라벨 정보를 읽어와 DB의 address_labels 테이블에 일괄 로드합니다.
    
    1. 로컬 데이터 경로에 파일이 존재하는지 검증합니다.
    2. CSV 파일을 읽어서 주소(소문자 처리), 이름(name), 카테고리(category) 정보를 파싱합니다.
    3. INSERT INTO ... ON CONFLICT (address) DO UPDATE DML 구문을 사용하여
    이미 존재하는 주소인 경우 이름과 카테고리를 최신 정보로 업데이트합니다.
    
    Args:
        pool (asyncpg.Pool): DB에 쿼리를 실행할 커넥션 풀 객체
    """
    csv_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "data",
        "known_labels.csv",
    )
    if not os.path.exists(csv_path):
        logging.warning("Known labels CSV not found: %s", csv_path)
        return

    logging.info("Loading known labels CSV into database")

    async with aiofiles.open(csv_path, mode="r", encoding="utf-8-sig") as f:
        content = await f.read()

    reader = csv.DictReader(content.splitlines())
    records = []
    for row in reader:
        address = row["address"].lower()
        name = row["name"]
        category = row["category"]
        records.append((address, name, category))

    query = """
    INSERT INTO address_labels (address, name, category)
    VALUES ($1, $2, $3)
    ON CONFLICT (address) DO UPDATE
    SET name = EXCLUDED.name,
        category = EXCLUDED.category;
    """

    async with pool.acquire() as conn:
        await conn.executemany(query, records)
        logging.info("Loaded/updated %s known label rows", len(records))

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

async def load_tokens_to_es(es_client: AsyncElasticsearch):
    """
    CSV 파일(top1000_erc20_tokens.csv)로부터 토큰 정보를 읽어와 Elasticsearch의 tokens 인덱스에 일괄 로드합니다.
    앱 시작 시 자동으로 실행되어 수동 인덱싱이 필요 없도록 합니다.
    """
    csv_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "data",
        "top1000_erc20_tokens.csv",
    )
    if not os.path.exists(csv_path):
        logging.warning("Token metadata CSV not found for ES: %s", csv_path)
        return

    logging.info("Loading token metadata CSV into Elasticsearch")

    async with aiofiles.open(csv_path, mode="r", encoding="utf-8-sig") as f:
        content = await f.read()

    reader = csv.DictReader(content.splitlines())
    actions = []
    for row in reader:
        address = row["address"].lower()
        actions.append({
            "_index": "tokens",
            "_id": address,
            "_source": {
                "address": address,
                "symbol": row["symbol"],
                "name": row["name"],
                "decimals": int(row["decimals"]) if row["decimals"].isdigit() else 18
            }
        })

    try:
        await async_bulk(es_client, actions)
        logging.info("Loaded/updated %s tokens in Elasticsearch", len(actions))
    except Exception as e:
        logging.error("Failed to load tokens to Elasticsearch: %s", e)


