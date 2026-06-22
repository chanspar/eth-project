import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager, suppress
from typing import Optional

from confluent_kafka import Consumer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic # type: ignore
from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.backend.api import gas, tokens, wallets, whales
from src.backend.core.config import settings

from src.backend.core.db import (
    close_db_pool,
    init_db_pool,
    load_known_label_from_csv,
    load_tokens_from_csv,
    load_tokens_to_es,
)
from src.backend.core.ws_manager import manager

from src.backend.core.logger import logger


def create_whale_alert_consumer() -> Consumer:
    """
    FastAPI 서버의 고유한 Kafka Consumer 인스턴스를 생성합니다.
    
    다중화(Scale-out) 환경에서 각 API 서버 인스턴스가 동일한 알림 메시지를
    복제(Fan-out)하여 받아볼 수 있도록, 무작위 UUID를 활용하여 독립된
    Consumer Group ID를 동적으로 생성합니다.
    """
    group_id = f"{settings.WHALE_ALERTS_GROUP_PREFIX}-{uuid.uuid4()}"
    return Consumer(
        {
            "bootstrap.servers": settings.KAFKA_BROKER,
            "group.id": group_id,
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )


def decode_whale_alert(raw_value: Optional[bytes]) -> Optional[dict]:
    """
    Kafka로부터 수신한 raw 바이트 메시지를 디코딩하고 JSON 파싱을 수행합니다.
    
    파싱 오류(JSONDecodeError) 등 예외 발생 시 None을 안전하게 반환하여,
    백그라운드 루프가 5초 백오프 대기 없이 즉시 다음 메시지를 소비할 수 있도록 보장합니다.
    """
    if raw_value is None:
        return None

    try:
        data = json.loads(raw_value.decode("utf-8"))
        value = data.get("value", 0)
        data.setdefault("value_eth", float(value) / 10**18)
        return data
    except Exception:
        logger.exception("Failed to decode whale alert payload")
        return None


async def consume_whale_alerts(consumer: Optional[Consumer] = None) -> None:
    """
    Kafka의 'whale-alerts' 토픽을 구독하여 백그라운드에서 실시간으로 메시지를 소비합니다.
    
    소비된 알림 데이터는 ws_manager를 통해 연결된 모든 웹소켓 클라이언트들에게
    브로드캐스트됩니다. 카프카 연결 순단 등 인프라 장애 발생 시 CPU 폭주를 막기 위해
    5초의 고정 백오프 대기 후 복구를 재시도합니다.
    """
    consumer = consumer or create_whale_alert_consumer()
    consumer.subscribe([settings.WHALE_ALERTS_TOPIC])
    logger.info("Subscribed to Kafka topic %s for websocket whale alerts", settings.WHALE_ALERTS_TOPIC)

    try:
        while True:
            try:
                msg = await asyncio.to_thread(consumer.poll, 1.0)
                if msg is None:
                    continue

                if msg.error():
                    raise KafkaException(msg.error())

                payload = decode_whale_alert(msg.value())
                if payload is not None:
                    await manager.broadcast(payload)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Failed to consume whale alert; retrying after backoff")
                await asyncio.sleep(5)
    finally:
        await asyncio.to_thread(consumer.close)
        logger.info("Kafka whale alert consumer closed")


from src.backend.core.redis_client import redis_manager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    FastAPI 애플리케이션의 시작(Startup) 및 종료(Shutdown) 생명주기를 관리합니다.
    
    시작 시 DB 커넥션 풀을 초기화하고 토큰 메타데이터를 적재하며, 카프카 알림 수신을 위한
    백그라운드 비동기 태스크를 가동합니다. 종료 시 백그라운드 태스크를 안전하게 취소(Cancel)하고
    소비자 연결 및 DB 커넥션 풀을 우아하게 해제합니다.
    """
    whale_consumer_task: Optional[asyncio.Task] = None
    try:
        pool = await init_db_pool()
        app.state.pool = pool
        await load_tokens_from_csv(pool)
        await load_known_label_from_csv(pool)

        await redis_manager.connect()

        es_client = AsyncElasticsearch([settings.ELASTICSEARCH_URL])
        app.state.es_client = es_client
        
        # 앱 시작 시 ES에 토큰 정보 자동 로드
        await load_tokens_to_es(es_client)



        # Kafka 토픽 자동 생성 보장
        try:
            admin = AdminClient({"bootstrap.servers": settings.KAFKA_BROKER})
            meta = admin.list_topics(timeout=2.0)
            if settings.WHALE_ALERTS_TOPIC not in meta.topics:
                logger.info("Kafka topic '%s' not found. Creating it...", settings.WHALE_ALERTS_TOPIC)
                futures = admin.create_topics([NewTopic(settings.WHALE_ALERTS_TOPIC, num_partitions=3, replication_factor=1)])
                for topic, fut in futures.items():
                    fut.result()
                    logger.info("Kafka topic '%s' created successfully", topic)
        except Exception as e:
            logger.warning("Failed to verify/create Kafka topics: %s", e)

        whale_consumer_task = asyncio.create_task(consume_whale_alerts())
        app.state.whale_consumer_task = whale_consumer_task
        yield
    except Exception:
        logger.exception("Server startup failed")
        raise
    finally:
        if whale_consumer_task:
            whale_consumer_task.cancel()
            with suppress(asyncio.CancelledError):
                await whale_consumer_task

        await manager.stop_ping_loop()


        
        if hasattr(app.state, 'es_client'):
            await app.state.es_client.close()

        try:
            await redis_manager.disconnect()
        except Exception:
            logger.exception("Redis disconnect failed")

        try:
            await close_db_pool()
        except Exception:
            logger.exception("Server shutdown failed")


app = FastAPI(
    title="Ethereum Real-time Dashboard API",
    description="Kafka and TimescaleDB backed real-time Ethereum dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

import time
from fastapi import Request
from fastapi.responses import JSONResponse
from src.backend.core.exceptions import DatabaseFetchError

@app.exception_handler(DatabaseFetchError)
async def database_fetch_error_handler(request: Request, exc: DatabaseFetchError):
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Database query failed",
            "path": str(request.url)
        },
    )


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000
    logger.info("API Request", extra={
        "method": request.method,
        "url": str(request.url.path),
        "status_code": response.status_code,
        "duration_ms": round(process_time, 2)
    })
    return response



app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


app.include_router(gas.router, prefix="/api/v1/metrics")
app.include_router(whales.router, prefix="/api/v1/whales")
app.include_router(tokens.router, prefix="/api/v1/tokens")
app.include_router(wallets.router, prefix="/api/v1/wallets")
app.include_router(whales.ws_router, prefix="/ws")


@app.get("/")
async def root():
    return {"message": "Ethereum Dashboard API is running."}

"""
uv run uvicorn src.backend.main:app --host 0.0.0.0 --port 8000 --reload
"""
