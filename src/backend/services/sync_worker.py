import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer
from elasticsearch import AsyncElasticsearch
from src.backend.core.config import settings

logger = logging.getLogger(__name__)

class ElasticSyncWorker:
    """
    Kafka의 token-events 토픽을 구독하여 Elasticsearch에 토큰 활동 지표를 업데이트하는 백그라운드 워커입니다.
    """
    def __init__(self, es_client: AsyncElasticsearch):
        self.es_client = es_client
        self.consumer = AIOKafkaConsumer(
            settings.TOKEN_EVENTS_TOPIC,
            bootstrap_servers=settings.KAFKA_BROKER,
            group_id="es-sync-worker-group",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest"
        )
        self._task = None

    async def start(self):
        await self.consumer.start()
        self._task = asyncio.create_task(self._consume_loop())
        logger.info("ElasticSyncWorker started.")

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        await self.consumer.stop()
        logger.info("ElasticSyncWorker stopped.")

    async def _consume_loop(self):
        try:
            async for msg in self.consumer:
                event = msg.value
                address = event.get("address")
                
                if address:
                    # Elasticsearch Update API를 이용해 검색 우선순위(예: transfer_count 등)를 증분 업데이트할 수도 있습니다.
                    # 여기서는 간단히 이벤트를 로깅하고 무시합니다. (트렌드 카운트는 DB에서 직접 조회하므로)
                    # 만약 ES에서 인기순 정렬을 위해 카운트 필드를 둔다면 아래와 같이 업데이트 가능합니다.
                    """
                    try:
                        await self.es_client.update(
                            index="tokens",
                            id=address,
                            body={
                                "script": {
                                    "source": "ctx._source.transfer_count = (ctx._source.transfer_count ?: 0) + 1",
                                    "lang": "painless"
                                },
                                "upsert": {
                                    "address": address,
                                    "transfer_count": 1
                                }
                            }
                        )
                    except Exception as e:
                        logger.error(f"Failed to update ES token {address}: {e}")
                    """
                    pass
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in ElasticSyncWorker loop: {e}")
