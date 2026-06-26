import asyncio
import asyncpg
import logging
from elasticsearch import AsyncElasticsearch

# 로거 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

POSTGRES_DSN = "postgresql://user:password@localhost:5432/eth_data"
ELASTICSEARCH_URL = "http://localhost:9200"
INDEX_NAME = "tokens"

async def bootstrap_es_tokens():
    es = AsyncElasticsearch([ELASTICSEARCH_URL])
    
    # ES 인덱스 생성
    if await es.indices.exists(index=INDEX_NAME):
        logger.info(f"인덱스 '{INDEX_NAME}'가 이미 존재합니다. 삭제 후 다시 생성합니다.")
        await es.indices.delete(index=INDEX_NAME)

    mapping = {
        "mappings": {
            "properties": {
                "address": {"type": "keyword"},
                "symbol": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                },
                "name": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                },
                "decimals": {"type": "short"}
            }
        }
    }
    
    await es.indices.create(index=INDEX_NAME, body=mapping)
    logger.info(f"인덱스 '{INDEX_NAME}' 생성 완료")

    # DB 연결 및 토큰 데이터 가져오기
    conn = await asyncpg.connect(POSTGRES_DSN)
    try:
        query = "SELECT address, symbol, name, decimals FROM tokens"
        rows = await conn.fetch(query)
        logger.info(f"Postgres에서 {len(rows)}개의 토큰 데이터를 가져왔습니다.")

        # ES Bulk indexing
        actions = []
        for row in rows:
            action = {
                "index": {"_index": INDEX_NAME, "_id": row['address']}
            }
            doc = {
                "address": row['address'],
                "symbol": row['symbol'],
                "name": row['name'],
                "decimals": row['decimals']
            }
            actions.append(action)
            actions.append(doc)

        if actions:
            response = await es.bulk(operations=actions)
            if response['errors']:
                logger.error("Bulk indexing 중 오류 발생!")
            else:
                logger.info("모든 토큰 데이터 색인 완료.")
        else:
            logger.info("색인할 데이터가 없습니다.")
            
    except Exception as e:
        logger.error(f"오류 발생: {e}")
    finally:
        await conn.close()
        await es.close()

if __name__ == "__main__":
    asyncio.run(bootstrap_es_tokens())
