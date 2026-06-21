import asyncpg
from fastapi import APIRouter, Depends, Request, Query
from typing import List
from src.backend.core.db import get_db
from src.backend.repositories.token_repo import TokenRepository
from src.backend.services.token_service import TokenService
from src.backend.models.schemas import TrendingTokensResponse, TokenSearchResponse, TokenTrendsResponse

router = APIRouter()

def get_token_service(conn: asyncpg.Connection = Depends(get_db)) -> TokenService:
    repo = TokenRepository(conn)
    return TokenService(repo)

@router.get("/", response_model=List[TokenSearchResponse])
async def get_all_tokens(limit: int = 100, offset: int = 0, prefix: str = Query(None, description="Filter by starting letter"), service: TokenService = Depends(get_token_service)):
    """
    데이터베이스에 저장된 모든 토큰의 목록을 반환합니다. prefix가 주어지면 해당 문자로 시작하는 토큰만 반환합니다.
    """
    return await service.get_all_tokens(limit=limit, offset=offset, prefix=prefix)

@router.get("/trending", response_model=TrendingTokensResponse)
async def get_trending_tokens(limit: int = 10, hours: int = 1, service: TokenService = Depends(get_token_service)):
    """
    최근 N시간 동안 가장 전송 건수가 많은 핫 토큰 랭킹을 반환합니다.
    """
    return await service.get_trending_tokens(limit=limit, hours=hours)

@router.get("/search", response_model=List[TokenSearchResponse])
async def search_tokens(q: str = Query(..., min_length=1), request: Request = None):
    """
    Elasticsearch를 사용하여 토큰 심볼 또는 이름으로 토큰을 검색합니다.
    """
    es = request.app.state.es_client
    query = {
        "multi_match": {
            "query": q,
            "fields": ["symbol^2", "name"],
            "fuzziness": "AUTO"
        }
    }
    resp = await es.search(index="tokens", query=query, size=10)
    results = []
    for hit in resp['hits']['hits']:
        source = hit['_source']
        results.append(TokenSearchResponse(
            address=source.get('address'),
            symbol=source.get('symbol'),
            name=source.get('name'),
            decimals=source.get('decimals')
        ))
    return results

@router.get("/{address}/trends", response_model=TokenTrendsResponse)
async def get_token_trends(address: str, bucket_width: str = '1 hour', limit: int = 24, service: TokenService = Depends(get_token_service)):
    """
    특정 토큰의 시간대별 이체 횟수 및 이체량 트렌드를 반환합니다.
    """
    trends = await service.get_token_trends(address, bucket_width, limit)
    return TokenTrendsResponse(address=address, trends=trends)
