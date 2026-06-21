import asyncpg
from typing import List, Dict, Any

class TokenRepository:
    """
    이더리움 토큰(ERC20) 메타데이터 및 거래 트렌드 데이터를 조회하는 데이터베이스 접근 클래스입니다.
    """
    def __init__(self, conn: asyncpg.Connection):
        """
        TokenRepository 인스턴스를 초기화합니다.
        
        Args:
            conn (asyncpg.Connection): 활성화된 PostgreSQL 비동기 커넥션 객체
        """
        self.conn = conn


    async def get_trending_tokens(self, hours: int, limit: int) -> List[Dict[str, Any]]:
        """
        최근 지정한 시간(hours) 동안 가장 거래 빈도가 높은 인기 토큰 목록을 조회합니다.
        
        SQL 인젝션 방지를 위해 INTERVAL 연산 시 바인딩 파라미터($1)를 곱하는 방식을 적용했습니다.
        
        Args:
            hours (int): 조회할 기준 시간 (예: 최근 24시간인 경우 24)
            limit (int): 반환할 인기 토큰 최대 개수
            
        Returns:
            List[Dict[str, Any]]: 인기 토큰 목록 (주소, 심볼, 이름, 거래 횟수 포함)
        """
        query = """
        SELECT t.address, t.symbol, t.name, COUNT(*) as transfer_count
        FROM token_transfers tt
        JOIN tokens t ON tt.token_address = t.address
        WHERE tt.timestamp >= NOW() - $1 * INTERVAL '1 hour'
        GROUP BY t.address, t.symbol, t.name
        ORDER BY transfer_count DESC
        LIMIT $2
        """
        rows = await self.conn.fetch(query, hours, limit) # hours: $1, limit: $2
        return [dict(row) for row in rows]

    async def get_token_trends_by_address(self, address: str, bucket_width: str = '1 hour', limit: int = 24) -> List[Dict[str, Any]]:
        """
        특정 토큰의 시간대별 이체 횟수 및 이체량 트렌드를 조회합니다.
        """
        query = """
        SELECT time_bucket($1::interval, timestamp) AS bucket,
               COUNT(*) as transfer_count,
               SUM(value) as total_value
        FROM token_transfers
        WHERE token_address = $2
          AND timestamp >= NOW() - ($1::interval * $3)
        GROUP BY bucket
        ORDER BY bucket ASC
        """
        rows = await self.conn.fetch(query, bucket_width, address, limit)
        return [dict(row) for row in rows]

    async def get_all_tokens(self, limit: int = 100, offset: int = 0, prefix: str = None) -> List[Dict[str, Any]]:
        """
        데이터베이스에 저장된 모든 토큰의 목록을 페이지네이션하여 조회합니다.
        
        Args:
            limit (int): 반환할 토큰 최대 개수
            offset (int): 건너뛸 레코드 수
            prefix (str, optional): 심볼의 시작 문자 필터
            
        Returns:
            List[Dict[str, Any]]: 토큰 목록
        """
        if prefix:
            query = """
            SELECT address, symbol, name, decimals
            FROM tokens
            WHERE symbol ILIKE $3 || '%'
            ORDER BY symbol ASC NULLS LAST
            LIMIT $1 OFFSET $2
            """
            rows = await self.conn.fetch(query, limit, offset, prefix)
        else:
            query = """
            SELECT address, symbol, name, decimals
            FROM tokens
            ORDER BY symbol ASC NULLS LAST
            LIMIT $1 OFFSET $2
            """
            rows = await self.conn.fetch(query, limit, offset)
        return [dict(row) for row in rows]
