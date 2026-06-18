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

