import asyncpg
from typing import List, Dict, Any

class WhaleRepository:
    """
    이더리움 네트워크에서 대량 대금 거래를 일으키는 '고래(Whale)' 거래 내역을 조회하는 데이터베이스 접근 클래스입니다.
    """
    def __init__(self, conn: asyncpg.Connection):
        """
        WhaleRepository 인스턴스를 초기화합니다.
        
        Args:
            conn (asyncpg.Connection): 활성화된 PostgreSQL 비동기 커넥션 객체
        """
        self.conn = conn


    async def get_recent_whales(self, threshold_wei: int, limit: int) -> List[Dict[str, Any]]:
        """
        임계값(threshold_wei) 이상의 큰 거래 대금을 가진 최근 고래 거래 내역 목록을 조회합니다.
        
        SQL 인젝션 방지를 위해 threshold_wei 변수를 바인딩 파라미터($1)로 전달합니다.
        
        Args:
            threshold_wei (int): 고래 거래로 판정할 최소 이체 금액 (Wei 단위)
            limit (int): 반환할 최대 거래 내역 개수
            
        Returns:
            List[Dict[str, Any]]: 최근 고래 거래 내역 목록
        """
        query = """
        SELECT 
            t.hash, 
            t.timestamp, 
            t.from_address, 
            t.to_address, 
            t.value,
            al_from.name as from_label,
            al_from.category as from_category,
            al_to.name as to_label,
            al_to.category as to_category
        FROM transactions t
        LEFT JOIN address_labels al_from ON t.from_address = al_from.address
        LEFT JOIN address_labels al_to ON t.to_address = al_to.address
        WHERE t.value >= $1
        ORDER BY t.timestamp DESC
        LIMIT $2
        """
        rows = await self.conn.fetch(query, threshold_wei, limit)
        return [dict(row) for row in rows]


