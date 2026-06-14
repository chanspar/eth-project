import asyncpg
from typing import List, Dict, Any

class TokenRepository:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_trending_tokens(self, hours: int, limit: int) -> List[Dict[str, Any]]:
        query = f"""
        SELECT t.address, t.symbol, t.name, COUNT(*) as transfer_count
        FROM token_transfers tt
        JOIN tokens t ON tt.token_address = t.address
        WHERE tt.timestamp >= NOW() - INTERVAL '{hours} hours'
        GROUP BY t.address, t.symbol, t.name
        ORDER BY transfer_count DESC
        LIMIT $1
        """
        rows = await self.conn.fetch(query, limit)
        return [dict(row) for row in rows]
