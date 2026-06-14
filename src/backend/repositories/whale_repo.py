import asyncpg
from typing import List, Dict, Any

class WhaleRepository:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_recent_whales(self, threshold_wei: int, limit: int) -> List[Dict[str, Any]]:
        query = f"""
        SELECT hash, timestamp, from_address, to_address, value
        FROM transactions
        WHERE value >= {threshold_wei}
        ORDER BY timestamp DESC
        LIMIT $1
        """
        rows = await self.conn.fetch(query, limit)
        return [dict(row) for row in rows]
