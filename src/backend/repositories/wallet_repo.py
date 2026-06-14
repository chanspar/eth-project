import asyncpg
from typing import List, Dict, Any

class WalletRepository:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_eth_transactions(self, address: str, limit: int) -> List[Dict[str, Any]]:
        query = """
        SELECT hash, timestamp, from_address, to_address, value, gas_price
        FROM transactions
        WHERE from_address = $1 OR to_address = $1
        ORDER BY timestamp DESC
        LIMIT $2
        """
        rows = await self.conn.fetch(query, address, limit)
        return [dict(row) for row in rows]

    async def get_token_transfers(self, address: str, limit: int) -> List[Dict[str, Any]]:
        query = """
        SELECT tt.transaction_hash as hash, tt.timestamp, tt.from_address, tt.to_address, tt.value, t.symbol, t.decimals
        FROM token_transfers tt
        LEFT JOIN tokens t ON tt.token_address = t.address
        WHERE tt.from_address = $1 OR tt.to_address = $1
        ORDER BY tt.timestamp DESC
        LIMIT $2
        """
        rows = await self.conn.fetch(query, address, limit)
        return [dict(row) for row in rows]
