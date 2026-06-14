import asyncpg

class GasRepository:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def get_average_gas_price_last_5_minutes(self) -> float:
        query = """
        SELECT AVG(gas_price) as avg_gas_price
        FROM transactions
        WHERE timestamp >= NOW() - INTERVAL '5 minutes'
        """
        row = await self.conn.fetchrow(query)
        if row and row['avg_gas_price'] is not None:
            return float(row['avg_gas_price'])
        return 0.0
