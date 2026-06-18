import asyncpg

class GasRepository:
    """
    이더리움 네트워크의 가스비(Gas Price) 관련 데이터를 조회하는 데이터베이스 접근 클래스입니다.
    """
    def __init__(self, conn: asyncpg.Connection):
        """
        GasRepository 인스턴스를 초기화합니다.
        
        Args:
            conn (asyncpg.Connection): 활성화된 PostgreSQL 비동기 커넥션 객체
        """
        self.conn = conn

    async def get_average_gas_price_last_5_minutes(self) -> float:
        """
        최근 5분 동안 발생한 이더리움 트랜잭션들의 평균 가스비를 조회합니다.
        
        Returns:
            float: 최근 5분 평균 가스비 (Wei 단위, 결과가 없을 시 0.0 반환)
        """
        query = """
        SELECT AVG(gas_price) as avg_gas_price
        FROM transactions
        WHERE timestamp >= NOW() - INTERVAL '5 minutes'
        """
        row = await self.conn.fetchrow(query)
        if row and row['avg_gas_price'] is not None:
            return float(row['avg_gas_price'])
        return 0.0

