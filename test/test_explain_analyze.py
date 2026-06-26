import asyncio
import asyncpg
from src.backend.core.config import settings

async def run_explain():
    print("데이터베이스 연결 중...")
    conn = await asyncpg.connect(settings.POSTGRES_DSN)
    test_address = '0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045' # Vitalik's address as an example
    
    print("\n" + "="*50)
    print("1. 기존 방식 (OR 조건 사용)")
    print("="*50)
    or_query = """
    EXPLAIN ANALYZE 
    SELECT t.hash 
    FROM transactions t 
    WHERE t.from_address = $1 OR t.to_address = $1 
    ORDER BY t.timestamp DESC 
    LIMIT 50
    """
    or_plan = await conn.fetch(or_query, test_address)
    for row in or_plan:
        print(row[0])
        
    print("\n" + "="*50)
    print("2. 개선된 방식 (UNION ALL 사용)")
    print("="*50)
    union_query = """
    EXPLAIN ANALYZE 
    SELECT * FROM (
        (SELECT t.hash, t.timestamp FROM transactions t WHERE t.from_address = $1 ORDER BY t.timestamp DESC LIMIT 50)
        UNION ALL
        (SELECT t.hash, t.timestamp FROM transactions t WHERE t.to_address = $1 ORDER BY t.timestamp DESC LIMIT 50)
    ) as sub 
    ORDER BY timestamp DESC 
    LIMIT 50
    """
    union_plan = await conn.fetch(union_query, test_address)
    for row in union_plan:
        print(row[0])
        
    await conn.close()

if __name__ == "__main__":
    asyncio.run(run_explain())
