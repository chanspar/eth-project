import asyncpg
from typing import List, Dict, Any

class WalletRepository:
    """
    특정 이더리움 지갑 주소의 거래 내역(ETH 트랜잭션 및 토큰 이체)을 조회하는 데이터베이스 접근 클래스입니다.
    """
    def __init__(self, conn: asyncpg.Connection):
        """
        WalletRepository 인스턴스를 초기화합니다.
        
        Args:
            conn (asyncpg.Connection): 활성화된 PostgreSQL 비동기 커넥션 객체
        """
        self.conn = conn

    async def get_eth_transactions(self, address: str, limit: int) -> List[Dict[str, Any]]:
        """
        특정 지갑 주소와 관련된 일반 ETH 트랜잭션 목록을 조회합니다. (송신 및 수신 내역 포함)
        
        Args:
            address (str): 조회할 지갑 주소 (0x 형식)
            limit (int): 반환할 최대 트랜잭션 개수
            
        Returns:
            List[Dict[str, Any]]: ETH 트랜잭션 내역 목록 (해시, 타임스탬프, 송수신 주소, 이체 금액, 가스비 등)
        """
        query = """
        SELECT 
            sub.hash, 
            sub.timestamp, 
            sub.from_address, 
            sub.to_address, 
            sub.value, 
            sub.gas_price,
            al_from.name as from_label,
            al_from.category as from_category,
            al_to.name as to_label,
            al_to.category as to_category
        FROM (
            (SELECT hash, timestamp, from_address, to_address, value, gas_price 
             FROM transactions WHERE from_address = $1 ORDER BY timestamp DESC LIMIT $2)
            UNION ALL
            (SELECT hash, timestamp, from_address, to_address, value, gas_price 
             FROM transactions WHERE to_address = $1 ORDER BY timestamp DESC LIMIT $2)
        ) as sub
        LEFT JOIN address_labels al_from ON sub.from_address = al_from.address
        LEFT JOIN address_labels al_to ON sub.to_address = al_to.address
        ORDER BY sub.timestamp DESC
        LIMIT $2
        """
        rows = await self.conn.fetch(query, address, limit)
        return [dict(row) for row in rows]

    async def get_token_transfers(self, address: str, limit: int) -> List[Dict[str, Any]]:
        """
        특정 지갑 주소와 관련된 ERC20 토큰의 이체(Transfer) 내역 목록을 조회합니다. (송신 및 수신 내역 포함)
        
        Args:
            address (str): 조회할 지갑 주소 (0x 형식)
            limit (int): 반환할 최대 이체 내역 개수
            
        Returns:
            List[Dict[str, Any]]: 토큰 이체 내역 목록 (해시, 타임스탬프, 송수신 주소, 금액, 토큰 심볼 및 소수점 자릿수 등)
        """
        query = """
        SELECT 
            sub.transaction_hash as hash, 
            sub.timestamp, 
            sub.from_address, 
            sub.to_address, 
            sub.value, 
            t.symbol, 
            t.decimals,
            al_from.name as from_label,
            al_from.category as from_category,
            al_to.name as to_label,
            al_to.category as to_category
        FROM (
            (SELECT transaction_hash, timestamp, from_address, to_address, value, token_address 
             FROM token_transfers WHERE from_address = $1 ORDER BY timestamp DESC LIMIT $2)
            UNION ALL
            (SELECT transaction_hash, timestamp, from_address, to_address, value, token_address 
             FROM token_transfers WHERE to_address = $1 ORDER BY timestamp DESC LIMIT $2)
        ) as sub
        LEFT JOIN tokens t ON sub.token_address = t.address
        LEFT JOIN address_labels al_from ON sub.from_address = al_from.address
        LEFT JOIN address_labels al_to ON sub.to_address = al_to.address
        ORDER BY sub.timestamp DESC
        LIMIT $2
        """
        rows = await self.conn.fetch(query, address, limit)
        return [dict(row) for row in rows]

