from fastapi import HTTPException
import logging
from src.backend.repositories.wallet_repo import WalletRepository
from src.backend.models.schemas import WalletHistoryResponse, EthTransactionHistory, TokenTransferHistory

logger = logging.getLogger(__name__)

class WalletService:
    """
    특정 이더리움 지갑 주소의 종합 거래 이력(일반 ETH 및 ERC20 토큰 이체 내역)을 수집하고 가공하는 비즈니스 서비스 클래스입니다.
    """
    def __init__(self, repo: WalletRepository):
        """
        WalletService 인스턴스를 초기화합니다.
        
        Args:
            repo (WalletRepository): 지갑 데이터베이스 접근 리포지토리 객체
        """
        self.repo = repo
        
    async def get_wallet_history(self, address: str, limit: int) -> WalletHistoryResponse:
        """
        특정 지갑 주소의 이더리움 트랜잭션 내역 및 토큰 이체 목록을 가져와 포맷팅합니다.
        
        각 내역별 수신(IN)/송신(OUT) 판정 시 안전하게 소문자 변환(lower)을 수행하여 대소문자 매칭 오작동을 차단합니다.
        
        Args:
            address (str): 조회할 지갑 주소
            limit (int): 반환할 거래 기록 최대 개수
            
        Returns:
            WalletHistoryResponse: 지갑의 ETH 거래 내역 및 ERC20 이체 내역 목록이 통합된 응답 DTO
            
        Raises:
            HTTPException: 데이터베이스 조회 혹은 포맷팅 처리 중 오류 발생 시 500 상태 코드 반환
        """
        address = address.lower()
        try:
            eth_rows = await self.repo.get_eth_transactions(address, limit)
            token_rows = await self.repo.get_token_transfers(address, limit)
            
            eth_txs = []
            for row in eth_rows:
                # 대소문자 차이로 인한 수신/송신 오판정 방지를 위해 lower() 안전 매칭 적용
                from_addr = row['from_address'].lower() if row['from_address'] else ""
                eth_txs.append(EthTransactionHistory(
                    hash=row['hash'],
                    timestamp=row['timestamp'],
                    type="OUT" if from_addr == address else "IN",
                    from_address=row['from_address'],
                    to_address=row['to_address'],
                    value_eth=float(row['value']) / 1e18,
                    gas_price_gwei=float(row['gas_price']) / 1e9 if row['gas_price'] else None,
                    from_label=row.get('from_label'),
                    to_label=row.get('to_label'),
                    from_category=row.get('from_category'),
                    to_category=row.get('to_category')
                ))
                
            token_txs = []
            for row in token_rows:
                decimals = row['decimals'] if row['decimals'] else 18
                # 대소문자 차이로 인한 수신/송신 오판정 방지를 위해 lower() 안전 매칭 적용
                from_addr = row['from_address'].lower() if row['from_address'] else ""
                token_txs.append(TokenTransferHistory(
                    hash=row['hash'],
                    timestamp=row['timestamp'],
                    type="OUT" if from_addr == address else "IN",
                    from_address=row['from_address'],
                    to_address=row['to_address'],
                    symbol=row['symbol'] or "UNKNOWN",
                    value=float(row['value']) / (10 ** decimals),
                    from_label=row.get('from_label'),
                    to_label=row.get('to_label'),
                    from_category=row.get('from_category'),
                    to_category=row.get('to_category')
                ))
                
            return WalletHistoryResponse(
                address=address,
                eth_transactions=eth_txs,
                token_transfers=token_txs
            )
        except Exception as e:
            logger.error(f"Failed to fetch wallet history for {address}: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")

