from fastapi import HTTPException
import logging
from src.backend.repositories.wallet_repo import WalletRepository
from src.backend.models.schemas import WalletHistoryResponse, EthTransactionHistory, TokenTransferHistory

logger = logging.getLogger(__name__)

class WalletService:
    def __init__(self, repo: WalletRepository):
        self.repo = repo
        
    async def get_wallet_history(self, address: str, limit: int) -> WalletHistoryResponse:
        address = address.lower()
        try:
            eth_rows = await self.repo.get_eth_transactions(address, limit)
            token_rows = await self.repo.get_token_transfers(address, limit)
            
            eth_txs = []
            for row in eth_rows:
                eth_txs.append(EthTransactionHistory(
                    hash=row['hash'],
                    timestamp=row['timestamp'],
                    type="OUT" if row['from_address'] == address else "IN",
                    from_address=row['from_address'],
                    to_address=row['to_address'],
                    value_eth=float(row['value']) / 1e18,
                    gas_price_gwei=float(row['gas_price']) / 1e9 if row['gas_price'] else None
                ))
                
            token_txs = []
            for row in token_rows:
                decimals = row['decimals'] if row['decimals'] else 18
                token_txs.append(TokenTransferHistory(
                    hash=row['hash'],
                    timestamp=row['timestamp'],
                    type="OUT" if row['from_address'] == address else "IN",
                    from_address=row['from_address'],
                    to_address=row['to_address'],
                    symbol=row['symbol'] or "UNKNOWN",
                    value=float(row['value']) / (10 ** decimals)
                ))
                
            return WalletHistoryResponse(
                address=address,
                eth_transactions=eth_txs,
                token_transfers=token_txs
            )
        except Exception as e:
            logger.error(f"Failed to fetch wallet history for {address}: {e}")
            raise HTTPException(status_code=500, detail="Database query failed")
