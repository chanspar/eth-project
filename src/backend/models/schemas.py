from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class GasMetricsResponse(BaseModel):
    average_gas_price_gwei: float = Field(..., description="최근 5분간 평균 가스비 (Gwei)")

class WhaleTransaction(BaseModel):
    hash: str
    timestamp: datetime
    from_address: str
    to_address: str
    value_eth: float

class WhaleListResponse(BaseModel):
    whales: List[WhaleTransaction]

class TrendingToken(BaseModel):
    address: str
    symbol: str
    name: str
    transfer_count: int

class TrendingTokensResponse(BaseModel):
    trending_tokens: List[TrendingToken]

class EthTransactionHistory(BaseModel):
    hash: str
    timestamp: datetime
    type: str = Field(..., description="IN or OUT")
    from_address: str
    to_address: str
    value_eth: float
    gas_price_gwei: Optional[float]

class TokenTransferHistory(BaseModel):
    hash: str
    timestamp: datetime
    type: str = Field(..., description="IN or OUT")
    from_address: str
    to_address: str
    symbol: str
    value: float

class WalletHistoryResponse(BaseModel):
    address: str
    eth_transactions: List[EthTransactionHistory]
    token_transfers: List[TokenTransferHistory]
