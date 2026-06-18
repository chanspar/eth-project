from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime

class GasMetricsResponse(BaseModel):
    average_gas_price_gwei: float = Field(description="최근 5분간 평균 가스비 (Gwei)")

class WhaleTransaction(BaseModel):
    hash: str = Field(description="트랜잭션 고유 해시값")
    timestamp: datetime = Field(description="트랜잭션 블록 확정 시각")
    from_address: str = Field(description="송신 지갑 주소")
    to_address: str = Field(description="수신 지갑 주소")
    value_eth: float = Field(description="이체된 이더리움 금액 (ETH 단위)")
    from_label: Optional[str] = Field(None, description="송신지 지갑 라벨명")
    to_label: Optional[str] = Field(None, description="수신지 지갑 라벨명")
    from_category: Optional[str] = Field(None, description="송신지 지갑 카테고리")
    to_category: Optional[str] = Field(None, description="수신지 지갑 카테고리")

class WhaleListResponse(BaseModel):
    whales: List[WhaleTransaction] = Field(description="최근 탐지된 고래 거래 내역 목록")

class TrendingToken(BaseModel):
    address: str = Field(description="토큰 스마트 계약(Contract) 주소")
    symbol: str = Field(description="토큰 심볼 (예: USDT)")
    name: str = Field(description="토큰 공식 명칭 (예: Tether USD)")
    transfer_count: int = Field(description="조회 기간 내 이체 발생 횟수")

class TrendingTokensResponse(BaseModel):
    trending_tokens: List[TrendingToken] = Field(description="최근 거래 트렌드 상위 인기 토큰 목록")

class EthTransactionHistory(BaseModel):
    hash: str = Field(description="트랜잭션 고유 해시값")
    timestamp: datetime = Field(description="트랜잭션 블록 확정 시각")
    type: str = Field(description="거래 성격 구분 (IN: 수신, OUT: 송신)")
    from_address: str = Field(description="송신 지갑 주소")
    to_address: str = Field(description="수신 지갑 주소")
    value_eth: float = Field(description="이체된 이더리움 금액 (ETH 단위)")
    gas_price_gwei: Optional[float] = Field(None, description="거래 가스 수수료 (Gwei 단위)")
    from_label: Optional[str] = Field(None, description="송신지 지갑 라벨명")
    to_label: Optional[str] = Field(None, description="수신지 지갑 라벨명")
    from_category: Optional[str] = Field(None, description="송신지 지갑 카테고리")
    to_category: Optional[str] = Field(None, description="수신지 지갑 카테고리")

class TokenTransferHistory(BaseModel):
    hash: str = Field(description="트랜잭션 고유 해시값")
    timestamp: datetime = Field(description="트랜잭션 블록 확정 시각")
    type: str = Field(description="거래 성격 구분 (IN: 수신, OUT: 송신)")
    from_address: str = Field(description="송신 지갑 주소")
    to_address: str = Field(description="수신 지갑 주소")
    symbol: str = Field(description="토큰 심볼 (예: USDC)")
    value: float = Field(description="이체된 토큰 수량")
    from_label: Optional[str] = Field(None, description="송신지 지갑 라벨명")
    to_label: Optional[str] = Field(None, description="수신지 지갑 라벨명")
    from_category: Optional[str] = Field(None, description="송신지 지갑 카테고리")
    to_category: Optional[str] = Field(None, description="수신지 지갑 카테고리")

class WalletHistoryResponse(BaseModel):
    address: str = Field(description="조회 요청한 지갑 주소")
    eth_transactions: List[EthTransactionHistory] = Field(description="지갑의 최근 ETH 거래 내역 목록")
    token_transfers: List[TokenTransferHistory] = Field(description="지갑의 최근 ERC20 토큰 이체 내역 목록")

