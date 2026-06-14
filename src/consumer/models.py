from pydantic import BaseModel, Field, AliasChoices, field_validator
from datetime import datetime, timezone
from typing import Optional, Union

class BlockModel(BaseModel):
    number: int
    timestamp: datetime = Field(validation_alias=AliasChoices('timestamp', 'block_timestamp', 'item_timestamp'))

def parse_eth_timestamp(v: Union[str, int, float, None]) -> datetime:
    """
    이더리움의 시간 데이터(v)를 받아서 파이썬의 datetime 객체로 변환해 주는 함수
    """
    if not v:
        return datetime.now(timezone.utc)
    try:
        # ethereum-etl은 항상 초(s) 단위의 유닉스 타임을 반환하므로 바로 변환합니다.
        return datetime.fromtimestamp(float(v), tz=timezone.utc)
    except Exception:
        return datetime.now(timezone.utc)

class TransactionModel(BaseModel):
    hash: str
    # ethereum-etl은 block_timestamp 필드로 시간을 반환할 때가 많습니다.
    # 외부에서 들어오는 데이터의 키 이름이 timestamp, block_timestamp, item_timestamp 중 무엇이든 상관없이 모두 이 timestamp라는 하나의 변수로 연결해서 받도록 설정
    timestamp: datetime = Field(validation_alias=AliasChoices('timestamp', 'block_timestamp', 'item_timestamp'))
    from_address: str
    to_address: Optional[str] = None
    value: int = Field(default=0)
    gas_price: int = Field(default=0)
    
    # timestamp 필드에 값이 들어가기 전에 실행되는 검증기
    # parse_eth_timestamp를 통과 시켜 datetime객체로 정재한뒤 저장
    @field_validator("timestamp", mode="before")
    @classmethod # 클래스 차원에서 처리해야 하는 일이라 @classmethod 데코레이터 사용
    def validate_timestamp(cls, v):
        return parse_eth_timestamp(v)

class TokenTransferModel(BaseModel):
    transaction_hash: str
    log_index: int
    # 현재 시간 꼼수 삭제! 반드시 외부(Web3 RPC나 DB)에서 정확한 블록 타임스탬프를 가져와서 넣어줘야 합니다.
    timestamp: Optional[datetime] = Field(default=None, validation_alias=AliasChoices('timestamp', 'block_timestamp', 'item_timestamp'))
    block_number: int
    token_address: str
    from_address: str
    to_address: str
    value: int = Field(default=0)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp(cls, v):
        return parse_eth_timestamp(v)
