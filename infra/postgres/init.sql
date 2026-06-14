-- 1. Transactions Table (이더리움 이체 및 가스비 동향 추적용)
CREATE TABLE transactions (
    hash VARCHAR(66) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    value NUMERIC, -- 고래 알림용 (ETH 이체 금액)
    gas_price NUMERIC, -- 대시보드 가스비 추이 분석용
    PRIMARY KEY (hash, timestamp) -- TimescaleDB 제약조건: PK에 timestamp 포함 필수
);

-- TimescaleDB Hypertable 변환
SELECT create_hypertable('transactions', 'timestamp');

-- 2. Token Transfers Table (ERC20 토큰 이체 흐름 추적용)
CREATE TABLE token_transfers (
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    token_address VARCHAR(42) NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    value NUMERIC, -- 토큰 이체 금액
    PRIMARY KEY (transaction_hash, log_index, timestamp) -- TimescaleDB 제약조건: PK에 timestamp 포함 필수
);

-- TimescaleDB Hypertable 변환
SELECT create_hypertable('token_transfers', 'timestamp');

-- 3. Tokens Metadata Table (대시보드에 심볼 및 이름 표시용)
CREATE TABLE tokens (
    address VARCHAR(42) PRIMARY KEY,
    symbol VARCHAR(50),
    name VARCHAR(255),
    decimals SMALLINT
);

-- ==========================================
-- Indexing (비즈니스 로직 최적화)
-- ==========================================

-- 지갑 주소 검색 API용
CREATE INDEX idx_transactions_from_address ON transactions(from_address);
CREATE INDEX idx_transactions_to_address ON transactions(to_address);

-- 토큰 랭킹 및 지갑별 토큰 내역 조회 API용
CREATE INDEX idx_token_transfers_token_address ON token_transfers(token_address);
CREATE INDEX idx_token_transfers_from_address ON token_transfers(from_address);
CREATE INDEX idx_token_transfers_to_address ON token_transfers(to_address);
