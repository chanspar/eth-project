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

-- 4. Address Labels Table (거래소 및 주요 기관 지갑 라벨링용)
CREATE TABLE address_labels (
    address VARCHAR(42) PRIMARY KEY,
    name VARCHAR(255),
    category VARCHAR(100)
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

-- 고래 거래(Whale Alert) 검색 최적화용 부분 인덱스 (Partial Index)
-- value >= 100 ETH 인 데이터만 따로 인덱싱하여, 풀스캔 없이 즉시 상위 10개를 뽑을 수 있게 합니다.
CREATE INDEX idx_transactions_whale ON transactions(timestamp DESC) WHERE value >= 100000000000000000000;

-- ==========================================
-- TimescaleDB Compression (운영 환경 데이터 압축)
-- ==========================================

-- 1. Transactions 압축 설정
-- SegmentBy: 특정 지갑을 조회할 때 한 덩어리로 묶어서 가져올 수 있게 address 기준 묶음
-- OrderBy: 시간 역순으로 최신 데이터부터 빠르게 압축 해제할 수 있게 정렬
ALTER TABLE transactions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'from_address',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- 데이터가 쌓이고 7일이 지나면 백그라운드 워커가 자동으로 압축 (디스크 용량 90% 이상 절감)
SELECT add_compression_policy('transactions', INTERVAL '7 days');

-- 2. Token Transfers 압축 설정
ALTER TABLE token_transfers SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'token_address',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('token_transfers', INTERVAL '7 days');

-- ==========================================
-- TimescaleDB Retention (운영 환경 데이터 삭제 정책)
-- ==========================================

-- 대시보드 용도이므로 무한정 데이터를 쌓지 않고, 6개월이 지난 오래된 데이터는 자동 삭제하여 디스크 풀을 방지합니다.
SELECT add_retention_policy('transactions', INTERVAL '6 months');
SELECT add_retention_policy('token_transfers', INTERVAL '6 months');
