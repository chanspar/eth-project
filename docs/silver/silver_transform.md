# Silver Layer: `txn_enriched` 테이블 정의서

`src/silver/transform/txn_enriched.py` 실행 시 생성되는 Silver 레이어의 핵심 테이블인 `txn_enriched`의 구조와 예시 데이터입니다.
이 테이블은 Bronze 레이어의 **Transactions**와 **Receipts** 데이터를 조인하여 분석에 최적화된 형태로 정제한 데이터입니다.

## 1. 테이블 스키마 (Schema)

| 컬럼명 | 타입 | 설명 | 비고 |
| :--- | :--- | :--- | :--- |
| **hash** | String | 트랜잭션 해시 | PK 역할을 수행하는 고유 ID |
| **block_number** | Long | 블록 번호 | |
| **block_timestamp** | Long | 블록 생성 시각 (Unix Timestamp) | |
| **from_address** | String | 송신자 주소 | |
| **to_address** | String | 수신자 주소 | 컨트랙트 배포 시 null |
| **value_eth** | Decimal(38,18) | 송금 금액 (ETH 단위) | 정밀한 계산 결과값 |
| **is_success** | Boolean | 트랜잭션 성공 여부 | `status == 1` |
| **dt** | Date | 파티션 날짜 (YYYY-MM-DD) | **Partition Column** |

---

## 2. 저장 위치 및 포맷
- **경로**: `gs://[BUCKET_NAME]/silver/ethereum/txn_enriched/dt=YYYY-MM-DD/`
- **포맷**: Parquet (Snappy Compressed)
- **파티션**: `dt` 컬럼 기준

---

## 3. 주요 특징 (Enrichment Logic)
1. **데이터 정제 및 ETH 변환**: `value` (Wei) 필드를 읽기 쉬운 `value_eth` (Decimal)로 변환하여 분석 편의성을 높였습니다.
2. **상태값 통합**: Receipts의 `status` 필드를 `is_success` (Boolean)로 변환하여 트랜잭션 성공 여부를 즉시 확인할 수 있습니다.
3. **조인 최적화**: Bronze의 `transactions`와 `receipts` 두 테이블을 `hash` 기준으로 조인하여 분석에 꼭 필요한 정보만 남기고 처리 속도를 극대화했습니다.

<br>

---

# Silver Layer: `whale_txns` 테이블 정의서

`src/silver/transform/whale_txns.py` 실행 시 생성되는 **Whale Intelligence** 테이블입니다. 
대규모 자금 이동을 추적할 뿐만 아니라, 엔티티 정규화 및 흐름 유형 분류를 통해 고래의 의도를 파악할 수 있는 인텔리전스를 제공합니다.

## 1. 주요 특징 (Key Features)

1. **엔티티 정규화 (Entity Normalization)**: "Binance 14", "Binance 15"와 같은 개별 지갑 이름을 "Binance"라는 하나의 브랜드 엔티티로 통합하여 집계 분석을 용이하게 합니다.
2. **고래 등급 분류 (Whale Tiering)**: 당일 전송 금액을 기준으로 고래의 규모를 분류합니다.
   - `Humpback` (1,000+ ETH), `Whale` (500+ ETH), `Shark` (100+ ETH), `Crab` (기타)
3. **흐름 유형 자동 분류 (Flow Classification)**: 자금의 이동 경로를 분석하여 유형을 정의합니다.
   - `CEX_Deposit`, `CEX_Withdrawal`, `CEX_to_CEX`, `DEX_Swap`, `Bridge`, `Private`, `Internal`
4. **누적 행동 분석 (Cumulative Stats)**: 윈도우 함수를 통해 주소별 당일 누적 송금액, 수신액, 거래 횟수를 실시간 계산하여 제공합니다.
5. **다크 머니 탐지 (Private Tx)**: MEV 봇이나 프라이빗 RPC를 통한 비공개 트랜잭션 여부를 감지합니다.

## 2. 테이블 스키마 (Schema)

| 컬럼명 | 타입 | 설명 | 비고 |
| :--- | :--- | :--- | :--- |
| **hash** | String | 트랜잭션 해시 | |
| **block_timestamp** | Long | 블록 생성 시각 | |
| **hour** | Integer | 발생 시간 (0-23) | 시간대별 분석용 |
| **dt** | Date | 파티션 날짜 | |
| **from_address** | String | 송신자 주소 | |
| **from_entity** | String | 송신자 통합 엔티티명 | (예: Binance, Upbit, Unknown) |
| **from_category** | String | 송신자 카테고리 | (예: CEX, DEX, Whale) |
| **to_address** | String | 수신자 주소 | |
| **to_entity** | String | 수신자 통합 엔티티명 | |
| **to_category** | String | 수신자 카테고리 | |
| **value_eth** | Double | 트랜잭션 금액 (ETH) | |
| **cumul_sent_eth** | Double | 송신자 당일 누적 송출량 | Window Function |
| **cumul_tx_count** | Long | 송신자 당일 누적 거래수 | Window Function |
| **cumul_recv_eth** | Double | 수신자 당일 누적 수신량 | Window Function |
| **whale_tier** | String | 고래 등급 | Humpback/Whale/Shark/Crab |
| **flow_type** | String | 자금 흐름 유형 | CEX_Deposit, DEX_Swap 등 |
| **is_private_transaction**| Boolean | 비공개 트랜잭션 여부 | Flashbots 등 사용 시 True |
| **flag_high_freq** | Boolean | 고빈도 거래 플래그 | 짧은 시간 내 반복 거래 발생 시 |

---

## 3. 데이터 예시 (Example Rows)

| from_entity | to_entity | value_eth | whale_tier | flow_type | cumul_sent_eth |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `Unknown` | `Binance` | 2,500.0 | **Humpback** | **CEX_Deposit** | 2,500.0 |
| `Binance` | `Unknown` | 500.0 | **Whale** | **CEX_Withdrawal** | 1,500.0 |
| `Unknown` | `Uniswap V3` | 1,200.0 | **Humpback** | **DEX_Swap** | 1,200.0 |
| `Unknown` | `Unknown` | 300.0 | **Shark** | **Private** | 300.0 |

<br>

---

# Silver Layer: `token_flow` 테이블 정의서

`src/silver/transform/token_flow.py` 실행 시 생성되는 주요 ERC-20 토큰의 이동 흐름을 추적하는 테이블입니다.

## 1. 주요 특징 (Key Features)

1. **주요 토큰 정밀 타격**: 모든 토큰이 아닌 `top1000_erc20_tokens` 마스터 데이터에 포함된 가치 있는 토큰들만 조인하여 분석 효율을 극대화합니다. (Bronze `contracts` 조인 제거로 성능 최적화)
2. **트랜잭션 신뢰도 확보**: `receipts` 데이터를 결합하여 실제로 **성공(`status == 1`)**한 토큰 전송 내역만 필터링합니다.
3. **단위 정규화 (Amount Decimals)**: 각 토큰의 `decimals` 정보를 활용하여 실제 유통되는 수량 단위로 정규화된 `amount` 컬럼을 생성합니다.
4. **상세 라벨링**: `load_address_labels`를 통해 송수신 지갑의 정체(CEX, DEX, 고래 등)와 카테고리를 한 번에 파악합니다.

## 2. 테이블 스키마 (Schema)

| 컬럼명 | 타입 | 설명 | 비고 |
| :--- | :--- | :--- | :--- |
| **transaction_hash** | String | 트랜잭션 해시 | |
| **status** | Integer | 트랜잭션 성공 여부 | 1: 성공 |
| **block_timestamp** | Long | 블록 생성 시각 | |
| **hour** | Integer | 발생 시간 (0-23) | |
| **dt** | Date | 파티션 날짜 | |
| **token_address** | String | 토큰 컨트랙트 주소 | |
| **symbol** | String | 토큰 심볼 | (예: USDC, PEPE) |
| **token_name** | String | 토큰 전체 이름 | (예: Tether USD) |
| **from_address** | String | 송신자 주소 | |
| **from_label** | String | 송신자 라벨명 | |
| **from_category** | String | 송신자 카테고리 | (예: CEX, DEX, DeFi) |
| **to_address** | String | 수신자 주소 | |
| **to_label** | String | 수신자 라벨명 | |
| **to_category** | String | 수신자 카테고리 | |
| **amount** | Double | 정규화된 토큰 전송량 | `value / 10^decimals` |

---

## 3. 데이터 예시 (Example Rows)

| symbol | amount | from_label | to_label | from_category | to_category |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `USDT` | 50,000.0 | `Binance` | `Unknown` | **CEX** | Unknown |
| `WETH` | 15.5 | `Unknown` | `Uniswap V3` | Unknown | **DEX** |
| `PEPE` | 100M | `Upbit` | `Unknown` | **CEX** | Unknown |
| `USDC` | 120,000.0 | `Unknown` | `Aave` | Unknown | **DeFi** |

---

## 4. 저장 위치 및 포맷
- **경로**: `gs://[BUCKET_NAME]/silver/ethereum/token_flow/dt=YYYY-MM-DD/`
- **포맷**: Parquet
- **파티션**: `dt`
