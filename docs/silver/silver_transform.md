# Silver Layer: `txn_enriched` 테이블 정의서

`src/silver/transform/txn_enriched.py` 실행 시 생성되는 Silver 레이어의 핵심 테이블인 `txn_enriched`의 구조와 예시 데이터입니다.
이 테이블은 Bronze 레이어의 **Transactions**, **Receipts**, **Blocks** 데이터를 조인하여 분석에 최적화된 형태로 정제한 데이터입니다.

## 1. 테이블 스키마 (Schema)

| 컬럼명 | 타입 | 설명 | 비고 |
| :--- | :--- | :--- | :--- |
| **hash** | String | 트랜잭션 해시 | PK 역할을 수행하는 고유 ID |
| **block_number** | Long | 블록 번호 | |
| **block_timestamp** | Long | 블록 생성 시각 (Unix Timestamp) | |
| **transaction_index** | Integer | 블록 내 트랜잭션 순서 | |
| **from_address** | String | 송신자 주소 | |
| **to_address** | String | 수신자 주소 | 컨트랙트 배포 시 null |
| **contract_address** | String | 생성된 컨트랙트 주소 | 컨트랙트 배포(deploy) 시에만 값이 존재 |
| **value_eth** | Decimal(38,18) | 송금 금액 (ETH 단위) | 정밀한 계산 결과값 |
| **gas** | Long | 요청한 가스 한도 (Gas Limit) | |
| **gas_used** | Decimal(38,0) | 실제 소모된 가스량 | Receipts 데이터에서 추출 |
| **effective_gas_price** | Decimal(38,0) | 실제 적용된 가스 단가 (Wei) | EIP-1559 등에 따른 최종 가격 |
| **tx_fee_eth** | Decimal(38,18) | 총 트랜잭션 수수료 (ETH) | 정밀한 계산 결과값 |
| **miner** | String | 블록 생성자(마이너) 주소 | Blocks 데이터에서 조인 |
| **transaction_type** | Integer | 트랜잭션 타입 코드 | 0, 1, 2, 3 등 |
| **tx_type_label** | String | 트랜잭션 타입 라벨 | legacy, eip1559, blob 등 |
| **is_success** | Boolean | 트랜잭션 성공 여부 | `status == 1` |
| **is_contract_call** | Boolean | 컨트랙트 호출 여부 | `input` 데이터 존재 여부로 판별 |
| **is_contract_deploy** | Boolean | 컨트랙트 배포 여부 | `contract_address` 존재 여부로 판별 |
| **input** | String | 입력 데이터 (Hex) | 스마트 컨트랙트 호출 시 사용되는 데이터 |
| **dt** | Date | 파티션 날짜 (YYYY-MM-DD) | **Partition Column** |

---

## 2. 저장 위치 및 포맷
- **경로**: `gs://[BUCKET_NAME]/silver/ethereum/txn_enriched/dt=YYYY-MM-DD/`
- **포맷**: Parquet (Snappy Compressed)
- **파티션**: `dt` 컬럼 기준

---

## 3. 데이터 예시 (Example Rows)

| hash | block_number | from_address | to_address | value_eth | tx_fee_eth | tx_type_label | is_success |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `0xabc...123` | 19,000,000 | `0xsender...` | `0xreceiver...` | 1.500000 | 0.000420 | eip1559 | true |
| `0xdef...456` | 19,000,001 | `0xuser...` | `0xcontract...` | 0.000000 | 0.001250 | legacy | true |
| `0x789...000` | 19,000,002 | `0xdeployer...` | *null* | 0.000000 | 0.015000 | eip1559 | true |
| `0x999...fff` | 19,000,003 | `0xbad...` | `0xdex...` | 0.500000 | 0.000210 | access_list | false |

---

## 4. 주요 특징 (Enrichment Logic)
1. **수수료 정밀 계산**: Ethereum의 수수료는 소수점 아래 자릿수가 매우 길어 일반적인 Double 타입을 쓰면 오차가 발생할 수 있습니다. 이를 방지하기 위해 `DecimalType(38, 18)`을 사용하여 정밀한 수수료를 계산합니다.
2. **비즈니스 로직 추가**: 단순히 Raw 데이터를 합치는 것을 넘어 `is_contract_call`, `tx_type_label` 등 데이터 분석 시 자주 쓰이는 필드를 미리 계산하여 제공합니다.
3. **조인 최적화**: Bronze의 `transactions`, `receipts`, `blocks` 세 테이블을 `hash`와 `block_number`를 기준으로 조인하여 정합성을 보장합니다.

<br>

---

# Silver Layer: `whale_txns` 테이블 정의서

`src/silver/transform/whale_txns.py` 실행 시 생성되는 고래(Whale) 트랜잭션 전용 테이블입니다. 
`txn_enriched` 데이터를 기반으로 일정 금액 이상의 대규모 자금 이동을 필터링하고, 주소별 라벨 및 이상 탐지 플래그를 결합한 분석용 테이블입니다.

## 1. 주요 특징 (Key Features)

1. **대규모 거래 필터링**: 설정된 임계치(예: 1,000 ETH) 이상의 성공한 트랜잭션만 추출합니다.
2. **주소 라벨링 (Entity Tagging)**: `known_labels` 데이터를 브로드캐스트 조인하여 송신자/수신자가 거래소(CEX), 탈중앙화 거래소(DEX), 브릿지 등인지 식별합니다.
3. **누적 통계 (Window Analytics)**: 주소별로 해당 날짜(`dt`) 내의 누적 송금액 및 거래 횟수를 실시간 계산하여 제공합니다.
4. **이상 패턴 플래그 (Anomaly Flags)**: 거래소 유입/유출, CEX 간 이동, 단기 반복 대량 송금 등의 패턴을 자동으로 분류합니다.

## 2. 테이블 스키마 (Schema)

| 컬럼명 | 타입 | 설명 | 비고 |
| :--- | :--- | :--- | :--- |
| **hash** | String | 트랜잭션 해시 | |
| **dt** | Date | 파티션 날짜 | |
| **from_address** | String | 송신자 주소 | |
| **from_label** | String | 송신자 엔티티 이름 | (예: Binance, Upbit, Unknown) |
| **from_category** | String | 송신자 카테고리 | (예: CEX, DEX, Bridge) |
| **to_address** | String | 수신자 주소 | |
| **to_label** | String | 수신자 엔티티 이름 | |
| **to_category** | String | 수신자 카테고리 | |
| **value_eth** | Decimal(38,18) | 트랜잭션 금액 (ETH) | |
| **tx_fee_eth** | Decimal | 트랜잭션 수수료 (ETH) | |
| **from_cumul_sent_eth** | Double | 해당 주소의 당일 누적 송금액 | Window function 결과 |
| **from_cumul_tx_count** | Long | 해당 주소의 당일 누적 거래 횟수 | Window function 결과 |
| **flag_cex_deposit** | Boolean | 거래소 입금 플래그 | |
| **flag_cex_withdrawal** | Boolean | 거래소 출금 플래그 | |
| **flag_cex_to_cex** | Boolean | 거래소 간 이동 플래그 | |
| **flag_dex_swap** | Boolean | DEX 스왑 플래그 | |
| **flag_high_freq_sender** | Boolean | 단기 반복 대량 송금 플래그 | |
| **has_flag** | Boolean | 이상 패턴 포함 여부 | |

---

## 3. 이상 패턴 플래그 정의 (Anomaly Flag Definitions)

데이터 분석 시 고래의 의도를 파악하기 위해 다음과 같은 플래그를 제공합니다.

*   **`flag_cex_deposit`**: 익명 주소 → 거래소 (매도 전 이동 가능성)
*   **`flag_cex_withdrawal`**: 거래소 → 익명 주소 (장기 보유를 위한 매집 신호)
*   **`flag_cex_to_cex`**: 거래소 간 이동 (거래소 내부 자산 정산 또는 자금 세탁 의심)
*   **`flag_dex_swap`**: 대규모 DEX 스왑 (Uniswap 등 탈중앙화 거래소를 통한 차익거래 또는 자산 교환)
*   **`flag_high_freq_sender`**: 단기 반복 대량 송금 (당일 동일 주소에서 5회 이상 & 누적 500 ETH 이상 송금 시)

---

## 3. 데이터 예시 (Example Rows)

| from_label | to_label | value_eth | from_cumul_sent_eth | flag_cex_deposit | flag_high_freq_sender |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `Unknown` | `Binance` | 2,500.0 | 2,500.0 | **true** | false |
| `Kraken` | `Unknown` | 5,000.0 | 5,000.0 | false | false |
| `Unknown` | `Uniswap V3` | 1,200.0 | 1,200.0 | false | false |
| `Unknown` | `Upbit` | 300.0 | 1,800.0 | **true** | **true** |

---

## 4. 저장 위치 및 포맷
- **경로**: `gs://[BUCKET_NAME]/silver/ethereum/whale_txns/dt=YYYY-MM-DD/`
- **포맷**: Parquet
- **파티션**: `dt`
