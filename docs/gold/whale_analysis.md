# 🐋 Whale Intelligence (Gold Layer)

## 1. 개요 (Overview)
Gold 레이어는 Silver 레이어에서 정제된 고래 트랜잭션 데이터(`whale_txns`)를 바탕으로, 시장의 거시적 흐름과 개별 고래의 행동 패턴을 집계/요약하는 최종 분석 단계입니다.

본 파이프라인은 다음 3가지 핵심 스크립트를 통해 온체인 인텔리전스를 생성합니다.

---

## 2. 주요 분석 스크립트 상세

### 🏆 1. 고래 지갑 인덱스 (`top_whales_daily.py`)
"누가 시장을 주도하고 있는가?"를 분석하여 고래 지갑의 랭킹을 관리합니다.

*   **분석 단위**: `address` + `dt` (지갑별 하루 요약)
*   **주요 컬럼**:
    *   `net_flow_eth`: 하루 동안의 순유입량 (받은 금액 - 보낸 금액). 포지션 변화의 핵심 지표.
    *   `whale_tier`: 거래 규모에 따른 등급 (Humpback, Whale, Shark).
    *   `total_activity_eth`: 하루 동안의 총 거래 대금 (매수+매도 합계).
*   **활용**: 당일의 매집왕(Accumulator) 및 투매왕(Dumper) 식별, 거대 고래 명단 관리.

### 📊 2. 시장 압력 지수 (`market_flow_hourly.py`)
"지금 고래들이 거래소로 입금 중인가, 출금 중인가?"를 시간 단위로 분석합니다.

*   **분석 단위**: `hour` + `flow_type` (시간대별 흐름 요약)
*   **주요 컬럼**:
    *   `flow_type`: `CEX_DEPOSIT` (매도 압력), `CEX_WITHDRAWAL` (매수 압력), `DEX_TRADE`, `PRIVATE_MOVE`.
    *   `total_eth`: 해당 시간대의 총 이동량.
    *   `active_whale_count`: 거래에 참여한 고래 수.
*   **활용**: 시간대별 매수/매도 압력 파악, 시장 변동성 전조 증상 포착.

### 🚨 3. 결정적 사건 보고서 (`daily_incident_report.py`)
"오늘 하루 중 반드시 주목해야 할 특이 거래는 무엇인가?"를 선별합니다.

*   **분석 단위**: `hash` (개별 트랜잭션 중 핵심 건만 추출)
*   **주요 컬럼**:
    *   `severity`: 사건의 중요도 (CRITICAL, HIGH, MEDIUM).
    *   `description`: 거래 주체와 의도를 한 줄로 요약 (예: "Binance -> Unknown (5000 ETH) - CEX_WITHDRAWAL").
    *   `is_private_transaction`: 개인 고래 간의 거대 이동 여부.
*   **활용**: 하루의 결정적 장면 요약, 비정상적 자금 흐름(고래 간 OTC 등) 감지.

---

## 3. 실행 방법 (Usage)

실버 레이어(`whale_txns.py`) 실행 완료 후, 분석 목적에 맞는 스크립트를 실행합니다.

```bash
# 1. 고래별 활동 요약 및 랭킹 생성
uv run src/gold/transform/top_whales_daily.py --date 2026-05-01

# 2. 시간대별 시장 흐름 분석
uv run src/gold/transform/market_flow_hourly.py --date 2026-05-01

# 3. 주요 특이 사건 리포트 생성
uv run src/gold/transform/daily_incident_report.py --date 2026-05-01
```

---

## 4. 데이터 가치 (Business Value)

1.  **시각화 최적화**: 모든 데이터가 `dt` 및 `hour` 단위로 집계되어 있어 Grafana, Tableau 등 BI 도구에서 즉시 차트 생성이 가능합니다.
2.  **데이터 정합성**: 실버 레이어의 `Unknown` 처리 및 `flow_type` 분류 로직을 그대로 계승하여 분석 결과의 신뢰도가 높습니다.
3.  **성능 최적화**: 수십만 건의 트랜잭션을 수백 건의 요약 데이터로 압축하여 쿼리 성능을 비약적으로 향상했습니다.
