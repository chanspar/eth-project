# 📈 Standalone Kafka Lag Test Sandbox (격리 테스트 환경)

프로젝트 전체를 다 실행할 필요 없이, **"초당 750건의 피크 트래픽 속에서 컨슈머 Lag이 우상향하고 수렴하는 현상"**을 10분 만에 시각적으로 재현하고 성능 튜닝 효과를 실측하기 위한 샌드박스 환경입니다.

---

## 📋 1. 사전 요구사항 (Prerequisites)

실행할 터미널 환경에 아래 라이브러리들이 설치되어 있어야 합니다.
```bash
pip install confluent-kafka psycopg2-binary
```

---

## 🚀 2. 실행 순서 및 모니터링 세팅

### Step 1. 인프라 실행
이 테스트용 샌드박스는 기존 프로젝트와 포트(Kafka 9092/9094, Postgres 5432)가 겹칠 수 있으므로, **기존에 기동 중인 Docker Compose가 있다면 먼저 종료해 주세요.**

그 후 아래 명령어로 샌드박스 인프라를 실행합니다:
```bash
cd lag_test
docker-compose up -d
```
* **동작 서비스:** Kafka, PostgreSQL, Prometheus, Grafana, Kafka Exporter

### Step 2. Grafana 접속 및 대시보드 연동
1. **Grafana UI:** `http://localhost:3000` 접속 (초기 ID/PW: `admin` / `admin`)
2. **Prometheus 데이터소스 등록:**
   - **Connections ➔ Data Sources** ➔ **Add data source** ➔ **Prometheus** 선택
   - Connection URL에 `http://prometheus:9090` 입력 후 하단 **Save & test** 클릭
3. **Kafka Exporter 대시보드 임포트:**
   - 우측 상단 `+` 버튼 ➔ **Import dashboard** 클릭
   - ID 입력칸에 `7589` 입력 후 **Load** 클릭
   - Prometheus 데이터소스로 좀 전에 추가한 `Prometheus`를 선택한 뒤 **Import** 클릭
   - *성공하면 카프카 모니터링 화면이 열립니다.*

---

## 🧪 3. 병목 시나리오 재현 및 검증 (실습)

### 🔴 시나리오 A: 컨슈머 장애 혹은 비효율적 처리 (Lag 우상향 재현)

1. **초당 750건 데이터 유입 시작 (Producer 실행):**
   ```bash
   python producer.py
   ```
   * 3초마다 `Current TPS: 750.00` 가량의 전송 속도가 실시간으로 출력됩니다.

2. **[옵션] 컨슈머가 완전히 멈췄거나, 한 건씩 느리게 처리할 때:**
   - **컨슈머를 켜지 않고 방치하거나**,
   - `consumer.py` 파일 상단의 `SLOW_MODE = True` 인 상태로 실행해 봅니다:
     ```bash
     python consumer.py
     ```
   - 이 경우 건당 강제 딜레이(15ms)로 인해 소비 속도가 **초당 약 60건**에 그칩니다. 
   - **결과:** 유입(750/s) 대비 소비(60/s)가 너무 느려 Grafana 대시보드의 **"Consumer Lag by Topic"** 그래프가 급격하게 **우상향(지연 축적)**하는 것을 확인할 수 있습니다.

---

### 🟢 시나리오 B: 배치 인서트(execute_values) 적용 (Lag 0 수렴 검증)

1. 실행 중이던 `consumer.py`를 `Ctrl + C`로 중지합니다.
2. `consumer.py` 파일 상단의 스위치 변수를 수정합니다:
   ```python
   # consumer.py 11라인
   SLOW_MODE = False  
   ```
3. 컨슈머를 다시 실행합니다:
   ```bash
   python consumer.py
   ```
   - 이제 `psycopg2`의 `execute_values`를 사용하여 100건씩 한 쿼리로 밀어 넣습니다.
   - 소비 속도가 **초당 수천 건 이상**으로 치솟습니다.
   - **결과:** 그동안 엄청나게 누적되어 계속 우상향하던 Grafana의 **Lag 그래프가 순식간에 수직 낙하하여 0으로 찰싹 달라붙는 현상**을 눈으로 직접 관측할 수 있습니다!
