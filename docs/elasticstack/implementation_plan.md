# [Engineering Plan Review] 정석 ELK 스택 (Filebeat + Logstash) 파이프라인

면접과 회사 지원(포트폴리오)을 위한 **"실무형 정석 버전"**으로 아키텍처를 전면 재설계했습니다. 
대용량 트래픽을 처리하는 IT 기업들이 왜 이 복잡한 파이프라인(Filebeat -> Logstash)을 표준으로 쓰는지, 그 아키텍처의 철학(Decoupling, Backpressure)을 완벽히 보여줄 수 있는 구조입니다.

---

## 🏗️ 1. Architecture Review (정석 ELK 데이터 흐름)

애플리케이션(FastAPI)과 로그 전송 인프라(Elastic Stack)의 책임을 완벽하게 분리(Decoupling)합니다.

### Data Flow Diagram
```text
[ FastAPI App ] --> (Writes JSON to file) --> [ app.log ]
                                                  |
                                                  v (Tails file)
[ Filebeat ] (경량 수집기: 로그 회전 및 Backpressure 처리)
     |
     v (Beats Protocol / Port 5044)
[ Logstash ] (데이터 가공: 타임스탬프 파싱, 필터링)
     |
     v (HTTP / Port 9200)
[ Elasticsearch ] (인덱싱 및 저장: app-logs-YYYY.MM.DD)
     |
     v
[ Kibana ] (시각화 대시보드)
```

### Component Boundaries (역할 분담)
- **FastAPI (Producer)**: 외부로 로그를 쏘는 코드는 1줄도 없습니다. 오직 로컬 폴더에 `app.log`를 JSON 형태로 떨구기만 합니다. (로직과 로깅 인프라의 완전한 분리)
- **Filebeat (Shipper)**: 애플리케이션 컨테이너 옆에 붙어서 `app.log` 파일의 변동을 감지하고 Logstash로 쏩니다.
- **Logstash (Processor)**: Logstash는 병목이 생기면 Filebeat에게 "잠깐 전송 멈춰!"라고 신호(Backpressure)를 보냅니다.
- **Docker Compose**: 기존 `infra/kafka/docker-compose.yaml`에 Filebeat와 Logstash 컨테이너를 추가합니다.

---

## 🛡️ 2. Edge Case Analysis (실무 면접 단골 질문 대비)

면접관이 "로그가 폭주해서 ES가 뻗으면 어떻게 되나요?" 라고 물었을 때 완벽하게 방어할 수 있는 아키텍처입니다.

1. **Logstash/Elasticsearch 장애 발생 시 (Network/Infra Failure)**
   - **문제**: 중앙 로그 서버가 죽었을 때 메인 API 서버도 같이 죽는가?
   - **방어**: FastAPI는 파일에 쓰기만 하므로 **전혀 타격을 받지 않습니다.**
   - **복구**: Filebeat는 Logstash가 죽은 걸 감지하면 파일 읽기를 멈추고 대기합니다. 서버가 복구되면 중단된 라인부터 다시 읽어 전송합니다 (Data Loss 방지).
2. **로그 파일 용량 초과 (Disk Full)**
   - **문제**: `app.log` 파일이 무한히 커져 서버 디스크가 터지는 문제.
   - **방어**: Python의 `TimedRotatingFileHandler`를 사용하여 매일 자정마다 로그 파일을 분리(`app.log.2026-06-21`)하고, 7일이 지난 로그는 자동 삭제(Retention)되게 구성합니다.

---

## 🧪 3. Test Matrix

| Scenario | Type | Priority | Covered? |
|---|---|---|---|
| FastAPI가 JSON 포맷으로 `app.log`를 정상 출력하는가 | Unit | P0 | ☐ |
| Filebeat가 새 로그 라인을 감지하고 Logstash로 쏘는가 | Integration | P0 | ☐ |
| Logstash가 죽었을 때 Filebeat가 대기하다가 복구 시 재전송하는가 (Backpressure) | Chaos/Smoke | P1 | ☐ |
| Kibana Discover 탭에서 파싱된 JSON 필드(예: `duration_ms`)로 검색이 되는가 | Integration | P1 | ☐ |

---

## 📝 4. 실행 계획 (Implementation Steps)

1. **[NEW]** `infra/kafka/logstash/logstash.conf` 및 `infra/kafka/filebeat/filebeat.yml` 설정 파일 작성.
2. **[MODIFY]** `infra/kafka/docker-compose.yaml` 파일에 `logstash`와 `filebeat` 컨테이너 추가 (기존 Elasticsearch, Kibana와 동일한 네트워크에 묶음).
3. **[MODIFY]** `src/backend/core/config.py` 등에 로깅 설정 추가하여 `python-json-logger`를 통해 `logs/app.log` 파일에 JSON 포맷으로 출력하도록 구성.
4. **[MODIFY]** `src/backend/main.py`에 간단한 미들웨어(응답 시간, 상태 코드 측정)를 추가하여 쓸만한 모니터링 데이터를 남기도록 세팅.

> [!IMPORTANT]
> **User Review Required**
> 
> 포트폴리오/면접용으로 완벽한 **"실무형 ELK 스택 (Filebeat + Logstash 추가)"** 아키텍처입니다. 인프라 설정이 조금 복잡해지지만, 구조적 우수성을 보여주기에 이보다 좋은 셋업은 없습니다. 이 방향으로 최종 진행할까요? 우측 하단의 **'Proceed(진행)'**를 눌러주시면 바로 Docker Compose와 설정 파일을 작성하겠습니다!
