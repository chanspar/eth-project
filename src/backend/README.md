# 📚 FastAPI 백엔드 시니어 아키텍처 공부 가이드

현재 `src/backend`에 구현된 코드는 실무에서 널리 쓰이는 **클린 아키텍처(Clean Architecture) 3계층 구조**를 따르고 있습니다. 처음 보면 파일이 여러 개로 나뉘어 있어 복잡해 보일 수 있지만, 흐름을 한 번 이해하고 나면 유지보수와 확장이 훨씬 쉬워지는 마법을 경험하실 수 있습니다!

어떤 순서로 코드를 뜯어보고 공부해야 하는지, 핵심 키워드와 함께 가이드를 정리해 드립니다.

---

## 🗺️ 전체 구조 한눈에 보기 (3-Layer Architecture)

이 백엔드는 식당에 비유할 수 있습니다.
1. **API (라우터)** = `웨이터` (주문을 받고 서빙만 함, 요리는 안 함)
2. **Service (비즈니스 로직)** = `주방장` (요리를 총괄함, 에러 처리 로직)
3. **Repository (DB 접근)** = `재료 창고 관리자` (창고(DB)에서 재료(데이터)만 꺼내옴)

이렇게 역할을 철저히 분리했기 때문에 코드가 섞이지 않고 깔끔하게 유지됩니다.

---

## 👣 코드를 읽는 추천 순서

### 1단계: 진입점과 설정 파악하기
가장 먼저 서버가 어떻게 켜지고 설정값을 어디서 가져오는지 확인하세요.
- [src/backend/core/config.py](file:///c:/llm/eth-proj-v2/src/backend/core/config.py)
  - **키워드**: `pydantic-settings`
  - `.env` 파일의 환경 변수를 파이썬 코드로 안전하게 불러오는 역할을 합니다.
- [src/backend/main.py](file:///c:/llm/eth-proj-v2/src/backend/main.py)
  - 서버의 시작점입니다. `lifespan` 함수를 통해 서버가 켜질 때 DB를 연결하고 CSV 데이터를 적재하는 과정을 눈여겨보세요. 라우터(API)들이 여기서 등록됩니다.
- [src/backend/core/db.py](file:///c:/llm/eth-proj-v2/src/backend/core/db.py)
  - DB 커넥션 풀을 생성(`init_db_pool`)하고 해제하는 핵심 코드입니다.
  - 여기서 `get_db` 함수가 매우 중요합니다. FastAPI의 **의존성 주입(Dependency Injection)**에 사용되는 함수입니다.

### 2단계: 데이터의 생김새 확인하기 (DTO)
- [src/backend/models/schemas.py](file:///c:/llm/eth-proj-v2/src/backend/models/schemas.py)
  - **키워드**: `Pydantic`
  - 프론트엔드로 나갈 응답(Response) 데이터의 모양을 정의해 둔 곳입니다. API가 어떤 데이터를 뱉어내는지 이곳에서 한눈에 파악할 수 있습니다.

### 3단계: 기본 API의 흐름 따라가기 (가스비 트래커 예시)
가장 구조가 단순한 `gas` 관련 파일 3개를 순서대로 열어놓고 데이터가 어떻게 흘러가는지 추적해 보세요.

1. **Repository (창고 관리자)**: [src/backend/repositories/gas_repo.py](file:///c:/llm/eth-proj-v2/src/backend/repositories/gas_repo.py)
   - 오직 날것의 SQL 쿼리를 날려서 DB에서 값을 가져오는 일만 합니다.
2. **Service (주방장)**: [src/backend/services/gas_service.py](file:///c:/llm/eth-proj-v2/src/backend/services/gas_service.py)
   - Repository가 가져온 `avg_wei` 값을 사람이 읽기 쉬운 `gwei` 단위로 변환하고, 에러가 나면 500 에러를 뱉도록 처리합니다. 여기서 `GasMetricsResponse`라는 Pydantic 스키마로 데이터를 포장합니다.
3. **API (웨이터)**: [src/backend/api/gas.py](file:///c:/llm/eth-proj-v2/src/backend/api/gas.py)
   - 브라우저에서 `/api/v1/metrics/gas`로 요청이 오면 `Service`에게 일을 시키고, 받은 결과를 그대로 반환합니다. `Depends()`를 이용해 필요한 부품(Service, DB 커넥션)을 조립(주입)받는 방식을 꼭 이해하세요!

> [!TIP]
> `tokens` API와 `wallets` API도 완전히 똑같은 구조로 되어 있으므로, `gas` 흐름을 이해하셨다면 복습 차원에서 쓱 읽어보시기 바랍니다.

### 4단계: 심화학습 - 실시간 웹소켓과 LISTEN/NOTIFY
마지막으로 가장 난이도가 높은 실시간 고래 알림(Whale Watcher) 코드를 봅니다. 일반적인 API 요청-응답(Request-Response) 구조가 아닌 이벤트 주도(Event-Driven) 방식입니다.

- [src/backend/core/db.py](file:///c:/llm/eth-proj-v2/src/backend/core/db.py) (다시 보기)
  - `init_db_pool` 안에 있는 `CREATE TRIGGER` SQL문을 확인하세요. DB에 데이터가 들어오면 `pg_notify`로 알림을 쏘도록 설정했습니다.
  - `handle_whale_notification` 함수가 그 알림을 받아 처리합니다.
- [src/backend/core/ws_manager.py](file:///c:/llm/eth-proj-v2/src/backend/core/ws_manager.py)
  - 연결된 모든 웹소켓 클라이언트들을 리스트로 관리하며, 메세지를 전체 방송(Broadcast)하는 매니저 클래스입니다.
- [src/backend/api/whales.py](file:///c:/llm/eth-proj-v2/src/backend/api/whales.py)
  - `/ws/whales` 라우터 코드를 보면, 무한 루프(`while True`)를 돌면서 DB에 쿼리를 날리는 로직이 싹 사라지고, 오직 클라이언트와의 연결을 끊기지 않게 유지하는 로직만 남은 것을 확인할 수 있습니다. 데이터는 `ws_manager`가 알아서 쏴줍니다.

---

## 💡 필수 학습 키워드 (구글링 추천)

코드를 보시다가 막히는 부분이 있다면 아래 키워드들로 구글링하시거나 저에게 물어보시면 이해가 빠릅니다.

1. **FastAPI Dependency Injection (`Depends`)**
   - 왜 귀찮게 `Depends`를 써서 `get_db`를 넘겨주는지? (정답: 재사용성과 단위 테스트를 위해서!)
2. **Pydantic Validation (`BaseModel`)**
   - 파이썬의 강력한 타입 체크 라이브러리. FastAPI가 왜 그토록 빠르고 문서화(Swagger)가 잘 되는지의 핵심입니다.
3. **Python Asyncio (`async / await`)**
   - 비동기 프로그래밍 개념. `async def` 안에서는 일반 동기 함수(`time.sleep()`이나 `open()`)를 쓰면 전체 서버가 멈춘다는(Event Loop Blocking) 개념을 이해해야 합니다.
4. **PostgreSQL LISTEN / NOTIFY**
   - 실시간 서비스를 구현할 때 무식하게 폴링(Polling)하지 않고 데이터베이스 단에서 이벤트를 쏴주는 우아한 방법입니다.
