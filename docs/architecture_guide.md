# Ethereum Real-time Dashboard: 작업 리포트 및 아키텍처 가이드

이 문서는 **이번 세션에서 변경/추가된 모든 코드 내역**과, 질문하셨던 **Elasticsearch 스택의 역할 및 실행 방법**을 포함한 전체적인 데이터 흐름을 모두 담은 최종 종합 보고서입니다.

---

## 1. 이번 세션 주요 변경 사항 및 코드 매핑 (Changelog)

제가 이번 세션에서 직접 수정하고 구현한 핵심 기능 4가지와 그 코드가 위치한 파일들입니다.

### ✨ A. Token Directory (독립 페이지화 및 A-Z 인덱스 필터링)
기존에 대시보드 하단에 붙어있던 디렉토리를 완전히 새로운 전용 페이지로 빼내고, 백엔드와 연동되는 알파벳(A~Z) 필터 기능을 구현했습니다.

*   **[frontend/src/pages/TokenDirectoryPage.tsx](file:///c:/llm/eth-proj-v2/frontend/src/pages/TokenDirectoryPage.tsx) [NEW]**
    *   알파벳 버튼 UI를 생성하고, 클릭 시 해당 문자로 시작하는 토큰만 가져오도록 `prefix` 파라미터를 담아 백엔드로 API 요청을 보냅니다.
*   **[frontend/src/App.tsx](file:///c:/llm/eth-proj-v2/frontend/src/App.tsx) [MODIFY]**
    *   `react-router-dom`을 적용해 기존 메인 화면과 새로운 디렉토리 화면 간의 라우팅 구조를 분리했습니다.
*   **[frontend/src/components/Header.tsx](file:///c:/llm/eth-proj-v2/frontend/src/components/Header.tsx) [MODIFY]**
    *   웹사이트 상단 우측에 두 페이지 간 이동을 위한 네비게이션 버튼을 만들었습니다.
*   **[src/backend/api/tokens.py](file:///c:/llm/eth-proj-v2/src/backend/api/tokens.py) [MODIFY]**
    *   프론트엔드가 넘겨주는 `prefix` 값을 받도록 API 파라미터 규격을 확장했습니다.
*   **[src/backend/repositories/token_repo.py](file:///c:/llm/eth-proj-v2/src/backend/repositories/token_repo.py) [MODIFY]**
    *   DB에 `ILIKE $3 || '%'` 구문을 추가하여 지정한 알파벳으로 시작하는 토큰만 걸러내도록(Filtering) 쿼리를 최적화했습니다.

### ✨ B. Token Explorer (키보드 조작 UX 및 스크롤 개선)
마우스 클릭 없이도 토큰을 부드럽게 검색하고 선택할 수 있도록 접근성을 높였습니다.

*   **[frontend/src/components/TokenExplorer.tsx](file:///c:/llm/eth-proj-v2/frontend/src/components/TokenExplorer.tsx) [MODIFY]**
    *   `onKeyDown`을 통해 **위(↑)/아래(↓) 방향키**로 검색 결과 리스트 사이를 이동하는 로직(`selectedIndex`)을 구현했습니다.
    *   리스트가 길어져 화면 밑으로 내려갈 때 **자동으로 스크롤바가 포커스를 따라가도록** `scrollIntoView` 기능을 도입했습니다.
    *   **엔터(Enter) 키**를 치면 하이라이트된 토큰의 트렌드 차트가 즉각 렌더링되게 연결했습니다.

### ✨ C. 치명적 버그 수정 및 보호막(Error Boundary) 구축
화면 전체가 하얗게 죽는(Crash) 현상을 완전히 고쳤습니다.

*   **[frontend/src/components/GasTracker.tsx](file:///c:/llm/eth-proj-v2/frontend/src/components/GasTracker.tsx) [MODIFY]**
    *   백엔드(`average_gas_price_gwei`)와 프론트엔드(`avg_gas_gwei`) 간의 변수명 불일치로 발생한 파싱 에러(TypeError)를 잡아 스키마를 동기화했습니다.
*   **[frontend/src/main.tsx](file:///c:/llm/eth-proj-v2/frontend/src/main.tsx) & [frontend/src/ErrorBoundary.tsx](file:///c:/llm/eth-proj-v2/frontend/src/ErrorBoundary.tsx) [NEW]**
    *   앱 내부에서 UI 렌더링 에러가 나더라도 브라우저가 먹통이 되지 않고 에러 원인을 텍스트로 안전하게 띄워주도록 최상단 보호막을 씌웠습니다.

### ✨ D. 낡은 레거시(찌꺼기) 코드 삭제
더 이상 사용하지 않는 옛날 코드를 지워 프로젝트 뼈대를 가볍게 했습니다.

*   **[src/backend/main.py](file:///c:/llm/eth-proj-v2/src/backend/main.py) [MODIFY]**
    *   과거 정적 파일 마운트를 위한 `StaticFiles` 관련 코드를 전부 삭제했습니다.
*   **[src/backend/static] 디렉토리 & [qa_test.py] [DELETE]**
    *   프론트엔드가 React로 전환되기 이전에 쓰던 구형 대시보드 뷰어(`dashboard.html`)와 이를 띄우던 Playwright 자동화 테스트 스크립트를 파일 시스템에서 영구 제거했습니다.

---

## 2. Elastic Stack (엘라스틱서치) 연동 아키텍처

질문하셨던 Elasticsearch와 시스템 아키텍처 전반의 데이터 파이프라인에 대한 설명입니다.

### 🔍 Elasticsearch의 핵심 역할
*   **어디서 쓰이나요?**: 프론트엔드의 **Token Explorer 컴포넌트**에서 검색창에 글자를 입력할 때 동작합니다.
*   **왜 쓰이나요?**: PostgreSQL(RDBMS)의 일반적인 `LIKE` 문으로 수만 개의 토큰 이름을 뒤지는 것보다, Elasticsearch 같은 역색인(Inverted Index) 기반의 전용 검색 엔진을 통하면 "usd"라는 키워드 하나로 USDC, USDT 등을 0.01초 이내에 자동완성으로 찾아낼 수 있기 때문입니다. 

### ⚙️ Elastic 관련 주요 파일과 스크립트

1. **[scripts/bootstrap_es_tokens.py](file:///c:/llm/eth-proj-v2/scripts/bootstrap_es_tokens.py)**
   *   **역할**: PostgreSQL DB에 얌전히 누워있는 전체 토큰 뭉치를 퍼올려서 Elasticsearch 안에 밀어넣는(Bulk Indexing) 스크립트입니다.
   *   **언제 쓰나요?**: 최초로 로컬 개발 환경을 세팅했을 때나, 검색 데이터가 꼬여서 엔진을 **완전 초기화(포맷 후 재색인)** 해야 할 때 관리자가 딱 한 번 수동으로 돌려주는 응급 스크립트입니다.

2. **[src/backend/services/sync_worker.py](file:///c:/llm/eth-proj-v2/src/backend/services/sync_worker.py)**
   *   **역할**: 평상시에 백엔드 뒤에 숨어서 도는(Background) 그림자 같은 존재입니다.
   *   **언제 쓰나요?**: Kafka를 통해 "새로운 토큰이 거래됐다!"라는 이벤트가 발생할 때마다, 이 녀석이 듣고 있다가 실시간으로 엘라스틱서치에 반영(Sync)해서 검색 결과가 최신 상태로 유지되게 만들어 줍니다. (앱 가동 시 자동으로 켜집니다.)

---

## 3. 프로젝트 실행 요약

현재 코드를 기반으로 서비스를 정상 구동하기 위한 명령어입니다.

**1. Elasticsearch 초기화 (최초 세팅 시에만)**
```bash
uv run python -m scripts.bootstrap_es_tokens
```

**2. 백엔드(FastAPI) 실행 (수정 시 실시간 적용)**
```bash
uv run uvicorn src.backend.main:app --host 0.0.0.0 --port 8000 --reload
```

**3. 프론트엔드(React) 실행**
```bash
cd frontend
npm run dev
```








---

---
```
On branch feat/kafka
Changes not staged for commit:
  (use "git add/rm <file>..." to update what will be committed)
  (use "git restore <file>..." to discard changes in working directory)
	modified:   frontend/src/App.tsx
	modified:   frontend/src/components/GasTracker.tsx
	modified:   frontend/src/components/Header.tsx
	modified:   frontend/src/components/TrendingTokens.tsx
	modified:   frontend/src/components/WalletExplorer.tsx
	deleted:    frontend/src/counter.ts
	modified:   frontend/src/index.css
	deleted:    frontend/src/main.ts
	modified:   frontend/src/main.tsx
	deleted:    frontend/src/style.css
	modified:   infra/kafka/docker-compose.yaml
	modified:   pyproject.toml
	modified:   src/backend/api/tokens.py
	modified:   src/backend/main.py
	modified:   src/backend/models/schemas.py
	modified:   src/backend/repositories/token_repo.py
	modified:   src/backend/services/token_service.py
	modified:   src/consumer/config.py
	modified:   src/consumer/main.py
	modified:   uv.lock

Untracked files:
  (use "git add <file>..." to include in what will be committed)
	docs/architecture_guide.md
	docs/implementation_plan.md
	frontend/src/ErrorBoundary.tsx
	frontend/src/components/TokenExplorer.tsx
	frontend/src/pages/
	scripts/bootstrap_es_tokens.py
	src/backend/services/sync_worker.py

no changes added to commit (use "git add" and/or "git commit -a")
```