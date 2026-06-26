# Token Explorer UX 개선 및 Token Directory 페이지 생성

이 계획서는 Token Explorer 검색 시 발생하는 UX 문제를 해결하고, Token Directory를 A-Z 필터링 시스템을 갖춘 전용 독립 페이지로 분리 및 이동시키는 작업을 다룹니다.

## 제안된 변경 사항

### 백엔드 (Backend)

#### [MODIFY] `src/backend/repositories/token_repo.py`
- `get_all_tokens` 함수가 선택적 파라미터인 `prefix: str = None`을 받을 수 있도록 업데이트합니다.
- `prefix`가 제공된 경우, `WHERE symbol ILIKE $3 || '%'` 조건을 추가하도록 SQL 쿼리를 수정합니다.

#### [MODIFY] `src/backend/services/token_service.py`
- `get_all_tokens` 함수의 시그니처를 수정하여 `prefix` 인자를 repository로 전달하도록 업데이트합니다.

#### [MODIFY] `src/backend/api/tokens.py`
- `get_all_tokens` 엔드포인트에 `prefix: str = Query(None, description="Filter by starting letter")` 파라미터를 추가합니다.

---

### 프론트엔드 설정 (Frontend Setup)

- 다중 페이지(라우팅) 지원을 위해 프론트엔드 디렉토리에서 `npm install react-router-dom`을 실행합니다.

#### [NEW] `frontend/src/pages/Dashboard.tsx`
- 현재 대시보드 레이아웃 (GasTracker, WhaleAlerts, TrendingTokens, TokenExplorer, WalletExplorer)을 이 새로운 컴포넌트로 이동시킵니다.

#### [NEW] `frontend/src/pages/TokenDirectoryPage.tsx`
- Token Directory를 위한 전용 페이지를 생성합니다.
- A-Z 알파벳 필터 UI를 추가합니다 (A-Z, 0-9 버튼).
- 업데이트된 백엔드 API를 사용하여 선택된 prefix 문자로 시작하는 토큰을 불러와 화면에 표시합니다.

#### [MODIFY] `frontend/src/App.tsx`
- `<BrowserRouter>`와 `<Routes>`를 설정합니다.
- `/` 경로를 `<Dashboard />` 컴포넌트로 연결(Map)합니다.
- `/directory` 경로를 `<TokenDirectoryPage />` 컴포넌트로 연결(Map)합니다.

#### [MODIFY] `frontend/src/components/Header.tsx`
- 사용자가 여러 페이지 간을 원활하게 이동할 수 있도록 "Dashboard"와 "Token Directory"로 연결되는 링크가 포함된 네비게이션 메뉴를 추가합니다.

#### [MODIFY] `frontend/src/components/TokenExplorer.tsx`
- 검색 입력 필드(Search input field)에 `onKeyDown` 이벤트 핸들러를 추가합니다.
- 사용자가 "Enter" 키를 누르고 (`e.key === 'Enter'`) 검색 결과가 하나 이상 존재하는 경우 (`searchResults.length > 0`), 자동으로 목록의 첫 번째 토큰이 선택되도록 (`selectToken(searchResults[0])`) 구현합니다.

#### [DELETE] `frontend/src/components/TokenDirectory.tsx`
- 새로운 `TokenDirectoryPage` 컴포넌트로 완전히 대체되므로, 기존 컴포넌트를 삭제합니다.

## 검증 계획 (Verification Plan)

### 자동화된 테스트 (Automated Tests)
- 추가로 계획된 자동화 테스트 스크립트는 없습니다; `npm run build` 명령어를 통해 전체 프론트엔드 빌드가 정상적으로 수행되는지 검증합니다.

### 수동 검증 (Manual Verification)
- Token Explorer에서 검색어 입력 후 "Enter" 키를 눌렀을 때 해당 토큰의 트렌드 차트가 즉시 로드되는지 확인합니다.
- 상단 헤더의 "Token Directory" 링크를 클릭했을 때 새로운 페이지로 정상적으로 이동하는지 확인합니다.
- Token Directory 페이지에서 여러 알파벳(A, B, C...) 버튼을 클릭해보고 토큰 리스트가 올바르게 업데이트(필터링)되는지 확인합니다.
- 라우팅 URL이 알맞게 변경되는지, 브라우저의 '뒤로 가기/앞으로 가기' 버튼이 예상대로 동작하는지 확인합니다.
