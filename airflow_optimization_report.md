# Airflow 3 & Ethereum-ETL 환경 최적화 리포트

## 1. 개요
Airflow 3 (Python 3.13) 환경에서 Python 3.9에 의존성이 있는 `ethereum-etl`을 안정적으로 실행하고, 알림 시스템을 구축하는 과정에서 발생한 이슈와 해결책을 정리합니다.

## 2. 주요 아키텍처: 듀얼 파이썬 환경 (Multi-Python Setup)
Airflow 제어부와 ETL 작업부의 파이썬 버전을 분리하여 의존성 충돌을 근본적으로 해결했습니다.

- **Airflow Plane**: Python 3.13 (최신 성능 및 보안)
- **ETL Worker**: Python 3.9 (ethereum-etl 라이브러리 호환성)
- **도구**: `uv`를 활용해 가상환경 생성 속도를 10배 이상 단축

## 3. 해결된 주요 이슈들

### ✅ 이슈 1: 라이브러리 버전 충돌 (Python 3.13 vs 3.9)
- **증상**: Airflow 3는 3.13을 권장하지만, ethereum-etl은 3.9 환경에서만 안정적으로 작동함.
- **해결**: `Dockerfile` 내에서 `uv`를 사용해 `/opt/airflow/eth_etl_venv` 가상환경을 미리 구축하고, DAG에서 `@task.external_python` 데코레이터를 사용하여 해당 환경의 인터프리터를 호출함.

### ✅ 이슈 2: 도커 볼륨 마운트 경로 오류
- **증상**: `docker-compose.yaml` 실행 시 DAG와 소스 코드가 인식되지 않음.
- **원인**: 컴포즈 파일이 루트가 아닌 `infra/airflow_docker/`에 있어 상대 경로 `./dags`가 엉뚱한 곳을 가리킴.
- **해결**: 모든 볼륨 마운트 경로를 `../../dags`와 같이 루트를 가리키도록 수정하고, `PYTHONPATH` 환경 변수를 추가하여 소스 코드 위치를 전역적으로 등록함.

### ✅ 이슈 3: 모듈 임포트 에러 (`ModuleNotFoundError`)
- **증상**: 컨테이너 내에서 `src.config`를 찾지 못함.
- **해결**: 프로젝트 구조에 맞게 임포트 경로를 `src.storage.config`로 일괄 수정하고, `dags/utils/` 폴더에 `__init__.py`를 생성하여 정식 파이썬 패키지로 인식하게 함.

### ✅ 이슈 4: Airflow 3 비동기 콜백 모순 (Critical)
- **증상**: 슬랙 알림 콜백 시 `is not awaitable` 에러 또는 `coroutine was never awaited` 경고 발생.
- **원인**: Airflow 3 SDK는 DAG 파싱 시 비동기(`async def`)를 요구하지만, 실행기(Runner)는 아직 동기 방식으로 호출하는 과도기적 버그 존재.
- **해결**: **클래스 기반 콜백(Class-based Notifier)** 개발.
    - `__qualname__` 속성 부여로 이름 검증 통과
    - `__await__` 가짜 메서드로 비동기 검증 통과
    - `__call__` 메서드로 실제 실행은 동기 방식으로 즉시 처리

### ✅ 이슈 5: Etherscan API 오타 및 미래 날짜 조회 에러 (`NOTOK`)
- **증상**: `status` 오타로 인한 `KeyError` 발생 및 내일 날짜 블록 조회 시 `NOTOK` 에러 발생.
- **해결**: 
    - `stauts` -> `status` 오타 수정.
    - 요청 시점이 미래일 경우 현재 시간(`now`)으로 자동 조정하여 API 에러 방지.
    - `AirflowFailException` 임포트 경로를 Airflow 3 SDK 규격으로 업데이트.

### ✅ 이슈 6: DAG 실행 병렬 제어 및 순차 처리
- **증상**: `catchup=True` 설정 시 가벼운 태스크들(`calculate_block_range`)이 여러 날짜에 걸쳐 한꺼번에 실행됨.
- **해결**: `max_active_runs=1` 설정을 추가하여 하루치 작업을 완전히 끝내고 다음 날 작업을 시작하도록 순차 실행 보장.

### ✅ 이슈 7: 가상환경 바이너리 실행 권한 및 경로 문제
- **증상**: `ethereumetl` 명령어 실행 시 `Permission denied` 또는 `Command not found` 발생.
- **해결**:
    - `Dockerfile`: 가상환경 폴더 소유권을 `airflow` 유저로 변경 (`chown`).
    - `shell.py`: 명령 실행 시 가상환경의 `bin` 폴더를 `PATH` 환경 변수에 자동으로 추가하도록 로직 보강.

### ✅ 이슈 8: GCP GCS 인증 에러 (`DefaultCredentialsError`)
- **증상**: 데이터 추출 후 GCS 업로드 단계에서 인증 키를 찾지 못해 실패.
- **해결**: 
    - `.env`: `GOOGLE_APPLICATION_CREDENTIALS` 경로 정의.
    - `docker-compose.yaml`: 로컬의 `gcp-key.json` 파일을 컨테이너 내 정해진 경로로 마운트하여 인증 정보 제공.

## 4. 인프라 관리 도구 (Makefile)
복잡한 도커 명령어를 단순화하기 위해 루트에 `Makefile`을 도입했습니다.
- `make init`: 초기 이미지 빌드, 권한 설정 및 DB 마이그레이션
- `make up`: 서비스 백그라운드 실행
- `make restart`: 코드/설정 변경 후 컨테이너 재시작
- `make logs`: 실시간 로그 모니터링

## 5. 향후 유지보수 가이드
- **의존성 관리**: Airflow 라이브러리는 `requirements.txt`, ETL 도구는 `requirements-etl.txt`에 분리해서 관리하세요.
- **인증 보안**: `gcp-key.json` 파일은 절대 Git에 커밋하지 마세요 (`.gitignore` 확인 필수).
- **성능 튜닝**: 대량 데이터 수집 시 Alchemy 플랜에 맞춰 `max_workers`와 `batch_size`를 조정하세요.
- **모니터링**: 슬랙 알림이 오지 않는다면 `Admin > Connections`에서 `eth_etl_webhook` 설정을 점검하세요.
