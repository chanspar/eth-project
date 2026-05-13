# Spark Session Configuration Guide

이 문서는 Ethereum ETL 프로젝트에서 사용하는 Spark 세션 설정(`src/silver/spark_session.py`)의 주요 내용과 역할을 설명합니다.

## 1. 개요
Spark 세션은 Google Cloud Storage(GCS)와의 연동 및 효율적인 데이터 처리(Silver Layer 변환 등)를 위해 최적화되어 있습니다. `APP_ENV` 환경 변수를 통해 로컬 개발 환경과 운영 환경(Airflow/Docker)을 자동으로 감지하여 최적의 설정을 적용합니다.

## 2. 주요 설정 상세

### ☁️ GCS 커넥터 (Hadoop FileSystem)
Spark가 `gs://` URI를 인식하고 GCS와 통신할 수 있도록 설정합니다.
*   **`fs.gs.impl`**: GCS용 Hadoop FileSystem 구현체를 지정합니다.
*   **`fs.AbstractFileSystem.gs.impl`**: 최신 Hadoop/Spark 환경에서 필수적인 추상 파일 시스템 구현체 설정입니다.

### 🔑 인증 (Authentication)
GCP 서비스 계정을 사용하여 GCS에 안전하게 접근합니다.
*   **Service Account JSON**: `GOOGLE_APPLICATION_CREDENTIALS` 환경 변수가 가리키는 JSON 키 파일을 로드합니다.
*   **Project ID**: `GCP_PROJECT_ID` 환경 변수를 사용하여 명시적인 프로젝트 컨텍스트를 제공합니다.

### 📦 커넥터 관리 (Packages vs Jars)
환경에 따라 GCS 커넥터를 불러오는 방식이 다릅니다.
*   **Local (`packages`)**: 별도의 설치 없이 Maven에서 자동으로 다운로드합니다.
*   **Production (`jars`)**: 속도와 안정성을 위해 Docker 이미지 내부에 미리 설치된 JAR 파일을 사용합니다.
    *   기본 경로: `/opt/airflow/spark_jars/gcs-connector-hadoop3-shaded.jar`

### ⚡ ETL 및 네트워크 최적화
*   **`partitionOverwriteMode: dynamic`**: 파티션 작업 시 기존 데이터를 전체 삭제하지 않고 해당 파티션만 덮어씁니다.
*   **`spark.driver.bindAddress`**: macOS 등 로컬 환경에서 발생하는 네트워크 바인딩 오류를 방지합니다.

## 3. 환경별 설정 가이드

### 💻 로컬 개발 (Local Development)
`APP_ENV`를 설정하지 않거나 `local`로 설정하면 활성화됩니다.
`.env` 파일 예시:
```bash
APP_ENV=local
GCP_PROJECT_ID=your-project-id
GOOGLE_APPLICATION_CREDENTIALS=/absolute/path/to/local-key.json
```

### 🚀 운영 환경 (Airflow/Docker)
`APP_ENV=prod` 설정을 통해 활성화됩니다.
Docker Compose 또는 Helm 차트 예시:
```yaml
env:
  - name: APP_ENV
    value: "prod"
  - name: GOOGLE_APPLICATION_CREDENTIALS
    value: "/opt/airflow/gcp-key.json"
```

## 4. 사용 방법
프로젝트 내 어디서든 다음과 같이 간단하게 Spark 세션을 생성할 수 있습니다.

```python
from src.silver.spark_session import get_spark_session

# 환경(APP_ENV)에 맞는 세션 자동 생성
spark = get_spark_session("Ethereum-Silver-ETL")

# GCS 데이터 읽기 예시
df = spark.read.parquet("gs://your-bucket/bronze/blocks/*.parquet")
```
