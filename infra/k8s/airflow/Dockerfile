FROM apache/airflow:3.2.1-python3.13

USER root
# uv 설치 및 python 빌드용 필수 패키지 설치
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# uv 바이너리 복사 (빠른 python venv 빌드용)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER airflow

# 1. Airflow 메인 환경 라이브러리 설치 (Python 3.13)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 2. Ethereum-ETL 전용 독립 환경 구성 (Python 3.9 venv)
COPY requirements-etl.txt /requirements-etl.txt
RUN uv python install 3.9 && \
    uv venv /opt/airflow/eth_etl_venv --python 3.9 && \
    uv pip install --python /opt/airflow/eth_etl_venv/bin/python -r /requirements-etl.txt

# 3. K8s Pod에서 즉시 코드를 실행할 수 있도록 소스코드와 DAG 복사
# Git-sync 사용시 소스 코드 복사 불필요
# COPY --chown=airflow:0 src/ /opt/airflow/src/
# COPY --chown=airflow:0 dags/ /opt/airflow/dags/

USER root
RUN chown -R airflow: /opt/airflow/eth_etl_venv
USER airflow

# ✅ PYTHONPATH 제거 (values.yaml extraEnv에서 주입하므로 불필요)
# ENV PYTHONPATH="/opt/airflow/dags/git"   ← 삭제
