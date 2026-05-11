FROM apache/airflow:3.2.1-python3.13

USER root
# 필요한 시스템 패키지 설치 (uv 설치 및 python 빌드 환경용)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# uv 설치 (이미지 레이어 최적화)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER airflow

# 1. Airflow용 기본 환경 (Python 3.13)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# 2. Ethereum-ETL 전용 환경 (Python 3.9)
# uv를 사용하여 Python 3.9 설치 및 전용 venv 구성
COPY requirements-etl.txt /requirements-etl.txt
RUN uv python install 3.9 && \
    uv venv /opt/airflow/eth_etl_venv --python 3.9 && \
    uv pip install --python /opt/airflow/eth_etl_venv/bin/python -r /requirements-etl.txt

# 가상환경 폴더 권한 설정
USER root
RUN chown -R airflow: /opt/airflow/eth_etl_venv
USER airflow
