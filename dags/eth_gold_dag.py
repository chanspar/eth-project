import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, Asset, get_current_context
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

# Silver DAG가 발행하는 데이터 자산 — ExternalTaskSensor 대체
SILVER_COMPLETE = Asset("silver/ethereum_silver_complete")


def _get_date_from_context() -> str:
    """Silver Asset 메타데이터에서 logical_date 추출, 없으면 본인의 logical_date 사용"""
    context = get_current_context()
    events = (context.get("triggering_asset_events") or {}).get(SILVER_COMPLETE, [])
    if events:
        dt_str = events[0].extra.get("logical_date")
        if dt_str:
            print(f"🎯 Asset 트리거: logical_date={dt_str}")
            return dt_str
    dt_str = context["logical_date"].strftime("%Y-%m-%d")
    print(f"👤 수동/Cron 트리거: logical_date={dt_str}")
    return dt_str

# 가상환경 및 Python 실행 경로
ETH_ETL_PYTHON = "/opt/airflow/eth_etl_venv/bin/python"

default_args = {
    "owner": "chanspar",
    "depends_on_past": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id="ethereum_gold_analysis",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    # Asset 기반 스케줄링: Silver DAG 완료 시 즉시 트리거
    # ExternalTaskSensor 대비 장점: 폴링 오버헤드 제거, 대기 시간 0
    schedule=[SILVER_COMPLETE],
    catchup=True,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    tags=["ethereum", "gold", "whale", "token", "slack"],
)
def ethereum_gold_dag():

    @task.external_python(python=ETH_ETL_PYTHON)
    def whale_analysis_task():
        """Gold: 고래 개인지갑 행동 분석"""
        import sys
        import subprocess
        sys.path.append("/opt/airflow")
        
        context = get_current_context()
        dt_str = _get_date_from_context()
        
        # subprocess를 사용하여 script 실행 (argparse 대응)
        cmd = [
            ETH_ETL_PYTHON, 
            "src/gold/whale_analysis.py", 
            "--date", dt_str, 
            "--top-n", "50"
        ]
        subprocess.run(cmd, check=True, cwd="/opt/airflow")

    @task.external_python(python=ETH_ETL_PYTHON)
    def token_ranking_task():
        """Gold: 유기적 토큰 랭킹 및 워시트레이딩 분석"""
        import sys
        import subprocess
        sys.path.append("/opt/airflow")
        
        context = get_current_context()
        dt_str = _get_date_from_context()
        
        cmd = [
            ETH_ETL_PYTHON, 
            "src/gold/token_ranking.py", 
            "--date", dt_str, 
            "--top-tokens", "30"
        ]
        subprocess.run(cmd, check=True, cwd="/opt/airflow")

    @task.external_python(python=ETH_ETL_PYTHON)
    def sync_bigquery_task():
        """Gold -> BigQuery: Tableau 연동을 위한 외부 테이블 동기화"""
        import sys
        sys.path.append("/opt/airflow")
        from src.gold.sync_bigquery import sync_all
        sync_all()

    @task.external_python(python=ETH_ETL_PYTHON)
    def send_alerts_task():
        """Slack: 분석 결과 요약 리포트 전송"""
        import sys
        import subprocess
        sys.path.append("/opt/airflow")
        
        context = get_current_context()
        dt_str = _get_date_from_context()
        
        cmd = [
            ETH_ETL_PYTHON, 
            "src/gold/send_alerts.py", 
            "--date", dt_str
        ]
        subprocess.run(cmd, check=True, cwd="/opt/airflow")

    # ── Orchestration ──────────────────────────────────────────────────────────
    # Asset 기반 스케줄링으로 ExternalTaskSensor 제거
    # Silver DAG가 SILVER_COMPLETE Asset을 발행하면 이 DAG가 자동 트리거됨

    # 1. 고래 및 토큰 분석 (병렬 실행)
    whales = whale_analysis_task()
    tokens = token_ranking_task()

    # 2. 분석 완료 후 BigQuery 동기화
    sync_bq = sync_bigquery_task()

    # 3. 마지막으로 슬랙 알림 전송
    alerts = send_alerts_task()

    # 의존성 연결
    [whales, tokens] >> sync_bq >> alerts

ethereum_gold_dag()
