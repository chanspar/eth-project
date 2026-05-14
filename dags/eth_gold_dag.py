import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

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
    schedule="0 14 * * *",  # Silver(오후 12:30) 완료 후 오후 2:00 실행
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
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        
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
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        
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
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        
        cmd = [
            ETH_ETL_PYTHON, 
            "src/gold/send_alerts.py", 
            "--date", dt_str
        ]
        subprocess.run(cmd, check=True, cwd="/opt/airflow")

    # ── Orchestration ──────────────────────────────────────────────────────────

    # 0. Silver DAG 완료 대기
    wait_for_silver = ExternalTaskSensor(
        task_id="wait_for_silver_dag",
        external_dag_id="ethereum_silver_transformation",
        external_task_id="quality_check_task",
        allowed_states=["success"],
        poke_interval=300,
        timeout=3600,
        mode="reschedule",
    )

    # 1. 고래 및 토큰 분석 (병렬 실행)
    whales = whale_analysis_task()
    tokens = token_ranking_task()

    # 2. 분석 완료 후 BigQuery 동기화
    sync_bq = sync_bigquery_task()

    # 3. 마지막으로 슬랙 알림 전송
    alerts = send_alerts_task()

    # 의존성 연결
    wait_for_silver >> [whales, tokens] >> sync_bq >> alerts

ethereum_gold_dag()
