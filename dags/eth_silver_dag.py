import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.sdk.exceptions import AirflowFailException
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

# 기존에 설정된 가상환경 경로 사용
ETH_ETL_PYTHON = "/opt/airflow/eth_etl_venv/bin/python"

default_args = {
    "owner": "chanspar",
    "depends_on_past": True,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id="ethereum_silver_transformation",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="30 12 * * *",  # Bronze ETL(오전 10시) 완료 후 여유 있게 오후 12:30 실행
    catchup=True,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=4), # 실버 가공은 4시간 이내 완료 권장
        callback=AsyncCallback(task_fail_slack_alert),
    ),
    tags=["ethereum", "silver", "spark"],
)
def ethereum_silver_dag():

    @task.external_python(python=ETH_ETL_PYTHON)
    def build_txn_enriched_task():
        """Bronze -> Silver: txn_enriched 가공 (LEFT JOIN 로직)"""
        import sys
        sys.path.append("/opt/airflow")
        from src.silver.transform.txn_enriched import build_txn_enriched
        from src.silver.spark_config import get_spark_session
        from src.silver.utils import write_silver
        
        context = get_current_context()
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        dt_partition = f"dt={dt_str}"
        
        spark = get_spark_session("Airflow-Build-Txn-Enriched")
        try:
            df = build_txn_enriched(spark, dt_partition)
            write_silver(df, "txn_enriched")
        finally:
            spark.stop()

    @task.external_python(python=ETH_ETL_PYTHON)
    def build_token_flow_task():
        """Bronze -> Silver: token_flow 가공 (전수 전송 내역 확보)"""
        import sys
        sys.path.append("/opt/airflow")
        from src.silver.transform.token_flow import build_token_flow
        from src.silver.spark_config import get_spark_session
        from src.silver.utils import write_silver
        
        context = get_current_context()
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        dt_partition = f"dt={dt_str}"
        
        spark = get_spark_session("Airflow-Build-Token-Flow")
        try:
            df = build_token_flow(spark, dt_partition)
            write_silver(df, "token_flow")
        finally:
            spark.stop()

    @task.external_python(python=ETH_ETL_PYTHON)
    def build_whale_txns_task():
        """Silver Enriched -> Silver Whale: 고래 자금 흐름 가공"""
        import sys
        sys.path.append("/opt/airflow")
        from src.silver.transform.whale_txns import build_whale_txns
        from src.silver.spark_config import get_spark_session
        from src.silver.utils import write_silver
        
        context = get_current_context()
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        dt_partition = f"dt={dt_str}"
        
        spark = get_spark_session("Airflow-Build-Whale-Txns")
        try:
            df = build_whale_txns(spark, dt_partition)
            write_silver(df, "whale_txns")
        finally:
            spark.stop()

    @task.external_python(python=ETH_ETL_PYTHON)
    def quality_check_task():
        """오늘 우리가 만든 깐깐한 품질 체크 (99% Retention)"""
        import sys
        sys.path.append("/opt/airflow")
        from src.silver.check.txn_enriched_check import run_kpi_check
        from src.silver.check.token_flow_check import run_token_flow_kpi_check
        from src.silver.spark_config import get_spark_session
        
        context = get_current_context()
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        
        spark = get_spark_session("Airflow-Silver-Quality-Check")
        try:
            # 1. 트랜잭션 유실 체크 (317만 건 기준)
            run_kpi_check(spark, dt_str)
            
            # 2. 토큰 전송 유실 체크 (245만 건 기준)
            run_token_flow_kpi_check(spark, dt_str)
        finally:
            spark.stop()

    # 0. Bronze DAG 완료 대기 (Sensor)
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze_dag",
        external_dag_id="ethereum_etl_to_gcs", # 감시할 Bronze DAG ID
        external_task_id="send_summary_report", # Bronze DAG의 마지막 태스크
        allowed_states=["success"],
        poke_interval=300, # 5분 간격으로 체크
        timeout=7200,      # 최대 2시간 대기
        mode="reschedule", # 대기 중 리소스 반환
    )

    # 1. 기초 데이터(Enriched) 생성
    enriched = build_txn_enriched_task()
    
    # 의존성 연결: Bronze가 성공해야 Enriched 가공 시작
    wait_for_bronze >> enriched
    
    # 2. Enriched 데이터를 바탕으로 토큰 흐름 및 고래 데이터 생성 (병렬 가능)
    token_flow = build_token_flow_task()
    whale_txns = build_whale_txns_task()
    
    # 의존성 연결: enriched가 끝나야 다음 두 작업이 가능함
    enriched >> [token_flow, whale_txns]
    
    # 3. 모든 가공이 완료된 후 최종 품질 체크 수행
    qc = quality_check_task()
    [token_flow, whale_txns] >> qc

ethereum_silver_dag()
