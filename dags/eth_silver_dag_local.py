"""
Ethereum Silver Layer DAG (로컬 전용)
- Bronze DAG 완료 시 Asset 기반으로 자동 트리거
- 일반 @task 데코레이터로 PySpark 실행 (external_python 불필요)
- 3종 품질 체크 (txn_enriched, token_flow, whale_txns)
"""
import pendulum
from pendulum import datetime

# pyrefly: ignore [missing-module-attribute]
from airflow.sdk import dag, task, Asset, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.sdk.exceptions import AirflowFailException

# pyrefly: ignore [missing-import]
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

# ── Asset 정의 ────────────────────────────────────────────────────────────────
# Producer: Bronze DAG의 send_summary_report가 이 Asset을 발행
BRONZE_COMPLETE = Asset("bronze/ethereum_etl_complete")
# Consumer: 이 DAG가 발행 → Gold DAG가 트리거
SILVER_COMPLETE = Asset("silver/ethereum_silver_complete")

default_args = {
    "owner": "chanspar",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}


@dag(
    dag_id="ethereum_silver_local",
    default_args=default_args,
    start_date=datetime(2026, 5, 1, tz="Asia/Seoul"),
    # Asset 기반 스케줄링: Bronze DAG 완료 시 즉시 트리거
    # ExternalTaskSensor 대비 장점: 폴링 오버헤드 제거, 대기 시간 0
    schedule=[BRONZE_COMPLETE],
    catchup=False,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=pendulum.duration(hours=4),  # 실버 가공은 4시간 이내 완료 권장
        callback=AsyncCallback(task_fail_slack_alert),
    ),
    tags=["ethereum", "silver", "spark", "local"],
)
def ethereum_silver_local_dag():

    # ── 1단계: Bronze → Silver 기초 가공 ──────────────────────────────────────

    @task
    def build_txn_enriched_task():
        """Bronze → Silver: txn_enriched 가공 (LEFT JOIN 로직)"""
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

    # ── 2단계: Enriched 기반 파생 데이터 가공 (병렬 실행) ─────────────────────

    @task
    def build_token_flow_task():
        """Bronze → Silver: token_flow 가공 (전수 전송 내역 확보)"""
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

    @task
    def build_whale_txns_task():
        """Silver Enriched → Silver Whale: 고래 자금 흐름 가공"""
        from src.silver.transform.whale_txns import build_whale_txns
        from src.silver.spark_config import get_spark_session
        from src.silver.utils import write_silver

        context = get_current_context()
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        dt_partition = f"dt={dt_str}"

        spark = get_spark_session("Airflow-Build-Whale-Txns")
        try:
            df = build_whale_txns(spark, dt_partition, threshold_eth=100.0)
            write_silver(df, "whale_txns")
        finally:
            spark.stop()

    # ── 3단계: 품질 체크 (3종 완전 검증) ──────────────────────────────────────

    @task(outlets=[SILVER_COMPLETE])
    def quality_check_task():
        """Silver 3종 품질 체크 (txn_enriched + token_flow + whale_txns)

        기존 DAG 대비 개선: whale_txns_check 추가 → 전체 Silver 레이어 검증 완료
        """
        from src.silver.check.txn_enriched_check import run_kpi_check
        from src.silver.check.token_flow_check import run_token_flow_kpi_check
        from src.silver.check.whale_txns_check import run_whale_kpi_check
        from src.silver.spark_config import get_spark_session

        context = get_current_context()
        dt_str = context["logical_date"].strftime("%Y-%m-%d")

        spark = get_spark_session("Airflow-Silver-Quality-Check")
        try:
            # 1. 트랜잭션 유실 체크 (99% Retention)
            run_kpi_check(spark, dt_str)

            # 2. 토큰 전송 유실 체크
            run_token_flow_kpi_check(spark, dt_str)

            # 3. 고래 트랜잭션 품질 체크 (기존에 누락 — 신규 추가)
            run_whale_kpi_check(spark, dt_str)
        finally:
            spark.stop()

    # ── Task Wiring ───────────────────────────────────────────────────────────

    # 1. 기초 데이터(Enriched) 생성
    enriched = build_txn_enriched_task()

    # 2. Enriched 데이터를 바탕으로 토큰 흐름 및 고래 데이터 생성 (병렬)
    token_flow = build_token_flow_task()
    whale_txns = build_whale_txns_task()

    # 의존성: enriched가 끝나야 다음 두 작업 시작
    enriched >> [token_flow, whale_txns]

    # 3. 모든 가공이 완료된 후 최종 품질 체크 수행
    qc = quality_check_task()
    [token_flow, whale_txns] >> qc


ethereum_silver_local_dag()
