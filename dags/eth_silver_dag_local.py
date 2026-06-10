"""
Ethereum Silver Layer DAG (로컬 전용)
- Bronze DAG 완료 시 Asset 기반으로 자동 트리거
- 일반 @task 데코레이터로 PySpark 실행 (external_python 불필요)
- 3종 품질 체크 (txn_enriched, token_flow, whale_txns)
"""
import pendulum
from pendulum import datetime

# pyrefly: ignore [missing-module-attribute]
from airflow.sdk import dag, task, Asset, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference, Metadata
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
    # ── 0단계: 실행 대상 날짜 결정 ────────────────────────────────────────────

    @task
    def get_execution_date() -> str:
        """Bronze Asset 메타데이터에서 logical_date 추출, 없으면 본인의 logical_date 사용"""
        context = get_current_context()
        # Asset 트리거 시 메타데이터에서 날짜 추출
        events = (context.get("triggering_asset_events") or {}).get(BRONZE_COMPLETE, [])
        if events:
            dt_str = events[0].extra.get("logical_date")
            if dt_str:
                print(f"🎯 Asset 트리거: logical_date={dt_str}")
                return dt_str
        # 수동 트리거 또는 Cron 스케줄
        dt_str = context["logical_date"].strftime("%Y-%m-%d")
        print(f"👤 수동/Cron 트리거: logical_date={dt_str}")
        return dt_str

    # ── 1단계: Bronze → Silver 기초 가공 ──────────────────────────────────────

    @task
    def build_txn_enriched_task(dt_str: str):
        """Bronze → Silver: txn_enriched 가공 (LEFT JOIN 로직)"""
        from src.silver.transform.txn_enriched import build_txn_enriched
        from src.silver.spark_config import get_spark_session
        from src.silver.utils import write_silver

        dt_partition = f"dt={dt_str}"
        spark = get_spark_session("Airflow-Build-Txn-Enriched")
        try:
            df = build_txn_enriched(spark, dt_partition)
            write_silver(df, "txn_enriched")
        finally:
            spark.stop()

    # ── 2단계: Enriched 기반 파생 데이터 가공 (병렬 실행) ─────────────────────

    @task
    def build_token_flow_task(dt_str: str):
        """Bronze → Silver: token_flow 가공 (전수 전송 내역 확보)"""
        from src.silver.transform.token_flow import build_token_flow
        from src.silver.spark_config import get_spark_session
        from src.silver.utils import write_silver

        dt_partition = f"dt={dt_str}"
        spark = get_spark_session("Airflow-Build-Token-Flow")
        try:
            df = build_token_flow(spark, dt_partition)
            write_silver(df, "token_flow")
        finally:
            spark.stop()

    @task
    def build_whale_txns_task(dt_str: str):
        """Silver Enriched → Silver Whale: 고래 자금 흐름 가공"""
        from src.silver.transform.whale_txns import build_whale_txns
        from src.silver.spark_config import get_spark_session
        from src.silver.utils import write_silver

        dt_partition = f"dt={dt_str}"
        spark = get_spark_session("Airflow-Build-Whale-Txns")
        try:
            df = build_whale_txns(spark, dt_partition, threshold_eth=100.0)
            write_silver(df, "whale_txns")
        finally:
            spark.stop()

    # ── 3단계: 품질 체크 (3종 완전 검증) ──────────────────────────────────────

    @task
    def quality_check_task(dt_str: str):
        """Silver 3종 품질 체크 (txn_enriched + token_flow + whale_txns)"""
        from src.silver.check.txn_enriched_check import run_kpi_check
        from src.silver.check.token_flow_check import run_token_flow_kpi_check
        from src.silver.check.whale_txns_check import run_whale_kpi_check
        from src.silver.spark_config import get_spark_session

        spark = get_spark_session("Airflow-Silver-Quality-Check")
        try:
            run_kpi_check(spark, dt_str)
            run_token_flow_kpi_check(spark, dt_str)
            run_whale_kpi_check(spark, dt_str)
        finally:
            spark.stop()

    # ── 4단계: Silver 자산 이벤트 발행 ──────────────────────────────────────

    @task(outlets=[SILVER_COMPLETE])
    def publish_silver_asset(dt_str: str):
        """Silver 레이어 가공 및 검증 완료 — Asset 이벤트 발행 (logical_date 메타데이터 포함)"""
        print(f"✅ Silver 레이어 가공 완료 ({dt_str}). Gold DAG 트리거 준비.")
        yield Metadata(SILVER_COMPLETE, {"logical_date": dt_str})

    # ── Task Wiring ───────────────────────────────────────────────────────────

    dt_str = get_execution_date()

    enriched = build_txn_enriched_task(dt_str)
    token_flow = build_token_flow_task(dt_str)
    whale_txns = build_whale_txns_task(dt_str)

    enriched >> [token_flow, whale_txns]

    qc = quality_check_task(dt_str)
    [token_flow, whale_txns] >> qc

    publish = publish_silver_asset(dt_str)
    qc >> publish


ethereum_silver_local_dag()
