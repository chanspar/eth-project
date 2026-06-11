"""
Ethereum Silver Layer DAG (K8s 전용)
- Bronze K8s DAG 완료 시 Asset 기반으로 자동 트리거
- SparkKubernetesOperator 로 각 transform 을 독립 SparkApplication 으로 제출
- Spark on K8s 클러스터에서 분산 처리
"""
# pyrefly: ignore [missing-import]
import pendulum
# pyrefly: ignore [missing-import]
from pendulum import datetime

# pyrefly: ignore [missing-import, missing-module-attribute]
from airflow.sdk import dag, task, Asset, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference, Metadata
# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# pyrefly: ignore [missing-import]
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert
from utils.spark_spec import build_spark_spec, SPARK_NAMESPACE, K8S_CONN_ID

# ── Asset 정의 ────────────────────────────────────────────────────────────────
BRONZE_K8S_COMPLETE = Asset("bronze/ethereum_etl_k8s_complete")
SILVER_K8S_COMPLETE = Asset("silver/ethereum_silver_complete")

default_args = {
    "owner": "chanspar",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}



@dag(
    dag_id="ethereum_silver_k8s",
    default_args=default_args,
    start_date=datetime(2026, 6, 1, tz="Asia/Seoul"),
    # Asset 기반 스케줄링: Bronze K8s DAG 완료 시 즉시 트리거
    schedule=[BRONZE_K8S_COMPLETE],
    catchup=False,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=pendulum.duration(hours=4),
        callback=AsyncCallback(task_fail_slack_alert),
    ),
    tags=["ethereum", "silver", "spark", "k8s"],
)
def ethereum_silver_k8s_dag():

    @task
    def get_execution_date() -> str:
        """Bronze Asset 메타데이터에서 logical_date 추출, 없으면 본인의 logical_date 사용"""
        context = get_current_context()
        # Asset 트리거 시 메타데이터에서 날짜 추출
        events = (context.get("triggering_asset_events") or {}).get(BRONZE_K8S_COMPLETE, [])
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

    dt_str = get_execution_date()

    build_txn_enriched = SparkKubernetesOperator(
        task_id="build_txn_enriched",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="silver-txn-enriched-{{ ti.xcom_pull(task_ids='get_execution_date') | replace('-', '') }}",
            main_file="src/silver/transform/txn_enriched.py",
            arguments=["--date", "{{ ti.xcom_pull(task_ids='get_execution_date') }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    # ── 2단계: Enriched 기반 파생 데이터 가공 (병렬 실행) ─────────────────────

    build_token_flow = SparkKubernetesOperator(
        task_id="build_token_flow",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="silver-token-flow-{{ ti.xcom_pull(task_ids='get_execution_date') | replace('-', '') }}",
            main_file="src/silver/transform/token_flow.py",
            arguments=["--date", "{{ ti.xcom_pull(task_ids='get_execution_date') }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    build_whale_txns = SparkKubernetesOperator(
        task_id="build_whale_txns",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="silver-whale-txns-{{ ti.xcom_pull(task_ids='get_execution_date') | replace('-', '') }}",
            main_file="src/silver/transform/whale_txns.py",
            arguments=["--date", "{{ ti.xcom_pull(task_ids='get_execution_date') }}", "--whale-threshold", "100.0"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    # ── 3단계: 품질 체크 (3종 완전 검증) ──────────────────────────────────────

    # quality_check = SparkKubernetesOperator(
    #     task_id="quality_check",
    #     namespace=SPARK_NAMESPACE,
    #     template_spec=build_spark_spec(
    #         app_name="silver-quality-check-{{ ti.xcom_pull(task_ids='get_execution_date') | replace('-', '') }}",
    #         main_file="src/jobs/silver_quality_check_job.py",
    #         arguments=["--date", "{{ ti.xcom_pull(task_ids='get_execution_date') }}"],
    #         driver_memory="2g",
    #         executor_memory="2g",
    #     ),
    #     kubernetes_conn_id=K8S_CONN_ID,
    #     get_logs=True,
    # )

    # 품질 체크 완료 시 Silver Asset 발행 → Gold DAG 트리거
    @task(outlets=[SILVER_K8S_COMPLETE])
    def publish_silver_asset(dt_str: str):
        """Silver 레이어 가공 및 검증 완료 — Asset 이벤트 발행 (logical_date 메타데이터 포함)"""
        print(f"✅ Silver K8s 레이어 가공 완료 ({dt_str}). Gold DAG 트리거 준비.")
        yield Metadata(SILVER_K8S_COMPLETE, {"logical_date": dt_str})

    # ── Task Wiring ───────────────────────────────────────────────────────────

    # dt_str → build_txn_enriched (데이터 의존성은 Jinja 템플릿으로 처리)
    dt_str >> build_txn_enriched

    # enriched → [token_flow, whale_txns] (병렬)
    build_txn_enriched >> [build_token_flow, build_whale_txns]

    # [token_flow, whale_txns] → publish_asset
    [build_token_flow, build_whale_txns] >> publish_silver_asset(dt_str)


ethereum_silver_k8s_dag()
