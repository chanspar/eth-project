"""
Ethereum Silver Layer 테스트 DAG (K8s 전용)
- schedule=None: Airflow UI에서 수동 트리거만 가능
- K8s 상에서 SparkOperator가 각 잡을 정상 제출하고 수행하는지 검증
"""
import pendulum
from pendulum import datetime

# pyrefly: ignore [missing-module-attribute]
from airflow.sdk import dag, task, get_current_context

# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# pyrefly: ignore [missing-import]
from utils.spark_spec import build_spark_spec, SPARK_NAMESPACE, K8S_CONN_ID

@dag(
    dag_id="ethereum_silver_test_k8s",
    start_date=datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    max_active_runs=1,
    tags=["ethereum", "silver", "spark", "test", "k8s"],
    # Airflow UI에서 트리거 시 날짜를 파라미터로 입력 가능
    params={"target_date": "2026-05-01"},
)
def ethereum_silver_test_dag():

    @task
    def get_target_date() -> str:
        """트리거 시 입력한 날짜를 가져오기"""
        context = get_current_context()
        target = context["params"].get("target_date")
        print(f"📅 테스트 대상 날짜: {target}")
        # pyrefly: ignore [bad-return]
        return target

    dt_str = get_target_date()

    # K8s Operator 기반 태스크
    build_txn_enriched = SparkKubernetesOperator(
        task_id="test_build_txn_enriched",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="test-silver-txn-{{ params.target_date | replace('-', '') }}",
            main_file="src/silver/transform/txn_enriched.py",
            arguments=["--date", "{{ params.target_date }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    build_token_flow = SparkKubernetesOperator(
        task_id="test_build_token_flow",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="test-silver-token-{{ params.target_date | replace('-', '') }}",
            main_file="src/silver/transform/token_flow.py",
            arguments=["--date", "{{ params.target_date }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    build_whale_txns = SparkKubernetesOperator(
        task_id="test_build_whale_txns",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="test-silver-whale-{{ params.target_date | replace('-', '') }}",
            main_file="src/silver/transform/whale_txns.py",
            arguments=["--date", "{{ params.target_date }}", "--whale-threshold", "100.0"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    quality_check = SparkKubernetesOperator(
        task_id="test_quality_check",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="test-silver-qc-{{ params.target_date | replace('-', '') }}",
            main_file="src/jobs/silver_quality_check_job.py",
            arguments=["--date", "{{ params.target_date }}"],
            driver_memory="2g",
            executor_memory="2g",
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    @task
    def report_results(dt: str):
        """모든 테스트 통과 리포트"""
        print("=" * 60)
        print(f"🎉 Silver 레이어 K8s 전체 테스트 통과! (대상 날짜: {dt})")
        print("   - txn_enriched 변환 (K8s) ✅")
        print("   - token_flow 변환 (K8s) ✅")
        print("   - whale_txns 변환 (K8s) ✅")
        print("   - quality_check (K8s) ✅")
        print("=" * 60)

    # Task Wiring
    dt_str >> build_txn_enriched
    build_txn_enriched >> [build_token_flow, build_whale_txns]
    [build_token_flow, build_whale_txns] >> quality_check
    quality_check >> report_results(dt_str)

ethereum_silver_test_dag()
