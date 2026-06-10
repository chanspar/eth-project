"""
Ethereum Gold Layer 테스트 DAG (K8s 전용)
- schedule=None: Airflow UI에서 수동 트리거만 가능
- K8s 상에서 SparkOperator가 각 Gold 파이프라인을 정상 제출하고 수행하는지 검증
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
    dag_id="ethereum_gold_test_k8s",
    start_date=datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    max_active_runs=1,
    tags=["ethereum", "gold", "spark", "test", "k8s"],
    params={"target_date": "2026-05-01"},
)
def ethereum_gold_test_dag():

    @task
    def get_target_date() -> str:
        """트리거 시 입력한 날짜를 가져오기"""
        context = get_current_context()
        target = context["params"].get("target_date")
        print(f"📅 테스트 대상 날짜: {target}")
        return target

    dt_str = get_target_date()

    build_top_whales = SparkKubernetesOperator(
        task_id="test_build_top_whales",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="test-gold-whales-{{ params.target_date | replace('-', '') }}",
            main_file="src/gold/transform/top_whales_daily.py",
            arguments=["--date", "{{ params.target_date }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    build_token_pop = SparkKubernetesOperator(
        task_id="test_build_token_popularity",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="test-gold-token-pop-{{ params.target_date | replace('-', '') }}",
            main_file="src/gold/transform/token_popularity_daily.py",
            arguments=["--date", "{{ params.target_date }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    @task
    def report_results(dt: str):
        print("=" * 60)
        print(f"🎉 Gold 레이어 K8s 전체 테스트 통과! (대상 날짜: {dt})")
        print("   - top_whales_daily 변환 (K8s) ✅")
        print("   - token_popularity_daily 변환 (K8s) ✅")
        print("=" * 60)

    dt_str >> [build_top_whales, build_token_pop]
    [build_top_whales, build_token_pop] >> report_results(dt_str)

ethereum_gold_test_dag()
