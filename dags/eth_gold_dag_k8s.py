"""
Ethereum Gold Layer DAG (K8s 전용)
- Silver DAG 완료 시 Asset 기반으로 자동 트리거
- SparkKubernetesOperator 로 각 transform 을 독립 SparkApplication 으로 제출
- 3단계 공통 스펙 빌더(utils.spark_spec) 사용
"""
import pendulum
from pendulum import datetime

# pyrefly: ignore [missing-module-attribute]
from airflow.sdk import dag, task, Asset, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference

# pyrefly: ignore [missing-import]
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

# pyrefly: ignore [missing-import]
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert
from utils.spark_spec import build_spark_spec, SPARK_NAMESPACE, K8S_CONN_ID

SILVER_COMPLETE = Asset("silver/ethereum_silver_complete")

default_args = {
    "owner": "chanspar",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id="ethereum_gold_k8s",
    default_args=default_args,
    start_date=datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=[SILVER_COMPLETE],
    catchup=False,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    tags=["ethereum", "gold", "spark", "k8s"],
)
def ethereum_gold_k8s_dag():

    @task
    def get_execution_date() -> str:
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

    dt_str = get_execution_date()

    build_top_whales = SparkKubernetesOperator(
        task_id="build_top_whales",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="gold-top-whales-{{ ds_nodash }}",
            main_file="src/gold/transform/top_whales_daily.py",
            arguments=["--date", "{{ ds }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    build_token_pop = SparkKubernetesOperator(
        task_id="build_token_popularity",
        namespace=SPARK_NAMESPACE,
        template_spec=build_spark_spec(
            app_name="gold-token-pop-{{ ds_nodash }}",
            main_file="src/gold/transform/token_popularity_daily.py",
            arguments=["--date", "{{ ds }}"],
        ),
        kubernetes_conn_id=K8S_CONN_ID,
        get_logs=True,
    )

    dt_str >> [build_top_whales, build_token_pop]

ethereum_gold_k8s_dag()
