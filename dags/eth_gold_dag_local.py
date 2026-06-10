"""
Ethereum Gold Layer DAG (로컬 전용)
- Silver DAG 완료 시 Asset 기반으로 자동 트리거
- 일반 @task 데코레이터로 PySpark 실행
"""
import pendulum
from pendulum import datetime

# pyrefly: ignore [missing-module-attribute]
from airflow.sdk import dag, task, Asset, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference

# pyrefly: ignore [missing-import]
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

SILVER_COMPLETE = Asset("silver/ethereum_silver_complete")

default_args = {
    "owner": "chanspar",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}

@dag(
    dag_id="ethereum_gold_local",
    default_args=default_args,
    start_date=datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=[SILVER_COMPLETE],
    catchup=False,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    tags=["ethereum", "gold", "spark", "local"],
)
def ethereum_gold_local_dag():

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

    @task
    def top_whales_daily_task(dt_str: str):
        """Gold: top_whales_daily 가공 및 BigQuery 적재"""
        from src.gold.transform.top_whales_daily import build_top_whales_daily
        from src.silver.spark_config import get_spark_session
        from src.gold.utils import write_gold, write_gold_to_bq
        
        dt_partition = f"dt={dt_str}"
        spark = get_spark_session("Airflow-Gold-Top-Whales")
        try:
            df = build_top_whales_daily(spark, dt_partition)
            write_gold(df, "top_whales_daily")
            write_gold_to_bq(df, "top_whales_daily")
        finally:
            spark.stop()

    @task
    def token_popularity_daily_task(dt_str: str):
        """Gold: token_popularity_daily 가공 및 BigQuery 적재"""
        from src.gold.transform.token_popularity_daily import build_token_popularity_daily
        from src.silver.spark_config import get_spark_session
        from src.gold.utils import write_gold, write_gold_to_bq
        
        dt_partition = f"dt={dt_str}"
        spark = get_spark_session("Airflow-Gold-Token-Popularity")
        try:
            df = build_token_popularity_daily(spark, dt_partition)
            write_gold(df, "token_popularity_daily")
            write_gold_to_bq(df, "token_popularity_daily")
        finally:
            spark.stop()

    dt_str = get_execution_date()
    top_whales = top_whales_daily_task(dt_str)
    token_pop = token_popularity_daily_task(dt_str)

    dt_str >> [top_whales, token_pop]

ethereum_gold_local_dag()
