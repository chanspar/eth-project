"""
Ethereum Silver Layer 테스트 DAG (수동 트리거 전용)
- schedule=None: Airflow UI에서 수동 트리거만 가능
- Silver 레이어 Spark 변환이 정상 작동하는지 검증
- 각 태스크를 독립적으로 실행하여 문제 지점 빠르게 파악
"""
from pendulum import datetime

# pyrefly: ignore [missing-module-attribute]
from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id="ethereum_silver_test",
    start_date=datetime(2026, 5, 1, tz="Asia/Seoul"),
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    max_active_runs=1,
    tags=["ethereum", "silver", "spark", "test"],
    # Airflow UI에서 트리거 시 날짜를 파라미터로 입력 가능
    params={"target_date": "2026-05-01"},
)
def ethereum_silver_test_dag():

    @task
    def get_target_date() -> str:
        """트리거 시 입력한 날짜를 가져오거나, 기본값 사용"""
        context = get_current_context()
        target = context["params"].get("target_date")
        print(f"📅 테스트 대상 날짜: {target}")
        return target

    @task
    def test_spark_session():
        """Spark 세션 생성 및 기본 동작 테스트"""
        from src.silver.spark_config import get_spark_session
        from pyspark.sql import functions as F

        spark = get_spark_session("Silver-Spark-Test")
        try:
            print(f"✅ SparkSession 생성 성공")
            print(f"   Spark Version : {spark.version}")
            print(f"   App Name      : {spark.sparkContext.appName}")
            print(f"   Master        : {spark.sparkContext.master}")

            # 간단한 DataFrame 연산 테스트
            df = spark.createDataFrame(
                [("0xabc", "0xdef", 1.5), ("0xdef", "0xghi", 100.0)],
                ["from_address", "to_address", "value_eth"],
            )
            result = df.agg(F.count("*").alias("cnt"), F.sum("value_eth").alias("total")).collect()[0]
            print(f"   DataFrame 테스트: {result['cnt']}건, {result['total']} ETH")
            print("✅ Spark 기본 동작 정상")
        finally:
            spark.stop()

    @task
    def test_gcs_read(target_date: str):
        """GCS Bronze 데이터 읽기 테스트"""
        from src.silver.spark_config import get_spark_session, read_bronze
        from src.schema.bronze_schema import transaction_schema

        dt_partition = f"dt={target_date}"
        spark = get_spark_session("Silver-GCS-Read-Test")
        try:
            df = read_bronze(spark, "transactions", dt_partition, schema=transaction_schema)
            count = df.count()
            print(f"✅ GCS Bronze 읽기 성공: transactions/{dt_partition} → {count:,}건")
            df.show(3, truncate=False)
        finally:
            spark.stop()

    @task
    def test_build_txn_enriched(target_date: str):
        """txn_enriched 변환 테스트 (저장하지 않고 결과만 확인)"""
        from src.silver.transform.txn_enriched import build_txn_enriched
        from src.silver.spark_config import get_spark_session

        dt_partition = f"dt={target_date}"
        spark = get_spark_session("Silver-Test-Txn-Enriched")
        try:
            df = build_txn_enriched(spark, dt_partition)
            df.cache()
            count = df.count()
            print(f"✅ txn_enriched 변환 성공: {count:,}건")
            df.show(5, truncate=False)
            df.printSchema()
            df.unpersist()
        finally:
            spark.stop()

    @task
    def test_build_token_flow(target_date: str):
        """token_flow 변환 테스트 (저장하지 않고 결과만 확인)"""
        from src.silver.transform.token_flow import build_token_flow
        from src.silver.spark_config import get_spark_session

        dt_partition = f"dt={target_date}"
        spark = get_spark_session("Silver-Test-Token-Flow")
        try:
            df = build_token_flow(spark, dt_partition)
            df.cache()
            count = df.count()
            print(f"✅ token_flow 변환 성공: {count:,}건")
            df.show(5, truncate=False)
            df.printSchema()
            df.unpersist()
        finally:
            spark.stop()

    @task
    def test_build_whale_txns(target_date: str):
        """whale_txns 변환 테스트 (저장하지 않고 결과만 확인)"""
        from src.silver.transform.whale_txns import build_whale_txns
        from src.silver.spark_config import get_spark_session

        dt_partition = f"dt={target_date}"
        spark = get_spark_session("Silver-Test-Whale-Txns")
        try:
            df = build_whale_txns(spark, dt_partition, threshold_eth=100.0)
            df.cache()
            count = df.count()
            print(f"✅ whale_txns 변환 성공: {count:,}건")
            df.show(5, truncate=False)
            df.printSchema()
            df.unpersist()
        finally:
            spark.stop()

    @task
    def report_results():
        """모든 테스트 통과 리포트"""
        print("=" * 60)
        print("🎉 Silver 레이어 전체 Spark 테스트 통과!")
        print("   - Spark 세션 생성 ✅")
        print("   - GCS Bronze 읽기 ✅")
        print("   - txn_enriched 변환 ✅")
        print("   - token_flow 변환 ✅")
        print("   - whale_txns 변환 ✅")
        print("=" * 60)

    # ── Task Wiring ───────────────────────────────────────────────────────────

    dt = get_target_date()
    spark_ok = test_spark_session()

    # Spark 세션 테스트 통과 후 → GCS 읽기 테스트
    gcs_ok = test_gcs_read(dt)
    spark_ok >> gcs_ok

    # GCS 읽기 통과 후 → 각 변환 테스트 (병렬)
    enriched = test_build_txn_enriched(dt)
    token_flow = test_build_token_flow(dt)
    whale_txns = test_build_whale_txns(dt)

    gcs_ok >> [enriched, token_flow, whale_txns]

    # 모든 변환 통과 후 → 결과 리포트
    report = report_results()
    [enriched, token_flow, whale_txns] >> report


ethereum_silver_test_dag()
