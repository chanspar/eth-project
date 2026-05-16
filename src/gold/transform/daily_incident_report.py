from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from src.silver.spark_config import get_spark_session, read_silver
from src.gold.utils import write_gold
from src.config import get_logger

def build_daily_incident_report(spark: SparkSession, dt: str) -> DataFrame:
    """
    오늘의 주요 고래 사건 리포트 생성
    """
    logger = get_logger("Build Incident Report")
    
    # 1. Silver whale_txns 읽기
    df = read_silver(spark, "whale_txns", dt)
    
    # 2. 주요 사건 필터링 조건 정의
    # - 혹등고래(1만 ETH+)의 움직임
    # - 개인 지갑 간의 거대 이동 (PRIVATE_MOVE)
    # - 거래소 대량 유출입 (CEX_DEPOSIT/WITHDRAWAL > 5000 ETH)
    
    incidents = df.filter(
        (F.col("whale_tier") == "Humpback") | 
        ((F.col("flow_type") == "PRIVATE_MOVE") & (F.col("value_eth") >= 5000)) |
        ((F.col("flow_type").isin("CEX_DEPOSIT", "CEX_WITHDRAWAL")) & (F.col("value_eth") >= 5000))
    )

    # 3. 사건 중요도(Severity) 부여
    incidents = incidents.withColumn("severity",
        F.when(F.col("value_eth") >= 10000, "CRITICAL")
        .when(F.col("value_eth") >= 5000, "HIGH")
        .otherwise("MEDIUM")
    )

    # 4. 사건 설명(Description) 생성
    incidents = incidents.withColumn("description",
        F.concat(
            F.col("from_label"), F.lit(" -> "), F.col("to_label"),
            F.lit(" ("), F.round("value_eth", 2).cast("string"), F.lit(" ETH) - "),
            F.col("flow_type")
        )
    )

    return incidents.select(
        "hash", "block_timestamp", "severity", "description", 
        "value_eth", "flow_type", "whale_tier", "dt"
    ).orderBy(F.col("value_eth").desc())

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark_session("Gold: Daily Incident Report")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    # 실행
    report_df = build_daily_incident_report(spark, dt_val)
    
    # 저장
    write_gold(report_df, "daily_incident_report")
    
    print("\n🚨 오늘의 주요 고래 사건 보고서 🚨")
    report_df.show(20, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
