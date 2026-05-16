from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from src.silver.spark_config import get_spark_session, read_silver
from src.gold.utils import write_gold
from src.config import get_logger

def build_market_flow_hourly(spark: SparkSession, dt: str) -> DataFrame:
    """
    시간대별 시장 자금 흐름 요약
    """
    logger = get_logger("Build Market Flow")
    
    # 1. Silver whale_txns 읽기
    df = read_silver(spark, "whale_txns", dt)
    
    # 2. 시간대별, 흐름유형별 집계
    market_flow = df.groupBy("hour", "flow_type") \
        .agg(
            F.sum("value_eth").alias("total_eth"),
            F.count("hash").alias("tx_count"),
            F.countDistinct("from_address").alias("unique_senders"),
            F.countDistinct("to_address").alias("unique_receivers")
        )

    # 3. 시장 압력 점수 (단순 예시: CEX_OUT / CEX_IN 비율 등은 나중에 계산)
    # 날짜 컬럼 추가
    dt_val = dt.split('=')[-1]
    market_flow = market_flow.withColumn("dt", F.lit(dt_val).cast("date"))

    return market_flow.orderBy("hour", "flow_type")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark_session("Gold: Market Flow Hourly")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    # 실행
    flow_df = build_market_flow_hourly(spark, dt_val)
    
    # 저장
    write_gold(flow_df, "market_flow_hourly")
    
    flow_df.show(48, truncate=False) # 24시간 * flow_types
    spark.stop()

if __name__ == "__main__":
    main()
