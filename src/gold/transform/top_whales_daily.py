from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
from src.silver.spark_config import get_spark_session, read_silver
from src.gold.utils import write_gold
from src.config import get_logger

def build_top_whales_daily(spark: SparkSession, dt: str) -> DataFrame:
    """
    고래 지갑별 일일 요약 및 랭킹 생성
    """
    logger = get_logger("Build Top Whales")
    
    # 1. Silver whale_txns 읽기
    df = read_silver(spark, "whale_txns", dt)
    
    # 2. 주소별 유입/유출 집계
    # 보내는 쪽 (Outflow)
    sent = df.groupBy("from_address", "from_label", "from_category") \
             .agg(F.sum("value_eth").alias("sent_eth"),
                  F.count("hash").alias("sent_count")) \
             .withColumnRenamed("from_address", "address") \
             .withColumnRenamed("from_label", "label") \
             .withColumnRenamed("from_category", "category")

    # 받는 쪽 (Inflow)
    recv = df.groupBy("to_address", "to_label", "to_category") \
             .agg(F.sum("value_eth").alias("received_eth"),
                  F.count("hash").alias("received_count")) \
             .withColumnRenamed("to_address", "address") \
             .withColumnRenamed("to_label", "label") \
             .withColumnRenamed("to_category", "category")

    # 3. 데이터 통합 (Full Outer Join)
    whales = sent.join(recv, on=["address", "label", "category"], how="full")
    
    # Null 처리 및 순유입량 계산
    whales = whales.fillna(0, subset=["sent_eth", "received_eth", "sent_count", "received_count"]) \
                   .withColumn("net_flow_eth", F.col("received_eth") - F.col("sent_eth")) \
                   .withColumn("total_activity_eth", F.col("received_eth") + F.col("sent_eth")) \
                   .withColumn("total_tx_count", F.col("received_count") + F.col("sent_count"))

    # 4. 고래 등급(Tier) 재산출 (가장 높은 체급 기준)
    whales = whales.withColumn("whale_tier", 
        F.when(F.col("total_activity_eth") >= 10000, "Humpback")
        .when(F.col("total_activity_eth") >= 1000,  "Whale")
        .otherwise("Shark")
    )

    # 5. 날짜 컬럼 추가
    dt_val = dt.split('=')[-1]
    whales = whales.withColumn("dt", F.lit(dt_val).cast("date"))

    return whales.orderBy(F.abs(F.col("net_flow_eth")).desc())

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark_session("Gold: Top Whales Daily")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    # 실행
    top_whales = build_top_whales_daily(spark, dt_val)
    
    # 저장
    write_gold(top_whales, "top_whales_daily")
    
    top_whales.show(20, truncate=False)
    spark.stop()

if __name__ == "__main__":
    main()
