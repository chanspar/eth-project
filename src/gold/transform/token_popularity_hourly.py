from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.window import WindowSpec
from src.silver.spark_config import get_spark_session, read_silver
from src.schema.silver_schema import token_flow_schema
from src.gold.utils import write_gold, write_gold_to_bq
from src.config import get_logger


def build_token_popularity_hourly(spark: SparkSession, dt: str) -> DataFrame:
    """
    Gold Layer: token_popularity_hourly.py
    목적: 시간대별 ERC-20 토큰 인기 랭킹 + 급등 탐지
    분석 단위: token_address + dt + hour
    """
    df = read_silver(spark, "token_flow", dt, schema=token_flow_schema)
    df = df.filter(F.col("status") == 1)
    
    base = df.groupBy("dt", "hour", "token_address", "symbol").agg(
        F.count("transaction_hash").alias("tx_count"),
        F.round(F.sum("amount"), 4).alias("total_volume"),
        F.round(F.sum(
            F.when(F.col("to_category") == "CEX",   F.col("amount"))
            .when(F.col("from_category") == "CEX",  -F.col("amount"))
            .otherwise(0)
        ), 4).alias("cex_net_flow"),
    )
    
    w_lag = Window.partitionBy("token_address", "dt").orderBy("hour")
    with_trend = (
        base
        .withColumn("_prev_tx", F.lag("tx_count", 1).over(w_lag))
        .withColumn("tx_count_change_pct", F.round(
            F.when(F.col("_prev_tx") > 0,
                (F.col("tx_count") - F.col("_prev_tx")) / F.col("_prev_tx") * 100
            ), 2
        ))
        .drop("_prev_tx")
    )

    # 시간대 내 랭킹
    w_hour = Window.partitionBy("dt", "hour")

    final = (
        with_trend
        .withColumn("rank_by_tx_count",
            F.rank().over(w_hour.orderBy(F.col("tx_count").desc())))
        .withColumn("trending_signal",
            F.when(
                (F.col("tx_count_change_pct") >= 100) & (F.col("rank_by_tx_count") <= 20),
                "Surging"
            ).when(
                (F.col("tx_count_change_pct") >= 50) & (F.col("rank_by_tx_count") <= 20),
                "Rising"
            ).when(
                F.col("tx_count_change_pct") <= -50,
                "Cooling"
            ).otherwise(None))
    )

    output_cols = [
        "dt",
        "hour",
        "token_address",
        "symbol",
        "rank_by_tx_count",
        "tx_count",
        "total_volume",
        "tx_count_change_pct",
        "cex_net_flow",
        "trending_signal",
    ]
    
    return final.select(*output_cols)


def main():
    import argparse
    import time
    
    start_time = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    logger = get_logger("Main")
    spark = get_spark_session("Gold: Token Popularity hourly")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    # 실행
    token_popularity_hourly = build_token_popularity_hourly(spark, dt_val)
    
    # 저장
    logger.info(f"Gold Layer에 데이터 저장 시작: {dt_val}")
    write_gold(token_popularity_hourly, "token_popularity_hourly")
    write_gold_to_bq(token_popularity_hourly, "token_popularity_hourly")
    logger.info("데이터 저장 완료!")
    
    token_popularity_hourly.show(20, truncate=False)
    
    duration = time.time() - start_time
    logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
    
    logger.info("Spark Session 종료")
    spark.stop()


if __name__ == "__main__":
    """uv run src/gold/transform/token_popularity_hourly.py --date 2026-05-01"""
    main()
