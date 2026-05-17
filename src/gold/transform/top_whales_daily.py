from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from src.silver.spark_config import get_spark_session, read_silver
from src.gold.utils import write_gold
from src.config import get_logger


# 고래 티어 재산정 (당일 total_activity 기준으로 재분류)
def assign_whale_tier(col_name: str) -> F.Column:
    return (
        F.when(F.col(col_name) >= 1000, "Humpback")
        .when(F.col(col_name) >= 500, "Whale")
        .when(F.col(col_name) >= 100, "Shark")
        .otherwise("Crab")
    )


def build_top_whales_daily(spark: SparkSession, dt: str) -> DataFrame:
    """
    Gold Layer: top_whales_daily.py
    ================================
    실버 레이어 whale_txns를 집계하여 지갑별 하루 요약 인덱스를 생성합니다.
    "누가 시장을 주도하고 있는가?"

    분석 단위: address + dt
    """
    logger = get_logger("Build Top Whales")
    logger.info(f"[{dt}] Top Whales Daily 데이터 파이프라인 시작")
    logger.info(f"[{dt}] Silver Layer에서 'whale_txns' 데이터 읽어오기")
    df = read_silver(spark, "whale_txns", dt)
    
    
    logger.info("송신(sent) 및 수신(recv) 데이터 집계 중...")
    sent = (
        df.groupBy("from_address", "dt")
        .agg(
            F.sum("value_eth").alias("total_sent_eth"),
            F.count("hash").alias("tx_count_as_sender"),
            # 고빈도 플래그가 한 번이라도 있으면 True
            F.max(F.col("flag_high_freq").cast("int")).alias("_hf_sent")
        )
        .withColumnRenamed("from_address", "address")
    )
    
    recv = (
        df.groupBy("to_address", "dt")
        .agg(
            F.sum("value_eth").alias("total_recv_eth"),
            F.count("hash").alias("tx_count_as_receiver")
        )
        .withColumnRenamed("to_address", "address")
    )
    
    logger.info("Outer Join 수행 및 핵심 지표/랭킹 계산 중...")
    joined = (
        sent.join(recv, on=["address", "dt"], how="outer")
        .fillna(0.0, subset=["total_sent_eth", "total_recv_eth"])
        .fillna(0, subset=["tx_count_as_sender", "tx_count_as_receiver", "_hf_sent"])
    )
    
    result = (
        joined
        .withColumn(
            "net_flow_eth", # 순유입량
            F.round(F.col("total_recv_eth") - F.col("total_sent_eth"), 6)
        ).withColumn(
            "total_activity_eth", # 총 거래대금
            F.round(F.col("total_sent_eth") + F.col("total_recv_eth"), 6)
        ).withColumn(
            "total_tx_count", # 총 거래횟수
            F.col("tx_count_as_sender") + F.col("tx_count_as_receiver")
        ).withColumn(
            "position_label",
            F.when(F.col("net_flow_eth") > 0, "Accumulator") # 매집자
            .when(F.col("net_flow_eth") < 0, "Dumper") # 투매자
            .otherwise("Neutral") # 중립
        ).withColumn(
            "whale_tier",
            assign_whale_tier("total_activity_eth")
        ).withColumn(
            "flag_high_freq", # 고빈도 플래그
            F.col("_hf_sent").cast("boolean")
        ).drop("_hf_sent")
        .withColumn( # 매집왕: net_flow_eth 내림차순
            "rank_accumulator",
            F.rank().over(
                Window.partitionBy("dt").orderBy(F.col("net_flow_eth").desc())
            )
        ).withColumn( # 투매왕: net_flow_eth 오름차순
            "rank_dumper",
            F.rank().over(
                Window.partitionBy("dt").orderBy(F.col("net_flow_eth").asc())
            )
        ).withColumn( # 전체 활동량 랭킹
            "rank_activity",
            F.rank().over(
                Window.partitionBy("dt").orderBy(F.col("total_activity_eth").desc())
            )
        )
    )
    
    
    logger.info("대표 Entity 매핑 보강 중...")
    # from_entity 이름 보강: 해당 address의 from_entity 최빈값
    entity_map = (
        df.groupBy("from_address", "dt", "from_entity", "from_category")
        .agg(F.count("*").alias("_cnt"))
        .withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("from_address", "dt").orderBy(F.col("_cnt").desc())
            ),
        )
        .filter(F.col("_rank") == 1)
        .select(
            F.col("from_address").alias("address"),
            "dt",
            F.col("from_entity").alias("entity_name"),
            F.col("from_category").alias("entity_category"),
        )
    )

    final = result.join(entity_map, on=["address", "dt"], how="left")
    
    output_cols = [
        "dt",
        "address",
        "entity_name",
        "entity_category",
        "whale_tier",
        "position_label",
        "net_flow_eth",
        "total_sent_eth",
        "total_recv_eth",
        "total_activity_eth",
        "total_tx_count",
        "tx_count_as_sender",
        "tx_count_as_receiver",
        "rank_accumulator",
        "rank_dumper",
        "rank_activity",
        "flag_high_freq",
    ]

    logger.info(f"[{dt}] 데이터 파이프라인 DAG 구성 완료")
    return final.select(*output_cols)

def main():
    import argparse
    import time
    
    start_time = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    logger = get_logger("Main")
    spark = get_spark_session("Gold: Top Whales Daily")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    # 실행
    top_whales = build_top_whales_daily(spark, dt_val)
    
    # 저장
    logger.info(f"Gold Layer에 데이터 저장 시작: {dt_val}")
    write_gold(top_whales, "top_whales_daily")
    logger.info("데이터 저장 완료!")
    
    top_whales.show(20, truncate=False)
    
    duration = time.time() - start_time
    logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
    
    logger.info("Spark Session 종료")
    spark.stop()


if __name__ == "__main__":
    """uv run src/gold/transform/top_whales_daily.py --date 2026-05-01"""
    main()
