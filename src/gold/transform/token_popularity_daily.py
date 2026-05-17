from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.window import WindowSpec
from src.silver.spark_config import get_spark_session, read_silver
from src.schema.silver_schema import token_flow_schema
from src.gold.utils import write_gold, write_gold_to_bq
from src.config import get_logger


# tx_count, unique_addresses, total_volume 세 축을 정규화 후 가중 합산
WEIGHT_TX_COUNT          = 0.40
WEIGHT_UNIQUE_ADDRESSES  = 0.35
WEIGHT_TOTAL_VOLUME      = 0.25


def _min_max_norm(col_name: str, window: "WindowSpec") -> F.Column:
    """dt 파티션 내 min-max 정규화 (0~1)"""
    min_val = F.min(F.col(col_name)).over(window)
    max_val = F.max(F.col(col_name)).over(window)
    denom   = F.when((max_val - min_val) == 0, F.lit(1)).otherwise(max_val - min_val)
    return (F.col(col_name) - min_val) / denom
    


def build_token_popularity_daily(spark: SparkSession, dt: str) -> DataFrame:
    """
    Gold Layer: token_popularity_daily.py
    목적: 당일 ERC-20 토큰 인기 랭킹
    분석 단위: token_address + dt
    """
    
    logger = get_logger("token_popularity_daily")
    logger.info(f"[{dt}] token_popularity_daily 데이터 파이프라인 시작")
    
    # 1. 데이터 로드
    logger.info(f"[{dt}] Silver Layer에서 'token_flow' 데이터 읽어오기 (status=1 필터링)")
    df = read_silver(spark, "token_flow", dt, schema=token_flow_schema)
    df = df.filter(F.col("status") == 1)
    
    # 2. 토큰별 기본 지표 집계
    logger.info("토큰별 기본 집계(트랜잭션 수, 총 볼륨, 유니크 경로, CEX 순유입) 계산 중...")
    base = df.groupBy("dt", "token_address", "symbol", "token_name").agg(
        F.count("transaction_hash").alias("tx_count"),
        F.round(F.sum("amount"), 4).alias("total_volume"),
        F.approx_count_distinct(
            F.concat_ws("_", "from_address", "to_address")
        ).alias("unique_addresses"), # 🚨 오타 수정됨 (addresss -> addresses)
        F.round(F.sum(
            F.when(F.col("to_category") == "CEX", F.col("amount"))
            .when(F.col("from_category") == "CEX", -F.col("amount"))
            .otherwise(0)
        ), 4).alias("cex_net_flow")
    )
    
    # 3. 정규화 및 점수 계산
    logger.info("Min-Max 정규화 및 가중치 기반 인기도(Popularity Score) 산출 중...")
    w = Window.partitionBy("dt")
    
    final = (
        base
        .withColumn("_n_tx",   _min_max_norm("tx_count", w))
        .withColumn("_n_addr", _min_max_norm("unique_addresses", w))
        .withColumn("_n_vol",  _min_max_norm("total_volume", w))
        .withColumn("popularity_score", F.round(
            F.col("_n_tx")   * WEIGHT_TX_COUNT
            + F.col("_n_addr") * WEIGHT_UNIQUE_ADDRESSES
            + F.col("_n_vol")  * WEIGHT_TOTAL_VOLUME, 4
        ))
        .drop("_n_tx", "_n_addr", "_n_vol")
    )
    
    # 4. 랭킹 및 레이블 부여
    logger.info("인기도 랭킹(Tier) 및 CEX 흐름(In/Outflow) 레이블링 중...")
    final = (
        final
        .withColumn("rank_popularity",
            F.rank().over(w.orderBy(F.col("popularity_score").desc())))
        .withColumn("popularity_tier",
            F.when(F.col("rank_popularity") == 1,  "Top1")
            .when(F.col("rank_popularity") <= 3,   "Top3")
            .when(F.col("rank_popularity") <= 10,  "Top10")
            .when(F.col("rank_popularity") <= 50,  "Hot")
            .otherwise("Normal"))
        .withColumn("cex_flow_label",
            F.when(F.col("cex_net_flow") >  0, "CEX_INFLOW")
            .when(F.col("cex_net_flow") <  0, "CEX_OUTFLOW")
            .otherwise("NEUTRAL"))
    )

    output_cols = [
        "dt",
        "token_address",
        "symbol",
        "token_name",
        "rank_popularity",
        "popularity_tier",
        "popularity_score",
        "tx_count",
        "total_volume",
        "unique_addresses",
        "cex_net_flow",
        "cex_flow_label",
    ]
    
    logger.info(f"[{dt}] token_popularity_daily 데이터프레임 구성 완료!")
    return final.select(*output_cols)


def main():
    import argparse
    import time
    
    start_time = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    logger = get_logger("Main")
    spark = get_spark_session("Gold: Token Popularity Daily")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    # 실행
    token_popularity = build_token_popularity_daily(spark, dt_val)
    
    # 저장
    logger.info(f"Gold Layer에 데이터 저장 시작: {dt_val}")
    write_gold(token_popularity, "token_popularity_daily")
    write_gold_to_bq(token_popularity, "token_popularity_daily")
    logger.info("데이터 저장 완료!")
    
    token_popularity.show(20, truncate=False)
    
    duration = time.time() - start_time
    logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
    
    logger.info("Spark Session 종료")
    spark.stop()


if __name__ == "__main__":
    """uv run src/gold/transform/token_popularity_daily.py --date 2026-05-01"""
    main()
