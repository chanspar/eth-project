import argparse
from pyspark.sql import SparkSession, functions as F
from src.silver.spark_config import get_spark_session, get_logger, read_silver
from src.schema.silver_schema import token_flow_schema

def run_token_flow_kpi_check(spark: SparkSession, dt: str):
    logger = get_logger("Token-Flow-KPI")
    
    dt_partition = dt if "dt=" in dt else f"dt={dt}"
    logger.info(f"🔍 Token Flow 데이터 검증 시작 (날짜: {dt_partition})")
    
    try:
        df = read_silver(spark, "token_flow", dt_partition, schema=token_flow_schema)
    except Exception as e:
        logger.error(f"❌ 데이터를 읽는 도중 에러가 발생했습니다: {e}")
        return

    total_count = df.count()
    if total_count == 0:
        logger.warning(f"⚠️ {dt_partition} 날짜에 토큰 데이터가 없습니다.")
        return

    logger.info("=" * 60)
    logger.info(f"📊 [Token Flow KPI Report] 날짜: {dt_partition}")

    # 1. 기초 지표 (Volume, DEX Ratio, Active Tokens, CV)
    aggs = df.select(
        F.sum("value_normalized").alias("total_volume"),
        F.countDistinct("token_address").alias("active_tokens"),
        F.avg(F.when((F.col("from_dex").isNotNull()) | (F.col("to_dex").isNotNull()), 1).otherwise(0)).alias("dex_ratio"),
        F.stddev("value_normalized").alias("std_val"),
        F.avg("value_normalized").alias("avg_val")
    ).collect()[0]

    cv = (aggs['std_val'] / aggs['avg_val']) if aggs['avg_val'] and aggs['avg_val'] > 0 else 0

    logger.info(f"📈 1. Total Volume       : {aggs['total_volume'] or 0:,.2f}")
    logger.info(f"💎 2. Active Tokens      : {aggs['active_tokens']:,}종")
    logger.info(f"🔄 3. DEX Ratio %        : {aggs['dex_ratio']*100:.2f}%")
    logger.info(f"📊 4. Transfer CV        : {cv:.4f}")

    # 5. 시간대별 거래량 (Hourly Volume)
    logger.info(f"⏰ 5. Hourly Volume (Top 3 Hours):")
    df.withColumn("hour", F.hour(F.from_unixtime("block_timestamp"))) \
      .groupBy("hour") \
      .agg(F.count("*").alias("tx_cnt")) \
      .orderBy(F.col("tx_cnt").desc()) \
      .show(3)

    # 6. 워시 트레이딩 의심 쌍 (Wash Trade Pairs)
    # A->B, B->A 가 같은 날 같은 토큰에서 발생 (Self-Join 컨셉)
    # 정밀한 구현을 위해 Alias 사용
    df_a = df.alias("a")
    df_b = df.alias("b")

    wash_trades = df_a.join(
        df_b,
        (F.col("a.token_address") == F.col("b.token_address")) &
        (F.col("a.from_address") == F.col("b.to_address")) &
        (F.col("a.to_address") == F.col("b.from_address")) &
        (F.col("a.transaction_hash") != F.col("b.transaction_hash")),
        how="inner"
    ).filter(
        (F.col("a.value_normalized") / F.col("b.value_normalized")).between(0.8, 1.2)
    )

    wash_count = wash_trades.count()
    logger.info(f"🧼 6. Wash Trade Pairs   : {wash_count:,}건 발견 (의심 거래)")

    # 7. 프로토콜 점유율 (DEX Share)
    logger.info(f"🏗️ 7. Protocol Share (DEX):")
    df.select(F.coalesce("from_dex", "to_dex").alias("dex")) \
      .filter(F.col("dex").isNotNull()) \
      .groupBy("dex") \
      .count() \
      .orderBy(F.col("count").desc()) \
      .show(3)

    logger.info("=" * 60)

    # 🚨 [Critical Check] 자동 품질 검증 로직
    critical_errors = []
    
    # 1. 데이터 유실 검증 (시스템 오류)
    if total_count < 1000:
        critical_errors.append(f"Too Few Transfers: {total_count:,}건")

    # 2. 토큰 감지 검증 (시스템 오류)
    if aggs['active_tokens'] == 0:
        critical_errors.append("No Active Tokens Detected")

    # 3. 워시 트레이딩 비중 (단순 정보성 출력으로 변경, 시스템을 멈추지 않음)
    wash_ratio = wash_count / total_count if total_count > 0 else 0
    if wash_ratio > 0.5:
        logger.warning(f"⚠️ 높은 워시 트레이딩 비중 감지: {wash_ratio*100:.1f}% (시장 상황 확인 필요)")

    if critical_errors:
        error_msg = " | ".join(critical_errors)
        logger.error(f"❌ [데이터 유실/오류 발생] {error_msg}")
        raise ValueError(f"Token Flow Quality Gate Failed: {error_msg}")
    
    logger.info("✅ 토큰 데이터 품질 검증 완료")

def main():
    parser = argparse.ArgumentParser(description="Ethereum Token Flow KPI Checker")
    parser.add_argument("--date", required=True, help="검증할 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session("Token-Flow-Check")
    try:
        run_token_flow_kpi_check(spark, args.date)
    finally:
        spark.stop()

if __name__ == "__main__":
    """uv run python src/silver/check/token_flow_check.py --date 2026-05-01"""
    main()
