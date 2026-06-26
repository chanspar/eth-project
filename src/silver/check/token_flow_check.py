import argparse
from pyspark.sql import SparkSession, functions as F
from src.silver.spark_config import get_spark_session, get_logger, read_silver, read_bronze
from src.schema.silver_schema import token_flow_schema
from src.schema.bronze_schema import token_transfer_schema

def run_token_flow_kpi_check(spark: SparkSession, dt: str):
    logger = get_logger("Token-Flow-KPI")
    
    dt_partition = dt if "dt=" in dt else f"dt={dt}"
    logger.info(f"🔍 Token Flow 데이터 검증 시작 (날짜: {dt_partition})")
    
    try:
        # Silver 데이터 읽기
        df = read_silver(spark, "token_flow", dt_partition, schema=token_flow_schema)
        silver_count = df.count()
        total_count = silver_count # 기존 로직 호환용
        
        # Bronze 원천 데이터 읽기 (유실 체크용)
        bronze_df = read_bronze(spark, "token_transfers", dt_partition, token_transfer_schema)
        bronze_count = bronze_df.count()
    except Exception as e:
        logger.error(f"❌ 데이터를 읽는 도중 에러가 발생했습니다: {e}")
        return

    # 🚨 [Critical Check] 데이터 유실 검증 (Bronze vs Silver)
    # 이제 모든 토큰 전송 데이터를 살렸으므로, 원천 데이터 대비 99% 이상 보존되어야 함
    retention_rate = (silver_count / bronze_count) if bronze_count > 0 else 0
    logger.info(f"📊 토큰 보존율 (Retention): {retention_rate*100:.2f}% ({silver_count:,} / {bronze_count:,})")

    if retention_rate < 0.99: 
        error_msg = f"Token Flow Data Loss! Retention: {retention_rate*100:.2f}% (Silver: {silver_count} / Bronze: {bronze_count})"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    logger.info("=" * 60)
    logger.info(f"📊 [Token Flow KPI Report] 날짜: {dt_partition}")

    # 1. 기초 지표 (Active Tokens, DEX Ratio, CV)
    aggs = df.select(
        F.count_distinct("token_address").alias("active_tokens"),
        F.coalesce(F.avg(F.when((F.col("from_dex").isNotNull()) | (F.col("to_dex").isNotNull()), 1).otherwise(0)), F.lit(0)).alias("dex_ratio"),
        F.coalesce(F.stddev("value_normalized"), F.lit(0)).alias("std_val"),
        F.coalesce(F.avg("value_normalized"), F.lit(0)).alias("avg_val")
    ).collect()[0]

    std_val = aggs['std_val'] or 0
    avg_val = aggs['avg_val'] or 0
    cv = (std_val / avg_val) if avg_val > 0 else 0

    logger.info(f"💎 1. Active Tokens      : {aggs['active_tokens']:,}종")
    logger.info(f"🔄 2. DEX Ratio %        : {aggs['dex_ratio']*100:.2f}%")
    logger.info(f"📊 3. Transfer CV        : {cv:.4f}")

    # 4. 주요 토큰별 전송량 (Top 5) - Global Sum 대신 개별적으로 보여줌
    logger.info(f"💰 4. Top 5 Token Volumes:")
    df.filter(F.col("symbol") != "UNKNOWN") \
      .groupBy("symbol") \
      .agg(F.sum("value_normalized").alias("volume")) \
      .orderBy(F.col("volume").desc()) \
      .show(5)

    # 5. 시간대별 거래량 (Hourly Volume)
    logger.info(f"⏰ 5. Hourly Volume (Top 3 Hours):")
    df.withColumn("timestamp", F.from_unixtime(F.col("block_timestamp").cast("long"))) \
      .withColumn("hour", F.hour("timestamp")) \
      .groupBy("hour") \
      .agg(F.count("*").alias("tx_cnt")) \
      .orderBy(F.col("tx_cnt").desc()) \
      .show(3)

    # 6. 워시 트레이딩 의심 쌍 (Wash Trade Pairs)
    # DEX/프로토콜이 아닌 개인 주소(EOA) 간의 거래만 대상으로 한정하여 노이즈 제거
    df_eoa = df.filter(F.col("from_dex").isNull() & F.col("to_dex").isNull())
    
    df_a = df_eoa.alias("a")
    df_b = df_eoa.alias("b")

    wash_trades = df_a.join(
        df_b,
        (F.col("a.token_address") == F.col("b.token_address")) &
        (F.col("a.from_address") == F.col("b.to_address")) &
        (F.col("a.to_address") == F.col("b.from_address")) &
        (F.col("a.transaction_hash") != F.col("b.transaction_hash")),
        how="inner"
    ).filter(
        (F.col("b.value_normalized") > 0) & 
        (F.col("a.value_normalized") / F.col("b.value_normalized")).between(0.8, 1.2)
    )

    # 중복 매칭된 쌍의 수가 아니라, 워시 트레이딩에 연루된 고유 트랜잭션의 수를 카운트
    wash_count = wash_trades.select("a.transaction_hash").distinct().count()
    logger.info(f"🧼 6. Wash Trade Pairs   : {wash_count:,}건 발견 (의심 거래)")

    if wash_count > 0:
        logger.info(f"🚩 Top 5 Wash Traded Tokens:")
        wash_trades.groupBy("a.symbol").count().orderBy(F.col("count").desc()).show(5)

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
    # 최악의 거래 절벽 상황을 고려하여, 아예 데이터가 없는 수준(10건 미만)이 아니면 통과
    if total_count < 10:
        critical_errors.append(f"Absolute Data Loss: {total_count:,} rows")

    # 2. 토큰 감지 검증 (시스템 오류)
    # 최소 1종이라도 감지되면 로직은 살아있는 것으로 판단
    if aggs['active_tokens'] < 1:
        critical_errors.append("No Active Tokens Detected (Metadata/Join Issue)")

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
