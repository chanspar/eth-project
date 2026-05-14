import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from src.silver.spark_config import get_spark_session, get_logger, read_silver
from src.schema.silver_schema import whale_txn_schema

def run_whale_kpi_check(spark: SparkSession, dt: str):
    logger = get_logger("Whale-KPI-Check")
    
    dt_partition = dt if "dt=" in dt else f"dt={dt}"
    logger.info(f"🔍 Whale Txns 데이터 검증 시작 (날짜: {dt_partition})")
    
    try:
        # whale_txn_schema 적용하여 읽기
        df = read_silver(spark, "whale_txns", dt_partition, schema=whale_txn_schema)
    except Exception as e:
        logger.error(f"❌ 데이터를 읽는 도중 에러가 발생했습니다: {e}")
        return

    total_count = df.count()
    if total_count == 0:
        logger.warning(f"⚠️ {dt_partition} 날짜에 고래 데이터가 없습니다.")
        return

    logger.info("=" * 60)
    logger.info(f"📊 [Whale Watcher KPI Report] 날짜: {dt_partition}")
    logger.info(f"📈 1. Whale TX Count      : {total_count:,}건")

    # 집계 연산
    aggs = df.select(
        # Flag Rate %
        F.avg(F.col("has_flag").cast("int")).alias("flag_rate"),
        # Net CEX Flow (Deposit - Withdrawal)
        F.sum(F.when(F.col("flag_cex_deposit"), F.col("value_eth")).otherwise(0)).alias("total_deposit"),
        F.sum(F.when(F.col("flag_cex_withdrawal"), F.col("value_eth")).otherwise(0)).alias("total_withdrawal"),
        # CEX-to-CEX
        F.sum(F.when(F.col("flag_cex_to_cex"), F.col("value_eth")).otherwise(0)).alias("cex_to_cex_eth"),
        # High Freq Count
        F.countDistinct(F.when(F.col("flag_high_freq_sender"), F.col("from_address"))).alias("high_freq_count")
    ).collect()[0]

    net_cex_flow = (aggs['total_deposit'] or 0) - (aggs['total_withdrawal'] or 0)

    # 출력
    logger.info(f"🚩 2. Flag Rate %         : {aggs['flag_rate']*100:.2f}%")
    logger.info(f"🌊 3. Net CEX Flow        : {net_cex_flow:.4f} ETH (In: {aggs['total_deposit'] or 0:.2f} / Out: {aggs['total_withdrawal'] or 0:.2f})")
    logger.info(f"🏦 4. CEX-to-CEX ETH      : {aggs['cex_to_cex_eth'] or 0:.4f} ETH")
    logger.info(f"⚡ 5. High Freq Addrs     : {aggs['high_freq_count']:,}명")

    # 거래소별 유입 분포 (By Exchange)
    logger.info(f"🏢 6. By Exchange (Top 3):")
    df.filter(F.col("flag_cex_deposit")) \
      .groupBy("to_label") \
      .agg(F.sum("value_eth").alias("total_in")) \
      .orderBy(F.col("total_in").desc()) \
      .show(3)

    # 포지션 요약 (Accum vs Dist - 상위 3명만 예시로)
    # 실제로는 (받은금액 - 보낸금액)의 합계를 구해야 함
    logger.info(f"⚖️ 7. Top Whales (Max Sent):")
    df.select("from_address", "value_eth", "from_label") \
      .orderBy(F.col("value_eth").desc()) \
      .show(3, truncate=False)

    logger.info("=" * 60)

    # 🚨 [Critical Check] 자동 품질 검증 로직 (시스템 오류만 체크)
    if total_count < 1: # 데이터가 아예 없는 경우만 체크
        logger.error("❌ [데이터 유실] 고래 데이터가 한 건도 생성되지 않았습니다.")
        raise ValueError("Whale Data Loss: count is 0")

    # 정보성 로그 (시장이 미쳐 날뛰어도 시스템은 돌아가야 함)
    if total_count > 0 and aggs['flag_rate'] > 0.9:
        logger.warning(f"⚠️ 오늘 고래들의 90% 이상이 이상 행동(Flag)을 보이고 있습니다. (시장 상황 확인 필요)")

    logger.info("✅ 고래 데이터 품질 검증 완료")

def main():
    parser = argparse.ArgumentParser(description="Ethereum Whale Layer KPI Checker")
    parser.add_argument("--date", required=True, help="검증할 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session("Whale-KPI-Check")
    try:
        run_whale_kpi_check(spark, args.date)
    finally:
        spark.stop()

if __name__ == "__main__":
    """uv run python src/silver/check/whale_txns_check.py --date 2026-05-01
"""
    main()
