from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from src.config import GCS_GOLD_PREFIX, BUCKET_NAME
from src.gold.utils import write_gold, silver_path
from pyspark.sql.types import DoubleType, StringType
from src.silver.spark_config import get_spark_session, get_logger


# top-N 고래 주소 선별
def get_top_whales(df: DataFrame, top_n: int) -> DataFrame:
    """
    당일 기준 Top-N 개인지갑 고래 선별

    [중요] 개인지갑(Individual)만 찾기 위해:
    - address_category 가 'Unknown' 인 지갑만 대상으로 함
    - (CEX, DEX, DeFi, Bridge 등 알려진 기관 지갑은 제외)
    """
    
    # 1. 기관/거래소 지갑 제외 (개인 지갑 추정만 남김)
    individual_txns = df.filter(
        (F.col("from_category") == "Unknown") | (F.col("to_category") == "Unknown")
    )

    w_last_from = (
        Window.partitionBy("from_address", "dt")
        .orderBy(F.col("block_timestamp").desc()))
    
    w_last_to = (
        Window.partitionBy("to_address", "dt")
        .orderBy(F.col("block_timestamp").desc()))

    # 송신자 최종 누적 (개인 주소만)
    sent_final = (
        individual_txns
        .filter(F.col("from_category") == "Unknown")
        .withColumn("rn", F.row_number().over(w_last_from))
        .filter(F.col("rn") == 1)
        .select(
            F.col("from_address").alias("address"),
            F.col("from_cumul_sent_eth").cast(DoubleType()).alias("total_sent_eth"),
            F.col("from_cumul_tx_count").alias("sent_count"),
            F.col("from_category").alias("address_category"),
            F.col("from_label").alias("address_label"),
            F.col("dt"),
        )
    )
 
    # 수신자 최종 누적 (개인 주소만)
    recv_final = (
        individual_txns
        .filter(F.col("to_category") == "Unknown")
        .withColumn("rn", F.row_number().over(w_last_to))
        .filter(F.col("rn") == 1)
        .select(
            F.col("to_address").alias("address"),
            F.col("to_cumul_recv_eth").cast(DoubleType()).alias("total_recv_eth"),
            F.col("to_cumul_tx_count").alias("recv_count"),
            F.col("dt"),
        )
    )

    # 주소 기준 outer join (개인 지갑들끼리의 이동 분석)
    whales = (
        sent_final.join(recv_final, on=["address", "dt"], how="outer")
        .fillna(0, subset=["total_sent_eth", "total_recv_eth", "sent_count", "recv_count"])
        .fillna("Unknown", subset=["address_category", "address_label"])
        .withColumn("total_volume_eth",
            F.col("total_sent_eth") + F.col("total_recv_eth"))
        .withColumn("total_tx_count",
            F.col("sent_count") + F.col("recv_count"))
        # 순이동 (양수=매집, 음수=분산)
        .withColumn("net_flow_eth",
            F.round(F.col("total_recv_eth") - F.col("total_sent_eth"), 4))
        .withColumn("position",
            F.when(F.col("net_flow_eth") > 0.1, "ACCUMULATING")
             .when(F.col("net_flow_eth") < -0.1, "DISTRIBUTING")
             .otherwise("NEUTRAL"))
        # 가중 점수 (거래량 80% + 거래 횟수 20%)
        .withColumn("whale_score",
            (F.col("total_volume_eth") * 0.8 + F.col("total_tx_count") * 0.2)
            .cast(DoubleType()))
        .filter(F.col("total_volume_eth") > 0)
        .orderBy(F.col("whale_score").desc())
        .limit(top_n)
    )
 
    # 순위 컬럼
    w_rank = Window.orderBy(F.col("whale_score").desc())
    return whales.withColumn("rank", F.row_number().over(w_rank))

# ─────────────────────────────────────────────────────────────
# Step 2. 거래소 입/출금 패턴
# ─────────────────────────────────────────────────────────────
def analyze_exchange_flows(df: DataFrame) -> DataFrame:
    """
    거래소별, 고래 주소별 집계
    """
    # CEX 입금 (Unknown → CEX)
    deposits = (
        df.filter(F.col("flag_cex_deposit"))
        .groupBy("from_address", "from_label", "to_label", "dt")
        .agg(
            F.count("hash").alias("tx_count"),
            F.round(F.sum(F.col("value_eth").cast(DoubleType())), 4).alias("total_eth"),
            F.round(F.avg(F.col("value_eth").cast(DoubleType())), 4).alias("avg_eth"),
            F.round(F.max(F.col("value_eth").cast(DoubleType())), 4).alias("max_eth"),
            F.min("block_timestamp").alias("first_ts"),
            F.max("block_timestamp").alias("last_ts"),
        )
        .withColumn("flow_type", F.lit("CEX_DEPOSIT"))
        .withColumnRenamed("from_address", "whale_address")
        .withColumnRenamed("to_label", "exchange_name")
        .drop("from_label")
    )
 
    # CEX 출금 (CEX → Unknown)
    withdrawals = (
        df.filter(F.col("flag_cex_withdrawal"))
        .groupBy("to_address", "to_label", "from_label", "dt")
        .agg(
            F.count("hash").alias("tx_count"),
            F.round(F.sum(F.col("value_eth").cast(DoubleType())), 4).alias("total_eth"),
            F.round(F.avg(F.col("value_eth").cast(DoubleType())), 4).alias("avg_eth"),
            F.round(F.max(F.col("value_eth").cast(DoubleType())), 4).alias("max_eth"),
            F.min("block_timestamp").alias("first_ts"),
            F.max("block_timestamp").alias("last_ts"),
        )
        .withColumn("flow_type", F.lit("CEX_WITHDRAWAL"))
        .withColumnRenamed("to_address", "whale_address")
        .withColumnRenamed("from_label", "exchange_name")
        .drop("to_label")
    )
 
    return deposits.union(withdrawals).orderBy(F.col("total_eth").desc())
 
 
# ─────────────────────────────────────────────────────────────
# Step 3. 고래 포지션 타임라인
# ─────────────────────────────────────────────────────────────
def build_position_timeline(df: DataFrame) -> DataFrame:
    """
    [개인지갑 전용] 고래 포지션 타임라인
    """
    # 수신 이벤트 (개인 지갑인 경우만)
    recv = (
        df.filter(F.col("to_category") == "Unknown")
        .select(
            F.col("to_address").alias("address"),
            F.col("value_eth").cast(DoubleType()).alias("value_eth"),
            F.col("block_timestamp"),
            F.col("hash"),
            F.col("to_category").alias("category"),
            F.col("to_label").alias("label"),
            F.col("dt"),
        )
        .withColumn("direction", F.lit(1.0))
    )
 
    # 송신 이벤트 (개인 지갑인 경우만)
    sent = (
        df.filter(F.col("from_category") == "Unknown")
        .select(
            F.col("from_address").alias("address"),
            F.col("value_eth").cast(DoubleType()).alias("value_eth"),
            F.col("block_timestamp"),
            F.col("hash"),
            F.col("from_category").alias("category"),
            F.col("from_label").alias("label"),
            F.col("dt"),
        )
        .withColumn("direction", F.lit(-1.0))
    )
 
    events = (
        recv.union(sent)
        .withColumn("signed_eth",
            (F.col("value_eth") * F.col("direction")).cast(DoubleType()))
    )
 
    # 주소별 timestamp 순 누적합
    w_cumul = (
        Window.partitionBy("address")
        .orderBy("block_timestamp")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_lag = Window.partitionBy("address").orderBy("block_timestamp")
 
    timeline = (
        events
        .withColumn("cumul_net_eth", F.sum("signed_eth").over(w_cumul))
        .withColumn("prev_cumul_net_eth",
            F.lag("cumul_net_eth", 1).over(w_lag))
        .withColumn("position_flip",
            F.when(
                F.col("prev_cumul_net_eth").isNotNull() &
                (F.signum("prev_cumul_net_eth") != F.signum("cumul_net_eth")),
                True
            ).otherwise(False))
        .withColumn("current_position",
            F.when(F.col("cumul_net_eth") > 0, "ACCUMULATING")
             .when(F.col("cumul_net_eth") < 0, "DISTRIBUTING")
             .otherwise("NEUTRAL"))
    )
 
    return timeline
 
 
# ─────────────────────────────────────────────────────────────
# Step 4. 시간대별 고래 활동 패턴
# ─────────────────────────────────────────────────────────────
def build_hourly_pattern(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("hour",
            F.hour(F.from_unixtime(F.col("block_timestamp"))))
        .groupBy("hour", "dt")
        .agg(
            F.count("hash").alias("tx_count"),
            F.round(F.sum(F.col("value_eth").cast(DoubleType())), 4).alias("total_eth"),
            F.round(F.avg(F.col("value_eth").cast(DoubleType())), 4).alias("avg_eth"),
            F.round(F.max(F.col("value_eth").cast(DoubleType())), 4).alias("max_eth"),
            F.countDistinct("from_address").alias("unique_senders"),
            F.countDistinct("to_address").alias("unique_receivers"),
            F.sum(F.col("flag_cex_deposit").cast("int")).alias("cex_deposit_cnt"),
            F.sum(F.col("flag_cex_withdrawal").cast("int")).alias("cex_withdrawal_cnt"),
            F.sum(F.col("flag_dex_swap").cast("int")).alias("dex_swap_cnt"),
            F.sum(F.col("flag_high_freq_sender").cast("int")).alias("high_freq_cnt"),
        )
        .orderBy("dt", "hour")
    )
 
 
# ─────────────────────────────────────────────────────────────
# Step 5. 알림 이벤트 통합
# ─────────────────────────────────────────────────────────────
def build_alert_events(df: DataFrame, position_timeline: DataFrame) -> DataFrame:
    events = []
 
    value_99th = (
        df.approxQuantile("value_eth", [0.95], 0.01)[0]
        if df.count() > 0 else 0.0
    )
    large_single = (
        df.filter(F.col("value_eth").cast(DoubleType()) >= float(value_99th))
        .select(
            F.col("hash").alias("tx_hash"),
            F.col("from_address"),
            F.col("to_address"),
            F.col("value_eth").cast(DoubleType()).alias("value_eth"),
            F.col("block_timestamp"),
            F.col("from_label"),
            F.col("to_label"),
            F.lit("LARGE_SINGLE").alias("event_type"),
            F.lit("상위 5% 대규모 단건 이동").alias("detail"),
        )
    )
    events.append(large_single)
 
    cex_dep = (
        df.filter(F.col("flag_cex_deposit"))
        .select(
            F.col("hash").alias("tx_hash"),
            F.col("from_address"),
            F.col("to_address"),
            F.col("value_eth").cast(DoubleType()).alias("value_eth"),
            F.col("block_timestamp"),
            F.col("from_label"),
            F.col("to_label"),
            F.lit("CEX_DEPOSIT").alias("event_type"),
            F.concat(F.lit("거래소 입금 → "), F.col("to_label")).alias("detail"),
        )
    )
    events.append(cex_dep)
 
    cex_wd = (
        df.filter(F.col("flag_cex_withdrawal"))
        .select(
            F.col("hash").alias("tx_hash"),
            F.col("from_address"),
            F.col("to_address"),
            F.col("value_eth").cast(DoubleType()).alias("value_eth"),
            F.col("block_timestamp"),
            F.col("from_label"),
            F.col("to_label"),
            F.lit("CEX_WITHDRAWAL").alias("event_type"),
            F.concat(F.lit("거래소 출금 ← "), F.col("from_label")).alias("detail"),
        )
    )
    events.append(cex_wd)
 
    high_freq = (
        df.filter(F.col("flag_high_freq_sender"))
        .select(
            F.col("hash").alias("tx_hash"),
            F.col("from_address"),
            F.col("to_address"),
            F.col("value_eth").cast(DoubleType()).alias("value_eth"),
            F.col("block_timestamp"),
            F.col("from_label"),
            F.col("to_label"),
            F.lit("HIGH_FREQ").alias("event_type"),
            F.concat(
                F.lit("당일 "),
                F.col("from_cumul_tx_count").cast(StringType()),
                F.lit("회 / "),
                F.round(F.col("from_cumul_sent_eth").cast(DoubleType()), 1).cast(StringType()),
                F.lit(" ETH")
            ).alias("detail"),
        )
    )
    events.append(high_freq)
 
    if position_timeline is not None:
        flip = (
            position_timeline
            .filter(F.col("position_flip"))
            .select(
                F.col("hash").alias("tx_hash"),
                F.col("address").alias("from_address"),
                F.lit(None).cast(StringType()).alias("to_address"),
                F.col("signed_eth").alias("value_eth"),
                F.col("block_timestamp"),
                F.col("label").alias("from_label"),
                F.lit(None).cast(StringType()).alias("to_label"),
                F.when(F.col("current_position") == "DISTRIBUTING",
                       F.lit("NET_SELL"))
                 .otherwise(F.lit("NET_BUY")).alias("event_type"),
                F.concat(
                    F.col("current_position"),
                    F.lit(" 전환, 누적 순이동: "),
                    F.round("cumul_net_eth", 2).cast(StringType()),
                    F.lit(" ETH")
                ).alias("detail"),
            )
        )
        events.append(flip)
 
    from functools import reduce
    all_events = reduce(lambda a, b: a.union(b), events)
    return (
        all_events
        .withColumn("dt",
            F.to_date(F.from_unixtime("block_timestamp")))
        .orderBy(F.col("value_eth").desc())
    )
 
 
# ─────────────────────────────────────────────────────────────
# 요약 출력
# ─────────────────────────────────────────────────────────────
def run_summary(df: DataFrame, top_whales: DataFrame,
                exchange_flows: DataFrame, hourly_pattern: DataFrame):
    total = df.count()
    flagged = df.filter(F.col("has_flag")).count()
 
    print(f"\n{'═'*55}")
    print(f"  🐋  Gold whale_analysis 요약")
    print(f"{'═'*55}")
    print(f"  총 고래 거래          : {total:>8,}")
    print(f"  플래그 있는 거래       : {flagged:>8,}  ({flagged/total*100:.1f}% if total>0 else 0%)")
 
    print(f"\n  ── Top-20 고래 주소 ──")
    top_whales.select(
        "rank", "address",
        F.round("total_volume_eth", 2).alias("volume"),
        F.round("net_flow_eth", 2).alias("net"),
        "position", "address_category",
    ).show(20, truncate=False)
 
    print(f"  ── 거래소 흐름 Top-10 ──")
    exchange_flows.show(10, truncate=False)
 
    print(f"  ── 시간대별 활동 ──")
    hourly_pattern.show(24, truncate=False)
 
 
# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    import argparse
    import time
    parser = argparse.ArgumentParser(description="Gold Layer: 고래 ETH 이동 추적")
    parser.add_argument("--date",    required=True, help="YYYY-MM-DD 또는 dt=YYYY-MM-DD")
    parser.add_argument("--top-n",   type=int, default=50, help="Top-N 고래 수 (기본: 50)")
    parser.add_argument("--summary", action="store_true")
    args = parser.parse_args()
 
    logger = get_logger("GoldWhaleAnalysis")
    spark = get_spark_session("GoldWhaleAnalysis")
    spark.sparkContext.setLogLevel("WARN")
 
    # 날짜 값 처리
    date_only = args.date.replace("dt=", "")
    dt_val = f"dt={date_only}"
    start = time.time()

    logger.info(f"🐋 Gold whale_analysis 시작: {dt_val}  top_n={args.top_n}")

    # 데이터 로드 및 dt 컬럼 추가 (파티션 경로 직접 로드 시 dt 컬럼이 누락됨)
    df = spark.read.parquet(silver_path("whale_txns", dt_val))
    df = df.withColumn("dt", F.lit(date_only))
    df.cache()
    logger.info(f"whale_txns 로드 완료: {df.count():,}건")
 
    logger.info("[1/5] Top-N 고래 선별...")
    top_whales = get_top_whales(df, args.top_n)
    top_whales.cache()
 
    logger.info("[2/5] 거래소 입출금 패턴...")
    exchange_flows = analyze_exchange_flows(df)
 
    logger.info("[3/5] 포지션 타임라인...")
    position_timeline = build_position_timeline(df)
 
    logger.info("[4/5] 시간대별 활동...")
    hourly_pattern = build_hourly_pattern(df)
 
    logger.info("[5/5] 알림 이벤트 통합...")
    alert_events = build_alert_events(df, position_timeline)
 
    if args.summary:
        run_summary(df, top_whales, exchange_flows, hourly_pattern)
 
    logger.info("💾 Gold 레이어 저장 중...")
    write_gold(top_whales,        "top_whales",        ["dt"])
    write_gold(exchange_flows,    "exchange_flows",    ["flow_type", "dt"])
    write_gold(position_timeline, "position_timeline")
    write_gold(hourly_pattern,    "hourly_pattern",    ["dt"])
    write_gold(alert_events,      "alert_events",      ["event_type", "dt"])
 
    df.unpersist()
    top_whales.unpersist()
    spark.stop()
 
    elapsed = time.time() - start
    logger.info(f"✅ 완료: {int(elapsed//60)}분 {elapsed%60:.1f}초")
    logger.info(f"📂 저장 위치: gs://{BUCKET_NAME}/{GCS_GOLD_PREFIX}/whale_analysis/")
 
 
if __name__ == "__main__":
    """uv run src/gold/whale_analysis.py --date 2026-05-01 --top-n 50 --summary
"""
    main()
