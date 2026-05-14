from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from src.config import GCS_GOLD_PREFIX, BUCKET_NAME
from src.gold.utils import write_gold, silver_path
from pyspark.sql.types import DoubleType, StringType
from src.silver.spark_config import get_spark_session, get_logger

NULL_ADDRESSES = [
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
]

STABLECOINS = [
    "0xdac17f958d2ee523a2206206994597c13d831ec7", # USDT
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", # USDC
    "0x6b175474e89094c44da98b954eedeac495271d0f", # DAI
    "0x4fabb145d64652a948d72533023f6e7a623c7c53", # BUSD
]


# ─────────────────────────────────────────────────────────────
# Step 1. 토큰별 거래량 종합 랭킹
# ─────────────────────────────────────────────────────────────
def build_token_ranking(df: DataFrame, top_n: int) -> DataFrame:
    """
    ERC-20 토큰별 당일 종합 지표 (유기적 성장 위주)
    """
    # 1. 워시 트레이딩(자전거래) 감지 로직
    # 동일 날짜, 동일 토큰에 대해 A->B, B->A 거래가 비슷한 금액으로 발생한 경우
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
    ).select(F.col("a.transaction_hash").alias("wash_tx_hash")).distinct()

    # 원본 데이터에 워시트레이딩 플래그 결합
    df_with_wash = df.join(wash_trades, df.transaction_hash == wash_trades.wash_tx_hash, "left_outer") \
                     .withColumn("is_wash", F.col("wash_tx_hash").isNotNull())

    ranked = (
        df_with_wash
        .withColumn("is_dex_swap",
            F.col("from_dex").isNotNull() | F.col("to_dex").isNotNull())
        .withColumn("is_mint",
            F.col("from_address").isin(NULL_ADDRESSES))
        .withColumn("is_burn",
            F.col("to_address").isin(NULL_ADDRESSES))
        .withColumn("is_pure_transfer",
            ~F.col("is_mint") & ~F.col("is_burn") & ~F.col("is_wash")) # 워시도 제외
        .groupBy("token_address", "symbol", "dt")
        .agg(
            F.count("transaction_hash").alias("total_events"),
            F.round(F.sum("value_normalized"), 4).alias("total_volume"),
            
            # 🫧 워시트레이딩 통계
            F.sum(F.col("is_wash").cast("int")).alias("wash_count"),
            F.round(F.sum(F.when(F.col("is_wash"), F.col("value_normalized")).otherwise(0)), 4).alias("wash_volume"),

            # 🌱 유기적 거래량 (Organic)
            F.round(F.sum(F.when(F.col("is_pure_transfer"), F.col("value_normalized")).otherwise(0)), 4).alias("organic_volume"),
            F.sum(F.col("is_pure_transfer").cast("int")).alias("organic_count"),

            # 👥 개인 활성 주소 (기관 제외)
            F.countDistinct(F.when(F.col("from_category") == "Unknown", F.col("from_address"))).alias("individual_senders"),
            F.countDistinct(F.when(F.col("to_category") == "Unknown", F.col("to_address"))).alias("individual_receivers"),
            
            # DEX
            F.sum(F.col("is_dex_swap").cast("int")).alias("dex_swap_count"),
            F.round(F.sum(F.when(F.col("is_dex_swap"), F.col("value_normalized")).otherwise(0)), 4).alias("dex_swap_volume"),
        )
        .withColumn("organic_score",
            (F.log1p(F.col("organic_volume")) * 0.5 + 
             F.log1p(F.col("organic_count")) * 0.3 + 
             F.log1p(F.col("individual_senders") + F.col("individual_receivers")) * 0.2
            ).cast(DoubleType()))
        .orderBy(F.col("organic_score").desc())
        .limit(top_n)
    )

    w_rank = Window.orderBy(F.col("organic_score").desc())
    return ranked.withColumn("rank", F.row_number().over(w_rank))


# ─────────────────────────────────────────────────────────────
# Step 2. DEX 스왑 패턴 분석
# ─────────────────────────────────────────────────────────────
def analyze_dex_flow(df: DataFrame):
    """
    from_dex / to_dex 컬럼 활용
    silver token_flow 에 이미 DEX 이름이 담겨있으므로 바로 집계 가능
    """
    swaps = df.filter(
        F.col("from_dex").isNotNull() | F.col("to_dex").isNotNull()
    )
 
    # DEX 이름 통합 (from_dex 또는 to_dex 중 하나)
    swaps = swaps.withColumn("dex_name",
        F.coalesce("from_dex", "to_dex"))
 
    # ── DEX별 집계 ───────────────────────────────────────────
    protocol_stats = (
        swaps
        .groupBy("dex_name", "dt")
        .agg(
            F.count("transaction_hash").alias("swap_events"),
            F.countDistinct("transaction_hash").alias("swap_txs"),
            F.round(F.sum("value_normalized"), 2).alias("total_volume"),
            F.countDistinct("token_address").alias("unique_tokens"),
            F.countDistinct("from_address").alias("unique_traders"),
            F.round(F.avg("value_normalized"), 4).alias("avg_swap_size"),
        )
        .orderBy(F.col("swap_txs").desc())
    )
 
    # ── 토큰 × DEX 조합 집계 ─────────────────────────────────
    token_dex_stats = (
        swaps
        .groupBy("dex_name", "token_address", "symbol", "dt")
        .agg(
            F.count("transaction_hash").alias("swap_count"),
            F.round(F.sum("value_normalized"), 4).alias("volume"),
            F.round(F.avg("value_normalized"), 4).alias("avg_size"),
        )
        .orderBy(F.col("swap_count").desc())
    )
 
    # ── 시간대별 DEX 활동 ─────────────────────────────────────
    hourly_dex = (
        swaps
        .withColumn("hour", F.hour(F.from_unixtime("block_timestamp")))
        .groupBy("hour", "dex_name", "dt")
        .agg(
            F.count("transaction_hash").alias("swap_count"),
            F.round(F.sum("value_normalized"), 4).alias("volume"),
        )
        .orderBy("dt", "hour", F.col("swap_count").desc())
    )
 
    return protocol_stats, token_dex_stats, hourly_dex
 
 
# ─────────────────────────────────────────────────────────────
# Step 3. 신규 홀더 유입 탐지
# ─────────────────────────────────────────────────────────────
def detect_new_holders(df: DataFrame) -> DataFrame:
    """
    당일 처음으로 특정 토큰을 수신한 주소를 신규 홀더로 정의
 
    silver token_flow 에 flag_new_holder 가 없으므로
    to_address 기준 첫 번째 수신 이벤트(row_number=1)로 직접 계산
    """
    w_first = (
        Window
        .partitionBy("to_address", "token_address")
        .orderBy("block_timestamp")
    )
 
    first_recv = (
        df
        .filter(~F.col("to_address").isin(NULL_ADDRESSES))
        .withColumn("recv_rank", F.row_number().over(w_first))
        .filter(F.col("recv_rank") == 1)   # 첫 수신 이벤트만
        .withColumn("is_new_holder", F.lit(True))
    )
 
    # 토큰별 신규 홀더 집계
    new_holder_summary = (
        first_recv
        .groupBy("token_address", "symbol", "dt")
        .agg(
            F.countDistinct("to_address").alias("new_holders_today"),
            F.round(F.avg("value_normalized"), 4).alias("avg_first_recv"),
            F.round(F.sum("value_normalized"), 4).alias("total_first_recv"),
            F.round(F.max("value_normalized"), 4).alias("max_first_recv"),
        )
        .orderBy(F.col("new_holders_today").desc())
    )
 
    return new_holder_summary
 
 
# ─────────────────────────────────────────────────────────────
# Step 4. 워시트레이딩 탐지 (A→B→A)
# ─────────────────────────────────────────────────────────────
def detect_wash_trading(df: DataFrame) -> DataFrame:
    """
    동일 토큰, 동일 주소 쌍에서 A→B 와 B→A 가 모두 존재하고
    금액이 ±20% 이내인 경우 워시트레이딩 의심
 
    token_flow 의 transaction_hash 가 다른 두 거래에서 발생해야 함
    (같은 tx 내 왕복은 단순 DEX 스왑으로 볼 수 있음)
    """
    # 송신 방향
    sent_df = (
        df
        .filter(~F.col("from_address").isin(NULL_ADDRESSES))
        .filter(~F.col("to_address").isin(NULL_ADDRESSES))
        .select(
            F.col("token_address"),
            F.col("symbol"),
            F.col("from_address").alias("addr_a"),
            F.col("to_address").alias("addr_b"),
            F.col("value_normalized").alias("sent_val"),
            F.col("transaction_hash").alias("tx_a"),
            F.col("block_timestamp").alias("ts_a"),
        )
    )
 
    # 역방향
    recv_df = (
        df
        .filter(~F.col("from_address").isin(NULL_ADDRESSES))
        .filter(~F.col("to_address").isin(NULL_ADDRESSES))
        .select(
            F.col("token_address"),
            F.col("to_address").alias("addr_a"),     # 역방향이므로 to = A
            F.col("from_address").alias("addr_b"),   # from = B
            F.col("value_normalized").alias("recv_val"),
            F.col("transaction_hash").alias("tx_b"),
            F.col("block_timestamp").alias("ts_b"),
        )
    )
 
    wash = (
        sent_df.join(recv_df,
                     on=["token_address", "addr_a", "addr_b"],
                     how="inner")
        # 다른 tx 여야 함
        .filter(F.col("tx_a") != F.col("tx_b"))
        # 금액 유사 ±20%
        .withColumn("amount_ratio",
            F.when(F.col("recv_val") > 0,
                   F.col("sent_val") / F.col("recv_val"))
             .otherwise(F.lit(0.0)))
        .filter(
            (F.col("amount_ratio") >= 0.8) &
            (F.col("amount_ratio") <= 1.2)
        )
        .groupBy("token_address", "symbol", "addr_a", "addr_b")
        .agg(
            F.count("tx_a").alias("wash_cycle_count"),
            F.round(F.sum("sent_val"), 4).alias("wash_volume"),
            F.round(F.avg("amount_ratio"), 3).alias("avg_ratio"),
            F.min("ts_a").alias("first_ts"),
            F.max("ts_b").alias("last_ts"),
        )
        .orderBy(F.col("wash_cycle_count").desc())
    )
 
    return wash
 
 
# ─────────────────────────────────────────────────────────────
# Step 5. 시간대별 히트맵 원본
# ─────────────────────────────────────────────────────────────
def build_hourly_heatmap(df: DataFrame, token_ranking: DataFrame) -> DataFrame:
    """
    Top-N 토큰 × 시간대(0~23) 거래량 히트맵
    Looker Studio 피벗 테이블 / BigQuery BI Engine 용
    """
    top_tokens = [
        row["token_address"]
        for row in token_ranking.select("token_address").collect()
    ]
 
    return (
        df
        .filter(F.col("token_address").isin(top_tokens))
        .withColumn("hour", F.hour(F.from_unixtime("block_timestamp")))
        .groupBy("hour", "token_address", "symbol", "dt")
        .agg(
            F.count("transaction_hash").alias("tx_count"),
            F.round(F.sum("value_normalized"), 4).alias("volume"),
            F.round(F.avg("value_normalized"), 6).alias("avg_size"),
            F.countDistinct("from_address").alias("active_senders"),
            F.sum(
                (F.col("from_dex").isNotNull() | F.col("to_dex").isNotNull()).cast("int")
            ).alias("dex_events"),
        )
        .orderBy("dt", "hour", F.col("volume").desc())
    )
 
 
# ─────────────────────────────────────────────────────────────
# 요약 출력
# ─────────────────────────────────────────────────────────────
def run_summary(token_ranking: DataFrame, protocol_stats: DataFrame, new_holders: DataFrame, wash: DataFrame):
    print(f"\n{'═'*65}")
    print(f"  💰 Gold token_ranking 요약")
    print(f"{'═'*65}")
 
    print(f"\n  ── [ALL] Top-20 토큰 랭킹 (Organic Score 기준) ──")
    token_ranking.select(
        "rank", "symbol",
        F.round("organic_volume", 2).alias("organic_vol"),
        "organic_count",
        F.col("individual_senders").alias("holders"),
        F.round("wash_volume", 2).alias("wash_vol"),
        F.round("organic_score", 1).alias("score"),
    ).show(20, truncate=False)

    print(f"\n  ── [Altcoin Only] Top-20 (스테이블코인 제외) ──")
    token_ranking.filter(~F.col("token_address").isin(STABLECOINS)).select(
        "rank", "symbol",
        F.round("organic_volume", 2).alias("organic_vol"),
        "organic_count",
        F.col("individual_senders").alias("holders"),
        F.round("organic_score", 1).alias("score"),
    ).show(20, truncate=False)
 
    print(f"\n  ── DEX 프로토콜별 집계 ──")
    protocol_stats.show(10, truncate=False)
 
    print(f"  ── 신규 홀더 유입 Top-10 ──")
    new_holders.show(10, truncate=False)
 
    print(f"  ── 워시트레이딩 의심 Top-10 ──")
    wash.show(10, truncate=False)
 
 
# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Gold Layer: 인기 ERC-20 토큰 흐름 분석")
    parser.add_argument("--date",        required=True)
    parser.add_argument("--top-tokens",  type=int, default=30, help="Top-N 토큰 수 (기본: 30)")
    parser.add_argument("--summary",     action="store_true")
    args = parser.parse_args()
 
    logger = get_logger("GoldTokenRanking")
    spark = get_spark_session("GoldTokenRanking")
    spark.sparkContext.setLogLevel("WARN")
 
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"
    start = time.time()
 
    logger.info(f"💰 Gold token_ranking 시작: {dt_val}  top_tokens={args.top_tokens}")
 
    # ── 로드 ──────────────────────────────────────────────────
    df = spark.read.parquet(silver_path("token_flow", dt_val))
    df.cache()
    logger.info(f"token_flow 로드 완료: {df.count():,}건")
 
    # ── 분석 ──────────────────────────────────────────────────
    logger.info("[1/5] 토큰 랭킹 집계...")
    token_ranking = build_token_ranking(df, args.top_tokens)
    token_ranking.cache()
 
    logger.info("[2/5] DEX 스왑 패턴 분석...")
    protocol_stats, token_dex_stats, hourly_dex = analyze_dex_flow(df)
 
    logger.info("[3/5] 신규 홀더 탐지...")
    new_holders = detect_new_holders(df)
 
    logger.info("[4/5] 워시트레이딩 탐지...")
    wash = detect_wash_trading(df)
 
    logger.info("[5/5] 시간대별 히트맵 원본 생성...")
    heatmap = build_hourly_heatmap(df, token_ranking)
 
    # ── 요약 ──────────────────────────────────────────────────
    if args.summary:
        run_summary(token_ranking, protocol_stats, new_holders, wash)
 
    # ── 저장 ──────────────────────────────────────────────────
    logger.info("💾 Gold 레이어 저장 중...")
    write_gold(token_ranking,    "token_ranking",    ["dt"])
    write_gold(protocol_stats,   "dex_protocol",     ["dt"])
    write_gold(token_dex_stats,  "dex_token_stats",  ["dt"])
    write_gold(hourly_dex,       "dex_hourly",        ["dt"])
    write_gold(new_holders,      "new_holders",       ["dt"])
    write_gold(wash,             "wash_trade",        None)
    write_gold(heatmap,          "hourly_heatmap",    ["dt"])
 
    df.unpersist()
    token_ranking.unpersist()
    spark.stop()
 
    elapsed = time.time() - start
    logger.info(f"✅ 완료: {int(elapsed//60)}분 {elapsed%60:.1f}초")
    logger.info(f"📂 저장 위치: gs://{BUCKET_NAME}/{GCS_GOLD_PREFIX}/token_ranking/")
 
 
if __name__ == "__main__":
    """
    uv run src/gold/token_ranking.py --date 2026-05-01 --top-tokens 30 --summary
    uv run src/gold/token_ranking.py --date 2026-05-01
    """
    main()
