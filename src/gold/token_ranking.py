from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from src.config import GCS_GOLD_PREFIX, BUCKET_NAME
from src.gold.utils import write_gold, silver_path
from pyspark.sql.types import DoubleType, StringType
from src.silver.spark_config import get_spark_session, get_logger
from src.silver.known_labels import TOKEN_META, KNOWN_LABELS
import argparse
import time

# Known Labels로부터 주소 목록 생성
NULL_ADDRESSES = [addr for addr, (name, cat) in KNOWN_LABELS.items() if cat == "Burn"]

# 스테이블코인 목록 (TOKEN_META에서 심볼 기준으로 필터링)
_STABLE_SYMBOLS = {"USDT", "USDC", "DAI", "FRAX", "TUSD", "PYUSD", "USDe", "BUSD", "EURCV"}
STABLECOINS = [addr for addr, (symbol, name, dec) in TOKEN_META.items() if symbol in _STABLE_SYMBOLS]


# ─────────────────────────────────────────────────────────────
# Step 1. 토큰별 거래량 종합 랭킹
# ─────────────────────────────────────────────────────────────
def build_token_ranking(df: DataFrame, top_n: int) -> DataFrame:
    """
    ERC-20 토큰별 당일 종합 지표 (유기적 성장 위주)
    """
    # 1. 워시 트레이딩(자전거래) 감지 로직
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
        F.when(F.col("b.value_normalized") > 0, 
               F.col("a.value_normalized") / F.col("b.value_normalized"))
         .otherwise(F.lit(0.0))
         .between(0.8, 1.2)
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
            ~F.col("is_mint") & ~F.col("is_burn") & ~F.col("is_wash"))
        .groupBy("token_address", "symbol", "dt")
        .agg(
            F.count("transaction_hash").alias("total_events"),
            F.round(F.sum("value_normalized"), 4).alias("total_volume"),
            
            # 워시트레이딩 통계
            F.sum(F.col("is_wash").cast("int")).alias("wash_count"),
            F.round(F.sum(F.when(F.col("is_wash"), F.col("value_normalized")).otherwise(0)), 4).alias("wash_volume"),

            # 유기적 거래량 (Organic)
            F.round(F.sum(F.when(F.col("is_pure_transfer"), F.col("value_normalized")).otherwise(0)), 4).alias("organic_volume"),
            F.sum(F.col("is_pure_transfer").cast("int")).alias("organic_count"),

            # 개인 활성 주소 (기관 제외)
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
    DEX별 집계 및 토큰별 DEX 활용 분석
    """
    swaps = df.filter(
        F.col("from_dex").isNotNull() | F.col("to_dex").isNotNull()
    )
 
    # DEX 이름 통합
    swaps = swaps.withColumn("dex_name", F.coalesce("from_dex", "to_dex"))
 
    # DEX별 집계
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
 
    # 토큰 × DEX 조합 집계
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
 
    # 시간대별 DEX 활동
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
    당일 특정 토큰을 처음 수신한 활성 주소 집계
    """
    # is_new_holder 플래그가 있으면 사용, 없으면 전체 수신자 집계
    has_flag = "is_new_holder" in df.columns
    filtered_df = df.filter(F.col("is_new_holder") == True) if has_flag else df

    return (
        filtered_df
        .groupBy("token_address", "symbol", "dt")
        .agg(
            F.countDistinct("to_address").alias("new_holder_count"),
            F.round(F.sum("value_normalized"), 4).alias("new_holder_volume")
        )
        .orderBy(F.col("new_holder_count").desc())
    )


# ─────────────────────────────────────────────────────────────
# Step 4. 워시트레이딩 상세 탐지
# ─────────────────────────────────────────────────────────────
def detect_wash_trading(df: DataFrame) -> DataFrame:
    """
    A -> B -> A 형태의 순환 거래(Wash Trading) 상세 탐지
    """
    sent_df = (
        df.filter(~F.col("from_address").isin(NULL_ADDRESSES) & ~F.col("to_address").isin(NULL_ADDRESSES))
        .select("token_address", "symbol", F.col("from_address").alias("addr_a"), F.col("to_address").alias("addr_b"),
                F.col("value_normalized").alias("sent_val"), F.col("transaction_hash").alias("tx_a"),
                F.col("block_timestamp").alias("ts_a"), "dt")
    )

    recv_df = (
        df.filter(~F.col("from_address").isin(NULL_ADDRESSES) & ~F.col("to_address").isin(NULL_ADDRESSES))
        .select("token_address", F.col("to_address").alias("addr_a"), F.col("from_address").alias("addr_b"),
                F.col("value_normalized").alias("recv_val"), F.col("transaction_hash").alias("tx_b"),
                F.col("block_timestamp").alias("ts_b"), "dt")
    )

    wash = (
        sent_df.join(recv_df, on=["token_address", "addr_a", "addr_b", "dt"], how="inner")
        .filter(F.col("tx_a") != F.col("tx_b"))
        .withColumn("amount_ratio", F.when(F.col("recv_val") > 0, F.col("sent_val") / F.col("recv_val")).otherwise(0.0))
        .filter(F.col("amount_ratio").between(0.8, 1.2))
        .groupBy("token_address", "symbol", "addr_a", "addr_b", "dt")
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
# Step 5. 시간대별 히트맵
# ─────────────────────────────────────────────────────────────
def build_hourly_heatmap(df: DataFrame, token_ranking: DataFrame) -> DataFrame:
    """
    Top-N 토큰 × 시간대별 거래량 히트맵
    """
    top_tokens = [row["token_address"] for row in token_ranking.select("token_address").collect()]
 
    return (
        df.filter(F.col("token_address").isin(top_tokens))
        .withColumn("hour", F.hour(F.from_unixtime("block_timestamp")))
        .groupBy("hour", "token_address", "symbol", "dt")
        .agg(
            F.count("transaction_hash").alias("tx_count"),
            F.round(F.sum("value_normalized"), 4).alias("volume"),
            F.round(F.avg("value_normalized"), 6).alias("avg_size"),
            F.countDistinct("from_address").alias("active_senders"),
            F.sum((F.col("from_dex").isNotNull() | F.col("to_dex").isNotNull()).cast("int")).alias("dex_events"),
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
        "rank", "symbol", "token_address",
        F.round("organic_volume", 2).alias("organic_vol"),
        "organic_count",
        F.col("individual_senders").alias("holders"),
        F.round("wash_volume", 2).alias("wash_vol"),
        F.round("organic_score", 1).alias("score"),
    ).show(20, truncate=False)

    print(f"\n  ── [Altcoin Only] Top-20 (스테이블코인 제외) ──")
    token_ranking.filter(~F.col("token_address").isin(STABLECOINS)).select(
        "rank", "symbol", "token_address",
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
 
 
def main():
    parser = argparse.ArgumentParser(description="Gold Layer: 인기 ERC-20 토큰 흐름 분석")
    parser.add_argument("--date",        required=True)
    parser.add_argument("--top-tokens",  type=int, default=30, help="Top-N 토큰 수 (기본: 30)")
    parser.add_argument("--summary",     action="store_true")
    args = parser.parse_args()
 
    logger = get_logger("GoldTokenRanking")
    spark = get_spark_session("GoldTokenRanking")
    spark.sparkContext.setLogLevel("WARN")
 
    date_only = args.date.replace("dt=", "")
    dt_val = f"dt={date_only}"
    start = time.time()
 
    logger.info(f"💰 Gold token_ranking 시작: {dt_val}  top_tokens={args.top_tokens}")
 
    df = spark.read.parquet(silver_path("token_flow", dt_val))
    df = df.withColumn("dt", F.lit(date_only))
    df.cache()
    logger.info(f"token_flow 로드 완료: {df.count():,}건")
 
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
 
    if args.summary:
        run_summary(token_ranking, protocol_stats, new_holders, wash)
 
    logger.info("💾 Gold 레이어 저장 중...")
    write_gold(token_ranking,    "token_ranking",    ["dt"])
    write_gold(protocol_stats,   "dex_protocol",     ["dt"])
    write_gold(token_dex_stats,  "dex_token_stats",  ["dt"])
    write_gold(hourly_dex,       "dex_hourly",        ["dt"])
    write_gold(new_holders,      "new_holders",       ["dt"])
    write_gold(wash,             "wash_trade",        ["dt"])
    write_gold(heatmap,          "hourly_heatmap",    ["dt"])
 
    df.unpersist()
    token_ranking.unpersist()
    spark.stop()
 
    elapsed = time.time() - start
    logger.info(f"✅ 완료: {int(elapsed//60)}분 {elapsed%60:.1f}초")
 
if __name__ == "__main__":
    main()
