from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from src.silver.spark_config import read_bronze, get_spark_session, get_logger
from src.silver.utils import write_silver
from src.schema.bronze_schema import token_transfer_schema, contract_schema, block_schema
from src.silver.known_labels import load_token_meta_df, load_dex_df
from src.config import BUCKET_NAME, GCS_SILVER_PREFIX

def build_token_flow(spark: SparkSession, dt: str):
    logger = get_logger("Build Token Flow")

    logger.info(f"[build_token_flow] - [{dt}] 데이터 로드")

    transfers = read_bronze(spark, "token_transfers", dt, token_transfer_schema)
    contracts = read_bronze(spark, "contracts", dt, contract_schema)
    blocks = read_bronze(spark, "blocks", dt, block_schema)
    
    logger.info(f"[build_token_flow] - [{dt}] 데이터 로드 완료")

    # ERC-20 필터
    erc20_contracts = contracts.filter(F.col("is_erc20") == True).select(
        F.col("address").alias("token_address")
    )
    
    blocks_slim = blocks.select(
        F.col("number").alias("block_number"),
        F.col("timestamp").alias("block_timestamp")
    )

    # transfers 중 ERC-20 컨트랙트인 것만 남김 (Inner Join 효과)
    flow = (
        transfers
        .join(F.broadcast(erc20_contracts), on="token_address", how="inner")
    )
    
    flow = (
        flow.join(F.broadcast(blocks_slim), on="block_number", how="left")
    )
    
    # 날짜 파티션용 컬럼
    if "dt" not in flow.columns:
        flow = flow.withColumn("dt", F.to_date(F.from_unixtime(F.col("block_timestamp"))))
        
    # ── 3. 토큰 메타 및 정규화 금액 계산 ──
    meta_df = load_token_meta_df(spark)
    flow = flow.join(F.broadcast(meta_df), on="token_address", how="left")
    
    flow = (
        flow
        .withColumn("symbol", F.coalesce("symbol", F.lit("UNKNOWN")))
        .withColumn("decimals", F.coalesce("decimals", F.lit(18)))
        .withColumn(
            "value_normalized",
            (F.col("value") / F.pow(F.lit(10.0), F.col("decimals").cast(DoubleType()))).cast(DoubleType())
        )
    )

    # ── 4. DEX 라벨링 (송/수신처가 거래소인지 파악하는 것은 고래 추적에 매우 중요) ──
    dex_df = load_dex_df(spark)
    
    flow = flow.join(
        F.broadcast(dex_df.select(F.col("address").alias("from_address"), F.col("dex_name").alias("from_dex"))), 
        on="from_address", how="left"
    )
    flow = flow.join(
        F.broadcast(dex_df.select(F.col("address").alias("to_address"), F.col("dex_name").alias("to_dex"))), 
        on="to_address", how="left"
    )

    # ── 최종 컬럼 선택 ──
    final_cols = [
        "transaction_hash", "block_timestamp", "dt",
        "token_address", "symbol",
        "from_address", "to_address",
        "value_normalized", 
        "from_dex", "to_dex" # 향후 고래가 DEX로 던졌는지(매도) 파악용
    ]

    return flow.select(*final_cols)


def run_summary(df):
    """--summary 옵션 시 콘솔 출력 (고래 추적 맞춤형)"""
    total = df.count()
    
    # DEX 유입/유출 카운트 (고래의 매수/매도 동향 파악)
    dex_in_cnt = df.filter(F.col("to_dex").isNotNull()).count()
    dex_out_cnt = df.filter(F.col("from_dex").isNotNull()).count()

    print(f"\n{'═'*50}")
    print(f"  🐋  Whale Flow 실버 레이어 요약")
    print(f"{'═'*50}")
    print(f"  총 ERC-20 전송 이벤트  : {total:>10,}")
    if total > 0:
        print(f"  DEX로 유입 (매도 추정) : {dex_in_cnt:>10,}  ({dex_in_cnt/total*100:.1f}%)")
        print(f"  DEX에서 유출 (매수 추정) : {dex_out_cnt:>10,}  ({dex_out_cnt/total*100:.1f}%)")

    print(f"\n  ── Top-10 토큰 전송 활성도 ──")
    df.groupBy("symbol") \
    .agg(
        F.count("transaction_hash").alias("tx_count"),
        F.round(F.sum("value_normalized"), 2).alias("total_volume")
    ) \
    .orderBy(F.col("tx_count").desc()) \
    .show(10, truncate=False)

    print(f"  ── 🚨 고래 움직임 포착 (Top 5 단일 최대 전송) ──")
    df.filter(F.col("symbol") != "UNKNOWN") \
    .select(
        "symbol", 
        F.col("from_address").substr(1, 10).alias("from_short"), 
        F.col("to_address").substr(1, 10).alias("to_short"), 
        F.round("value_normalized", 2).alias("amount"), 
        "to_dex"
    ) \
    .orderBy(F.col("value_normalized").desc()) \
    .show(5, truncate=False)


def main():
    import argparse
    import time

    logger = get_logger("Main")

    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD format")
    parser.add_argument("--summary", action="store_true", help="summary")
    args = parser.parse_args()

    spark = get_spark_session("Main")
    spark.sparkContext.setLogLevel("WARN")
    silver_path = f"gs://{BUCKET_NAME}/{GCS_SILVER_PREFIX}"
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    start_time = time.time()

    logger.info(f"Silver token_flow 빌드 시작: {args.date}")

    df = build_token_flow(spark, dt_val)

    # 데이터 건수 간략히 확인 (선택 사항)
    df.cache()
    count = df.count()
    logger.info(f"처리된 ERC-20 Transfer 건수: {count:,} 건")

    if args.summary:
        run_summary(df)
	
    write_silver(df, silver_path)
    
    df.unpersist() # 캐시 해제
    spark.stop()

    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
    logger.info("👋 Spark 세션 종료 및 리소스 해제 완료")

if __name__ == "__main__":
    """
    # 요약도 실행
    uv run src/silver/transform/token_flow.py --date 2026-05-01 --summary

    # 요약 없이 실행
    uv run src/silver/transform/token_flow.py --date 2026-05-01
    """
    main()
