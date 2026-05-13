from pyspark.sql import functions as F
from pyspark.sql import Window, SparkSession, DataFrame
from src.silver.spark_config import read_silver, get_logger, get_spark_session
from src.silver.known_labels import load_label_df
from src.silver.utils import write_silver
from src.schema.silver_schema import enriched_transaction_schema
from src.config import BUCKET_NAME, GCS_SILVER_PREFIX

def build_whale_txns(spark: SparkSession, dt: str, threshold_eth: float) -> DataFrame:
	"""
	고래 트랜잭션 뷰 생성
	----------------
	1. txn_enriched에서 threshold 이상 필터
	2. from/to 각각 라벨 조인
	3. 주소별 당일 누적 통계 (window)
	4. 이상 패턴 플래그
	"""
	logger = get_logger("Build Whale txns")

	txns = read_silver(spark, "txn_enriched", dt, schema=enriched_transaction_schema)
	
	# 특정 파티션 경로를 직접 읽을 경우 dt 컬럼이 null이 되므로, 인자로 받은 값을 채워줌
	dt_val = dt.split('=')[-1]
	txns = txns.withColumn("dt", F.lit(dt_val).cast("date"))
	
	logger.info(f"[build_whale_txns] - [{dt}] txn_enriched 데이터 로드 완료")


	whales = (
		txns
		.filter(F.col("value_eth") >= threshold_eth)
		.filter(F.col("is_success") == True)
	)

	labels = load_label_df(spark)
	labels_bc = F.broadcast(labels)

	whales = (
		whales.join(
			labels_bc.select(
				F.col("address").alias("from_address"),
				F.col("label_name").alias("from_label"),
				F.col("label_category").alias("from_category")
			),
			on="from_address", how="left"
		).join(
			labels_bc.select(
				F.col("address").alias("to_address"),
				F.col("label_name").alias("to_label"),
				F.col("label_category").alias("to_category"),
			),
			on="to_address", how="left"
		)
	)

	# 라벨 없으면 "Unkown" 처리
	whales = (
		whales
		.withColumn("from_label",    F.coalesce("from_label",    F.lit("Unknown")))
        .withColumn("from_category", F.coalesce("from_category", F.lit("Unknown")))
        .withColumn("to_label",      F.coalesce("to_label",      F.lit("Unknown")))
        .withColumn("to_category",   F.coalesce("to_category",   F.lit("Unknown")))
    )

	# 주소별 당일 누적 통계
	w_from = (
		Window
		.partitionBy("from_address", "dt")
		.orderBy("block_timestamp")
		.rowsBetween(Window.unboundedPreceding, Window.currentRow)
	)

	w_to = (
		Window
		.partitionBy("to_address", "dt")
		.orderBy("block_timestamp")
		.rowsBetween(Window.unboundedPreceding, Window.currentRow)
	)

	whales = (
		whales
		# 보낸 주소 기준 누적
        .withColumn("from_cumul_sent_eth",  F.sum("value_eth").over(w_from))
        .withColumn("from_cumul_tx_count",  F.count("hash").over(w_from))
        # 받은 주소 기준 누적
        .withColumn("to_cumul_recv_eth",    F.sum("value_eth").over(w_to))
        .withColumn("to_cumul_tx_count",    F.count("hash").over(w_to))
	)

	# 이상 패턴 플래그
    # 4-a. 거래소 유입 (고래 → CEX)
	whales = whales.withColumn(
        "flag_cex_deposit",
        (F.col("to_category") == "CEX") & (F.col("from_category") == "Unknown")
    )
 
    # 4-b. 거래소 출금 (CEX → 고래)
	whales = whales.withColumn(
        "flag_cex_withdrawal",
        (F.col("from_category") == "CEX") & (F.col("to_category") == "Unknown")
    )
 
    # 4-c. CEX 간 이동 (거래소 → 거래소) : 자금 세탁 / 거래소 내부 이동
	whales = whales.withColumn(
        "flag_cex_to_cex",
        (F.col("from_category") == "CEX") & (F.col("to_category") == "CEX")
    )
 
    # 4-d. DEX 대규모 스왑
	whales = whales.withColumn(
        "flag_dex_swap",
        (F.col("to_category") == "DEX") | (F.col("from_category") == "DEX")
    )
 
    # 4-e. 단기 반복 대량 송금 (당일 같은 주소에서 5회 이상 & 총 500 ETH 이상)
	whales = whales.withColumn(
        "flag_high_freq_sender",
        (F.col("from_cumul_tx_count") >= 5) & (F.col("from_cumul_sent_eth") >= 500)
    )
 
    # 4-f. 전체 플래그 요약 (OR 조합)
	flag_cols = [
        "flag_cex_deposit", "flag_cex_withdrawal", "flag_cex_to_cex",
        "flag_dex_swap", "flag_high_freq_sender",
    ]
	whales = whales.withColumn(
        "has_flag",
        F.greatest(*[F.col(c).cast("int") for c in flag_cols]) == 1
    )

	final_cols = [
        # 식별
        "hash", "block_number", "block_timestamp", "dt",
        # 주소 + 라벨
        "from_address", "from_label", "from_category",
        "to_address",   "to_label",   "to_category",
        # 금액 및 수수료 (txn_enriched 컬럼명 준수)
        "value_eth", "tx_fee_eth",
        # 누적 통계
        "from_cumul_sent_eth", "from_cumul_tx_count",
        "to_cumul_recv_eth",   "to_cumul_tx_count",
        # 플래그
        *flag_cols, "has_flag"
    ]

	return whales.select(*final_cols)


def run_summary(df: DataFrame):
	"""콘솔 요약 출력 (--Summary 옵션 시 실행)"""
	total = df.count()
	flagged = df.filter(F.col("has_flag")).count()

	print(f"\n{'═'*45}")
	print(f"  🐋  Whale Txns 요약")
	print(f"{'═'*45}")
	print(f"  총 고래 트랜잭션    : {total:>8,}")
	print(f"  플래그 있는 거래     : {flagged:>8,}  ({flagged/total*100:.1f}%)")
 
	print(f"\n  ── Top-10 대규모 거래 ──")
	df.select("hash", "from_address", "to_address", "value_eth",
		   "from_label", "to_label") \
		   .orderBy(F.col("value_eth").desc()) \
		   .show(10, truncate=False)
	

	print(f"  ── 거래소 유입 Top-5 주소 ──")
	df.filter(F.col("flag_cex_deposit")) \
      .groupBy("from_address", "to_label") \
      .agg(
          F.count("hash").alias("tx_count"),
          F.round(F.sum("value_eth"), 2).alias("total_eth"),
      ) \
      .orderBy(F.col("total_eth").desc()) \
      .show(5, truncate=False)
 
	print(f"  ── 플래그 유형별 건수 ──")
	flag_cols = [
        "flag_cex_deposit", "flag_cex_withdrawal", "flag_cex_to_cex",
        "flag_dex_swap", "flag_high_freq_sender",
    ]
	for col in flag_cols:
		cnt = df.filter(F.col(col)).count()
		print(f"  {col:<30}: {cnt:,}")


# 이거는 나중에 utils에 따로 빼야겠다.


def main():
	import argparse
	import time
	logger = get_logger("Main")

	parser = argparse.ArgumentParser(description="Ethereum Silver Layer: whale_txn 빌드")
	parser.add_argument("--date", required=True, help="처리할 날짜 (YYYY-MM-DD 또는 dt=YYYY-MM-DD)")
	parser.add_argument("--whale-threshold", type=float, default=100.0,help="고래 기준 ETH 금액 (기본값: 100)")
	parser.add_argument("--summary", action="store_true", help="고래 결과 요약 실행 여부")
	args = parser.parse_args()

    # 경로 고정 (config의 설정값 활용)
	spark = get_spark_session("Main")
	spark.sparkContext.setLogLevel("WARN")
	silver_path = f"gs://{BUCKET_NAME}/{GCS_SILVER_PREFIX}"
	dt_val = args.date if "dt=" in args.date else f"dt={args.date}"
    
	start_time = time.time()

	logger.info(f"\n🐋 Silver whale_txns 빌드 시작: {args.date}  (기준: ≥{args.whale_threshold} ETH)")
	logger.info(f"📂 Silver 저장 경로: {silver_path}")

    # 1. 데이터 생성
	df = build_whale_txns(spark, dt_val, args.whale_threshold)
    
    # 캐싱 적용 (품질 체크와 저장 시 데이터를 재사용하기 위함)
	df.cache()
	logger.info("⚡ DataFrame 캐싱 완료 (성능 최적화)")

    # 2. 요약 체크
	if args.summary:
		run_summary(df)

    # 3. 저장
	write_silver(df, "whale_txns")

    # 4. 리소스 해제
	df.unpersist() # 캐시 해제
	spark.stop()
    
	end_time = time.time()
	duration = end_time - start_time
	logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
	logger.info("👋 Spark 세션 종료 및 리소스 해제 완료")


if __name__ == "__main__":
	"""
	# 요약도 실행
	uv run src/silver/transform/whale_txns.py --date 2026-05-01 --summary --whale-threshold 200.0

	# 저장만 실행
	uv run src/silver/transform/whale_txns.py --date 2026-05-01 --whale-threshold 200.0
	"""
	main()
