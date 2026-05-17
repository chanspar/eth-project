from pyspark.sql import functions as F
from pyspark.sql import Window, SparkSession, DataFrame
from src.silver.spark_config import read_silver, get_logger, get_spark_session
from src.silver.known_labels import load_address_labels
from src.silver.utils import write_silver
from src.schema.silver_schema import enriched_transaction_schema


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

	labels = load_address_labels(spark)
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

	# 라벨 없으면 "Unknown" 처리 및 엔티티(Entity) 추출
	whales = (
		whales
		.withColumn("from_label",    F.coalesce("from_label",    F.lit("Unknown")))
        .withColumn("from_category", F.coalesce("from_category", F.lit("Unknown")))
        .withColumn("to_label",      F.coalesce("to_label",      F.lit("Unknown")))
        .withColumn("to_category",   F.coalesce("to_category",   F.lit("Unknown")))
        # 핵심 브랜드명만 추출 (예: Binance 14 -> Binance, Uniswap V3: Router -> Uniswap V3)
        .withColumn("from_entity", F.trim(F.regexp_extract(F.col("from_label"), r"^([^0-9:#]+)", 1)))
        .withColumn("to_entity",   F.trim(F.regexp_extract(F.col("to_label"),   r"^([^0-9:#]+)", 1)))
    )

	# --- 1. 기초 인텔리전스 추출 ---
	whales = (
		whales
		.withColumn("hour", F.hour(F.from_unixtime("block_timestamp").cast("timestamp")))
		# 보낸 쪽/받는 쪽이 모두 Unknown이면 순수 개인 고래 거래로 간주
		.withColumn("is_private_transaction", 
			(F.col("from_category") == "Unknown") & (F.col("to_category") == "Unknown"))
	)

	# --- 2. 고래 체급 분류 (Whale Tier) ---
	whales = whales.withColumn("whale_tier", 
		F.when(F.col("value_eth") >= 10000, "Humpback")
		.when(F.col("value_eth") >= 1000,  "Whale")
		.when(F.col("value_eth") >= 100,   "Shark")
		.otherwise("Crab")
	)

	# --- 3. 자금 흐름 성격 규정 (Flow Type) ---
	whales = whales.withColumn("flow_type",
		F.when((F.col("from_category") == "Unknown") & (F.col("to_category") == "CEX"), "CEX_DEPOSIT")    # 매도 압력
		.when((F.col("from_category") == "CEX") & (F.col("to_category") == "Unknown"), "CEX_WITHDRAWAL") # 매수 압력
		.when((F.col("from_category") == "DEX") | (F.col("to_category") == "DEX"),      "DEX_TRADE")      # 스왑 활동
		.when((F.col("from_category") == "Bridge") | (F.col("to_category") == "Bridge"), "BRIDGE_MOVE")   # 자산 이동
		.when((F.col("from_category") == "Unknown") & (F.col("to_category") == "Unknown"), "PRIVATE_MOVE") # 고래간 이동
		.otherwise("OTHER")
	)

	# --- 4. 누적 통계 및 플래그 (중간 연산용) ---
	w_from = Window.partitionBy("from_address", "dt").orderBy("block_timestamp") \
		.rowsBetween(Window.unboundedPreceding, Window.currentRow)
	
	w_to = Window.partitionBy("to_address", "dt").orderBy("block_timestamp") \
		.rowsBetween(Window.unboundedPreceding, Window.currentRow)
	
	whales = whales \
		.withColumn("cumul_sent_eth", F.sum("value_eth").over(w_from)) \
		.withColumn("cumul_tx_count",  F.count("hash").over(w_from)) \
		.withColumn("cumul_recv_eth", F.sum("value_eth").over(w_to))

	# 고빈도 송금 플래그
	whales = whales.withColumn("flag_high_freq", 
		(F.col("cumul_tx_count") >= 5) & (F.col("cumul_sent_eth") >= 500))

	# --- 5. 최종 컬럼 선택 (골드 레이어 준비물) ---
	final_cols = [
		"hash", "block_timestamp", "hour", "dt",
		"from_address", "from_label", "from_entity", "from_category",
		"to_address",   "to_label",   "to_entity",   "to_category",
		"value_eth",
		"cumul_sent_eth", "cumul_tx_count", "cumul_recv_eth",
		"whale_tier", "flow_type", "is_private_transaction",
		"flag_high_freq"
	]

	return whales.select(*final_cols)


def main():
	import argparse
	import time
	
	logger = get_logger("Main")

	parser = argparse.ArgumentParser(description="Ethereum Silver Layer: whale_txn 빌드")
	parser.add_argument("--date", required=True, help="처리할 날짜 (YYYY-MM-DD 또는 dt=YYYY-MM-DD)")
	parser.add_argument("--whale-threshold", type=float, default=100.0, help="고래 기준 ETH 금액 (기본값: 100)")
	args = parser.parse_args()

	spark = get_spark_session("Main")
	spark.sparkContext.setLogLevel("WARN")
	
	dt_val = args.date if "dt=" in args.date else f"dt={args.date}"
	start_time = time.time()

	logger.info(f"🐋 Silver whale_txns 빌드 시작: {args.date} (기준: ≥{args.whale_threshold} ETH)")

	try:
		# 1. 데이터 생성
		df = build_whale_txns(spark, dt_val, args.whale_threshold)
		
		# [성능 최적화] 중복 연산을 방지하기 위해 데이터프레임을 메모리에 캐싱
		df.cache()

		# 데이터 확인용 (상위 5건 출력)
		print(f"\n📊 [{args.date}] 처리 데이터 샘플 (상위 5건):")
		df.show(5, truncate=False)

		# 2. 저장
		write_silver(df, "whale_txns")
		
		# 메모리 확보를 위해 캐시 해제
		df.unpersist()
		
		logger.info(f"✅ [{args.date}] 고래 트랜잭션 처리 및 저장 완료")

	except Exception as e:
		logger.exception(f"❌ [{args.date}] 처리 중 오류 발생: {e}")
	finally:
		spark.stop()
		duration = time.time() - start_time
		logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
		logger.info("👋 Spark 세션 종료 및 리소스 해제 완료")


if __name__ == "__main__":
	"""
	# 실행 예시
	uv run src/silver/transform/whale_txns.py --date 2026-05-01 --whale-threshold 100.0
	"""
	main()
