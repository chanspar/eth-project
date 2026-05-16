from pyspark.sql import SparkSession
from src.silver.spark_config import WEI_PER_ETH, read_bronze, get_logger, get_spark_session
from src.silver.utils import write_silver
from src.schema.bronze_schema import transaction_schema, receipt_schema
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType


def build_txn_enriched(spark: SparkSession, dt: str):
	"""
	transactions + receipts 조인 뷰
	"""

	logger = get_logger("Build Txn Enriched")

	txns = read_bronze(spark, "transactions", dt, schema=transaction_schema)
	receipts = read_bronze(spark, "receipts", dt, schema=receipt_schema)

	logger.info(f"[build_txn_enriched] - [{dt}] 트랜잭션, 영수증 데이터 로드 완료")

	receipts_slim = receipts.select(
        F.col("transaction_hash"),
        F.col("status")
    )

	enriched = (
        txns
        .join(receipts_slim, txns["hash"] == receipts_slim["transaction_hash"], how="left")
        .drop("transaction_hash")
    )

	enriched = (
		enriched.withColumn(
			"value_eth",
			(F.col("value").cast(DecimalType(38, 0)) / F.lit(WEI_PER_ETH)).cast(DecimalType(38, 18))
		).withColumn(
            "is_success",
            F.col("status") == 1
        ).withColumn(
            "dt",
            F.to_date(F.from_unixtime(F.col("block_timestamp")))
        )
	)

	final_cols = [
			"hash", "block_number", "block_timestamp",
			"from_address", "to_address",
			"value_eth", "is_success", "dt"
	]

	logger.info(f"[build_txn_enriched] - [{dt}] 파생 컬럼 연산 완료")
	return enriched.select(*final_cols)


def main():
    import argparse
    import time
    logger = get_logger("Main")
    
    parser = argparse.ArgumentParser(description="Ethereum Silver Layer: txn_enriched 빌드")
    parser.add_argument("--date", required=True, help="처리할 날짜 (YYYY-MM-DD 또는 dt=YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session("Main")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"
    
    start_time = time.time()

    logger.info(f"🔧 Silver txn_enriched 빌드 프로세스 시작: {dt_val}")

    # 1. 데이터 생성
    df = build_txn_enriched(spark, dt_val)
    
    # 2. 저장
    write_silver(df, "txn_enriched")

    # 3. 리소스 해제
    spark.stop()
    
    end_time = time.time()
    duration = end_time - start_time
    logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
    logger.info("👋 Spark 세션 종료 및 리소스 해제 완료")


if __name__ == "__main__":
    """uv run python src/silver/transform/txn_enriched.py --date 2026-05-01"""
    main()
