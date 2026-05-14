from pyspark.sql import SparkSession
from src.silver.spark_config import WEI_PER_ETH, read_bronze, get_logger, get_spark_session
from src.silver.utils import write_silver
from src.config import BUCKET_NAME, GCS_SILVER_PREFIX
from src.schema.bronze_schema import transaction_schema, receipt_schema, block_schema
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, DoubleType


def build_txn_enriched(spark: SparkSession, dt: str):
	"""
	transactions + receipts + blocks 조인 뷰
    데이터 무결성(정밀도)을 위해 수수료 계산 시 DecimalType 사용
	"""

	logger = get_logger("Build Txn Enriched")

	txns = read_bronze(spark, "transactions", dt, schema=transaction_schema)
	receipts = read_bronze(spark, "receipts", dt, schema=receipt_schema)
	blocks = read_bronze(spark, "blocks", dt, schema=block_schema)

	logger.info(f"[build_txn_enriched] - [{dt}] 블록, 트랜잭션, 영수증 데이터 로드 완료")

	receipts_slim = receipts.select(
        F.col("transaction_hash"),
        F.col("block_number").alias("receipt_block_number"), # 조인 최적화용
        F.col("status"),
        F.col("gas_used").cast(DecimalType(38, 0)).alias("gas_used"),
        F.col("effective_gas_price").cast(DecimalType(38, 0)).alias("effective_gas_price"),
        F.col("contract_address")
    )


	blocks_slim = blocks.select(
        F.col("number").alias("block_number_from_block"), # 이름 변경하여 충돌 방지
        F.col("timestamp").alias("block_timestamp_from_block"), # 이름 변경하여 충돌 방지
        F.col("miner"),
        F.col("base_fee_per_gas").cast(DecimalType(38, 0)).alias("base_fee_per_gas"),
        F.col("gas_limit").alias("block_gas_limit"),
        F.col("gas_used").alias("block_gas_used")
    )
	
	join_cond_receipt = [
		txns["hash"] == receipts_slim["transaction_hash"],
		txns["block_number"] == receipts_slim["receipt_block_number"]
	]

	enriched = (
        txns
        .join(receipts_slim, join_cond_receipt, how="inner")
        # blocks와 조인할 때 중복되는 block_number 대신 명확한 컬럼 사용
        .join(blocks_slim, F.col("block_number") == F.col("block_number_from_block"), how="left")
        .drop("transaction_hash", "receipt_block_number", "block_number_from_block", "block_timestamp_from_block")
    )

	enriched = (
		enriched.withColumn(
			"tx_fee_eth",
			(F.col("gas_used") * F.col("effective_gas_price") / F.lit(WEI_PER_ETH)).cast(DecimalType(38, 18))
		).withColumn(
			"value_eth",
			(F.col("value").cast(DecimalType(38, 0)) / F.lit(WEI_PER_ETH)).cast(DecimalType(38, 18))
		).withColumn(
            "tx_type_label",
            F.when(F.col("transaction_type") == 0, "legacy")
             .when(F.col("transaction_type") == 1, "access_list")
             .when(F.col("transaction_type") == 2, "eip1559")
             .when(F.col("transaction_type") == 3, "blob")
             .otherwise("unknown")
        ).withColumn(
            "is_contract_call",
            F.when(F.length(F.col("input")) > 2, True).otherwise(False)
        ).withColumn(
            "is_contract_deploy",
            F.col("contract_address").isNotNull()
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
			"from_address", "to_address", "contract_address",
			"value_eth", "tx_fee_eth", 
			"tx_type_label",
			"is_success", "is_contract_call", "is_contract_deploy",
			"input", "dt"
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
