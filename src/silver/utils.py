from pyspark.sql import DataFrame
from src.silver.spark_config import get_logger
from src.config import BUCKET_NAME, GCS_SILVER_PREFIX


def write_silver(df: DataFrame, path: str):
	"""Silver 레이어 저장"""
	logger = get_logger("Write Silver")
	silver_path = f"gs://{BUCKET_NAME}/{GCS_SILVER_PREFIX}"
	output_path = f"{silver_path}/{path}"

	logger.info(f"💾 Silver 레이어 저장 시작: {output_path}")
	(
		df
		.repartition("dt")
		.write
		.mode("overwrite")
		.partitionBy("dt")
		.parquet(output_path)
	)
	logger.info(f"✅ 저장 완료: {output_path}")
