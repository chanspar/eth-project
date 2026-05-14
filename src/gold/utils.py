from src.config import GCS_SILVER_PREFIX, GCS_GOLD_PREFIX, BUCKET_NAME
from pyspark.sql import DataFrame
from src.silver.spark_config import get_logger

logger = get_logger("Gold Utils")


def silver_path(table: str, dt: str) -> str:
    return f"gs://{BUCKET_NAME}/{GCS_SILVER_PREFIX}/{table}/dt={dt}/"


def gold_path(table: str, dt: str) -> str:
    return f"gs://{BUCKET_NAME}/{GCS_GOLD_PREFIX}/{table}/dt={dt}/"


def write_gold(df: DataFrame, path: str):
    """Gold 레이어 저장"""
    output_path = f"gs://{BUCKET_NAME}/{GCS_GOLD_PREFIX}/{path}"
    logger.info(f"💾 Gold 레이어 저장 시작: {output_path}")
    (
        df
        .repartition("dt")
        .write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(output_path)
    )
    logger.info(f"✅ 저장 완료: {output_path}")
