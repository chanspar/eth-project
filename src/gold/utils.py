from src.config import GCS_SILVER_PREFIX, GCS_GOLD_PREFIX, BUCKET_NAME
from pyspark.sql import DataFrame
from src.silver.spark_config import get_logger

logger = get_logger("Gold Utils")


def silver_path(table: str, dt: str) -> str:
    partition = dt if dt.startswith("dt=") else f"dt={dt}"
    return f"gs://{BUCKET_NAME}/{GCS_SILVER_PREFIX}/{table}/{partition}/"


def gold_path(table: str, dt: str) -> str:
    partition = dt if dt.startswith("dt=") else f"dt={dt}"
    return f"gs://{BUCKET_NAME}/{GCS_GOLD_PREFIX}/{table}/{partition}/"


def write_gold(df: DataFrame, path: str, partition_cols: list = None):
    """Gold 레이어 저장"""
    if partition_cols is None:
        partition_cols = ["dt"]
        
    output_path = f"gs://{BUCKET_NAME}/{GCS_GOLD_PREFIX}/{path}"
    logger.info(f"💾 Gold 레이어 저장 시작: {output_path}")
    (
        df
        .repartition(*partition_cols)
        .write
        .mode("overwrite")
        .partitionBy(*partition_cols)
        .parquet(output_path)
    )
    logger.info(f"✅ 저장 완료: {output_path}")
