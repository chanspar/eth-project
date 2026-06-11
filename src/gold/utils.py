from src.config import GCS_SILVER_PREFIX, GCS_GOLD_PREFIX, BUCKET_NAME, PROJECT_ID, BQ_DATASET_ID
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

def write_gold_to_bq(df: DataFrame, table_name: str, partition_cols: list = None):
    if partition_cols is None:
        partition_cols = ["dt"]
    
    bq_table = f"{PROJECT_ID}.{BQ_DATASET_ID}.{table_name}"

    logger.info(f"BigQuery 적재 시작: {bq_table}")

    try:
        (
            df.write
            .format("bigquery")
            .option("table", bq_table)
            .option("temporaryGcsBucket", BUCKET_NAME)
            .option("partitionField", partition_cols[0])
            .option("parentProject", PROJECT_ID)
            .mode("append")
            .save()
        )
        logger.info(f"BigQuery 적재 완료: {bq_table}")
    except Exception as e:
        logger.error(f"BigQuery 적재 실패: {e}")
        raise e
