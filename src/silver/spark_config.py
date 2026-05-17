import os
import src.schema.bronze_schema as bronze_schema
from pyspark.sql import SparkSession
from src.config import BUCKET_NAME, get_logger, GCS_BRONZE_PREFIX, GCS_SILVER_PREFIX



WEI_PER_ETH = 1_000_000_000_000_000_000  # 1 ETH = 10^18 Wei

def get_spark_session(app_name: str):
    env = os.getenv("APP_ENV", "local").lower()
    gcp_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    gcp_project_id = os.getenv("GCP_PROJECT_ID")

    builder = SparkSession.builder.appName(app_name)

    # 1. Common GCS Connector Settings
    builder = (
        builder
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.session.timeZone", "UTC")  # 타임존 UTC 고정
        .config("spark.sql.shuffle.partitions", "8")
    )

    # 2. Environment Specific Settings
    if env == "local":
        print(f"🔧 Running in LOCAL mode (App: {app_name})")
        builder = (
            builder
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.memory", "4g")  # 4G로 증설하여 캐싱 시 GC Locker OOM 방지
            # 로컬에서는 커넥터 JAR를 Maven에서 자동으로 관리 (GCS 커넥터 + BigQuery 커넥터)
            .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22,com.google.cloud.spark:spark-4.0-bigquery:0.44.1")
        )
    else:
        print(f"🚀 Running in {env.upper()} mode (App: {app_name})")
        # 운영 환경에서도 동일한 커넥터 조합 사용
        builder = builder.config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22,com.google.cloud.spark:spark-4.0-bigquery:0.44.1")

    # 3. GCP Authentication & Project ID
    if gcp_key_path:
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_path)
    
    if gcp_project_id:
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.project.id", gcp_project_id)

    return builder.getOrCreate()


def read_bronze(spark: SparkSession, folder_name:str, dt:str, schema=None):
    logger = get_logger(f"Read Bronze Layer {folder_name} / {dt}")
    spark.sparkContext.setLogLevel("ERROR")
    path = f"gs://{BUCKET_NAME}/{GCS_BRONZE_PREFIX}/{folder_name}/{dt}"
    logger.info(f"[Read_Bronze] Reading from {path}")
    return spark.read.schema(schema).json(path)


def read_silver(spark: SparkSession, folder_name:str, dt: str, schema=None):
    logger = get_logger(f"Read Silver Layer {folder_name} / {dt}")
    spark.sparkContext.setLogLevel("ERROR")
    path = f"gs://{BUCKET_NAME}/{GCS_SILVER_PREFIX}/{folder_name}/{dt}"
    logger.info(f"[Read_Silver] Reading from {path}")
    return spark.read.schema(schema).parquet(path)


if __name__ == "__main__":
    spark = get_spark_session("Read Bronze Layer transactions")
    df = read_bronze(spark,"transactions","dt=2026-05-01",bronze_schema.transaction_schema)
    print("############ Read_bronze 테스트 ##################")
    print(df.first())
    print(df.count())
