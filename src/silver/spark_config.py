import os
import src.schema.bronze_schema as bronze_schema
import sys
from pyspark.sql import SparkSession
from src.config import BUCKET_NAME, get_logger, GCS_BRONZE_PREFIX, GCS_SILVER_PREFIX



WEI_PER_ETH = 1_000_000_000_000_000_000  # 1 ETH = 10^18 Wei

def get_spark_session(app_name: str):
    # K8s 파드 내부인지 자동 감지
    if os.getenv("KUBERNETES_SERVICE_HOST"):
        env = os.getenv("APP_ENV", "prod").lower()
    else:
        env = os.getenv("APP_ENV", "local").lower()
        
    builder = SparkSession.builder.appName(app_name)

    if env == "prod":
        print(f"🚀 Running in PROD (K8s) mode (App: {app_name})")
        # K8s 환경에서는 Airflow(SparkKubernetesOperator)가 주입한 설정을 100% 존중합니다.
        # 파이썬 코드 단에서 설정을 덮어씌우지 않고 순수하게 세션만 생성합니다.
        return builder.getOrCreate()

    # =========================================================================
    # 아래는 오직 '로컬 환경(내 컴퓨터)'에서 실행할 때만 적용되는 설정입니다.
    # =========================================================================
    print(f"🔧 Running in LOCAL mode (App: {app_name})")
    
    gcp_key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    gcp_project_id = os.getenv("GCP_PROJECT_ID")

    if os.name == "nt":
        # 윈도우 환경 경로 꼬임 방지
        os.environ.pop("PYSPARK_PYTHON", None)
        os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
        builder = (
            builder
            .config("spark.pyspark.python", "python3")
            .config("spark.pyspark.driver.python", sys.executable)
        )

    builder = (
        builder
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "4g")  # 4G로 증설하여 캐싱 시 GC Locker OOM 방지
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # GCS & BigQuery 커넥터 JAR (로컬 구동용)
        .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22,com.google.cloud.spark:spark-4.0-bigquery:0.44.1")
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
        .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
    )

    if gcp_key_path:
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcp_key_path)
    
    if gcp_project_id:
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.project.id", gcp_project_id)

    return builder.getOrCreate()


def read_bronze(spark: SparkSession, folder_name:str, dt:str, schema):
    logger = get_logger(f"Read Bronze Layer {folder_name} / {dt}")
    spark.sparkContext.setLogLevel("ERROR")
    path = f"gs://{BUCKET_NAME}/{GCS_BRONZE_PREFIX}/{folder_name}/{dt}"
    logger.info(f"[Read_Bronze] Reading from {path}")
    return spark.read.schema(schema).json(path)


def read_silver(spark: SparkSession, folder_name:str, dt: str, schema):
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
