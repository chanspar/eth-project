import os
from google.cloud import bigquery
from src.config import PROJECT_ID, BUCKET_NAME, GCS_GOLD_PREFIX, BQ_DATASET, get_logger

logger = get_logger("BigQuerySync")

def create_external_table(client, table_id, gcs_uri):
    """
    GCS Parquet 경로를 BigQuery 외부 테이블로 생성/업데이트
    """
    dataset_ref = client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(table_id)
 
    external_config = bigquery.ExternalConfig("PARQUET")
    external_config.source_uris = [f"{gcs_uri}/*.parquet"]
    
    # Hive 파티셔닝 자동 감지 (dt=YYYY-MM-DD 구조 대응)
    hive_partitioning = bigquery.HivePartitioningOptions()
    hive_partitioning.mode = "STRATEGIC"
    hive_partitioning.source_uri_prefix = gcs_uri
    external_config.hive_partitioning = hive_partitioning
 
    table = bigquery.Table(table_ref)
    table.external_data_configuration = external_config
 
    try:
        # 기존 테이블 삭제 후 재생성 (스키마 변경 대응)
        client.delete_table(table_ref, not_found_ok=True)
        client.create_table(table)
        logger.info(f"✅ 외부 테이블 생성 완료: {BQ_DATASET}.{table_id}")
    except Exception as e:
        logger.error(f"❌ 테이블 생성 실패 ({table_id}): {str(e)}")

def sync_all():
    client = bigquery.Client(project=PROJECT_ID)
    
    # 데이터셋이 없으면 생성
    dataset = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
    dataset.location = "US" # 필요시 변경
    client.create_dataset(dataset, exists_ok=True)
    logger.info(f"데이터셋 확인 완료: {BQ_DATASET}")
 
    # 동기화할 테이블 리스트 (분석명/테이블명)
    tables = {
        # Whale Analysis
        "top_whales":        f"whale_analysis/top_whales",
        "exchange_flows":    f"whale_analysis/exchange_flows",
        "position_timeline": f"whale_analysis/position_timeline",
        "alert_events":      f"whale_analysis/alert_events",
        "hourly_pattern":    f"whale_analysis/hourly_pattern",
        
        # Token Ranking
        "token_ranking":     f"token_ranking/token_ranking",
        "dex_protocol":      f"token_ranking/dex_protocol",
        "new_holders":       f"token_ranking/new_holders",
        "wash_trade":        f"token_ranking/wash_trade",
        "hourly_heatmap":    f"token_ranking/hourly_heatmap",
    }
 
    for table_id, sub_path in tables.items():
        gcs_uri = f"gs://{BUCKET_NAME}/{GCS_GOLD_PREFIX}/{sub_path}"
        create_external_table(client, table_id, gcs_uri)

if __name__ == "__main__":
    sync_all()
