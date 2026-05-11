from google.cloud import bigquery
from datetime import datetime, timedelta
from src.storage.config import PROJECT_ID, BUCKET_NAME

# 설정
START_DATE = "2026-04-24"
END_DATE = "2026-05-01"  # 테스트로 짧게 먼저 해보세요!

client = bigquery.Client(project=PROJECT_ID)

# 6가지 데이터 타입 정의
# (테이블명, GCS폴더명, 타임스탬프컬럼, 추출할컬럼들)
EXPORT_TASKS = [
    {
        "table": "blocks",
        "folder": "blocks",
        "time_col": "timestamp",
        "select_clause": "*"
    },
    {
        "table": "logs",
        "folder": "logs",
        "time_col": "block_timestamp",
        "select_clause": "*"
    },
    {
        "table": "token_transfers",
        "folder": "token_transfers",
        "time_col": "block_timestamp",
        "select_clause": "*"
    },
    {
        "table": "contracts",
        "folder": "contracts",
        "time_col": "block_timestamp",
        "select_clause": "*"
    },
    {
        "table": "transactions",
        "folder": "transactions",
        "time_col": "block_timestamp",
        "select_clause": """
            `hash`, nonce, block_hash, block_number, transaction_index, 
            from_address, to_address, value, gas, gas_price, input, 
            block_timestamp, max_fee_per_gas, max_priority_fee_per_gas, transaction_type
        """
    },
    {
        "table": "transactions",
        "folder": "receipts",
        "time_col": "block_timestamp",
        "select_clause": """
            `hash` AS transaction_hash, transaction_index, block_hash, block_number, 
            receipt_cumulative_gas_used AS cumulative_gas_used, 
            receipt_gas_used AS gas_used, 
            receipt_contract_address AS contract_address, 
            receipt_root AS root, 
            receipt_status AS status, 
            receipt_effective_gas_price AS effective_gas_price
        """
    }
]

def run_backfill():
    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    current_dt = start_dt
    while current_dt <= end_dt:
        date_str = current_dt.strftime("%Y-%m-%d")
        print(f"\n--- Processing Date: {date_str} ---")
        
        for task in EXPORT_TASKS:
            gcs_uri = f"gs://{BUCKET_NAME}/bronze/{task['folder']}/dt={date_str}/*.json"
            
            sql = f"""
                EXPORT DATA OPTIONS(
                  uri='{gcs_uri}',
                  format='JSON',
                  overwrite=true
                ) AS
                SELECT 
                    {task['select_clause']},
                    DATE({task['time_col']}) AS block_date
                FROM 
                    `bigquery-public-data.crypto_ethereum.{task['table']}`
                WHERE 
                    DATE({task['time_col']}) = '{date_str}'
            """
            
            print(f"🚀 Exporting {task['folder']} for {date_str}...")
            try:
                query_job = client.query(sql)
                query_job.result() # 작업 완료 대기
                print(f"✅ Success: {task['folder']}")
            except Exception as e:
                print(f"❌ Failed: {task['folder']} | Error: {e}")
                
        current_dt += timedelta(days=1)

if __name__ == "__main__":
    run_backfill()
    print("\n✨ All backfill tasks completed!")
    