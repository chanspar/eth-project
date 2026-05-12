import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.sdk.exceptions import AirflowFailException

from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

# pyrefly: ignore [missing-import]
from src.storage.utils.block import get_block_number_by_date

ETH_ETL_PYTHON = "/opt/airflow/eth_etl_venv/bin/python"
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

default_args = {
    "owner": "chanspar",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": task_fail_slack_alert,
}


@dag(
    dag_id="ethereum_etl_test",
    default_args=default_args,
    start_date=datetime(2026, 5, 11),
    schedule=None,
    catchup=False,
    on_success_callback=task_succ_slack_alert,
    tags=["test", "ethereum"],
)
def ethereum_etl_test_dag():

    @task
    def calculate_block_range() -> dict:
        """실행 날짜에 해당하는 블록 번호를 딱 10개만 계산 (테스트용)"""
        context = get_current_context()
        logical_date = context["logical_date"]

        date_str = logical_date.strftime("%Y-%m-%d")

        start_block = get_block_number_by_date(date_str, ETHERSCAN_API_KEY, "after")
        
        # 🧪 TEST: 블록 범위를 10개로 제한
        end_block = start_block + 9

        print(f"🧪 Test Mode: Processing 10 blocks from {start_block} to {end_block}")
        
        return {
            "start": start_block,
            "end": end_block,
            "date_str": date_str,
        }

    @task.external_python(python=ETH_ETL_PYTHON)
    def extract_blocks_and_transactions_task(range_data: dict) -> dict:
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl import export_blocks_and_transactions
        return export_blocks_and_transactions(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

    @task.external_python(python=ETH_ETL_PYTHON)
    def extract_receipts_and_logs_task(tx_file: str, range_data: dict) -> dict:
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl import export_receipts_and_logs
        return export_receipts_and_logs(
            tx_file=tx_file,
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

    @task.external_python(python=ETH_ETL_PYTHON)
    def extract_token_transfers_and_contracts_task(range_data: dict) -> None:
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl import export_token_transfers, export_contracts
        
        # 1. Token Transfers 추출
        transfer_file = export_token_transfers(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )
        
        # 2. 추출된 파일을 이용해 Contracts 추출
        if transfer_file:
            export_contracts(
                transfer_file=transfer_file,
                start=range_data["start"],
                end=range_data["end"],
                date_str=range_data["date_str"],
            )

    @task
    def quality_check_task(range_data: dict, blocks_stats: dict, receipts_stats: dict) -> bool:
        date_str = range_data["date_str"]
        whale_count = receipts_stats.get("whale_count", 0)
        receipt_count = receipts_stats["receipt_count"]

        issues = []
        if whale_count != receipt_count:
            issues.append(f"Consistency Mismatch: Whale Hashes ({whale_count}) != Receipts Count ({receipt_count})")

        if issues:
            error_msg = f"⚠️ Quality Check Failed ({date_str}): " + " | ".join(issues)
            raise AirflowFailException(error_msg)

        print(f"✅ Quality Check Passed for {date_str}: {receipt_count} whale receipts verified.")
        return True

    @task
    def send_summary_report(range_data: dict, blocks_stats: dict, receipts_stats: dict) -> str:
        context = get_current_context()
        dag_run = context["dag_run"]
        date_str = range_data["date_str"]

        start_date = dag_run.start_date
        duration = datetime.now(start_date.tzinfo) - start_date
        duration_str = str(duration).split(".")[0]

        summary_msg = (
            f"🧪 Ethereum ETL TEST Success Report for {date_str}\n"
            f"• Duration: {duration_str}\n"
            f"• Block Range: {range_data['start']} ~ {range_data['end']} (10 Blocks)\n"
        )
        print(summary_msg)
        return summary_msg

    # ── Orchestration ──────────────────────────────────────────────────────────

    range_info = calculate_block_range()
    blocks_stats = extract_blocks_and_transactions_task(range_info)
    receipts_stats = extract_receipts_and_logs_task(blocks_stats["tx_file"], range_info)
    
    # Token Transfers & Contracts (Merged)
    extract_token_transfers_and_contracts_task(range_info)

    qc = quality_check_task(range_info, blocks_stats, receipts_stats)
    summary = send_summary_report(range_info, blocks_stats, receipts_stats)
    summary << qc

ethereum_etl_test_dag()
