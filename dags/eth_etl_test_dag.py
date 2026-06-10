import os
import pendulum
from pendulum import datetime
from typing import Any

from airflow.sdk import dag, task, get_current_context
from airflow.sdk.exceptions import AirflowFailException

# pyrefly: ignore [missing-import]
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert
from src.storage.utils.block import get_block_number_by_date

ETH_ETL_PYTHON = "/opt/airflow/eth_etl_venv/bin/python"

default_args = {
    "owner": "chanspar",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=3),
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

    @task(task_id="caculate_block")
    def calculate_block_range() -> dict:
        """실행 날짜에 해당하는 블록 번호를 딱 10개만 계산 (테스트용)"""
        api_key = os.getenv("ETHERSCAN_API_KEY")
        if not api_key:
            raise AirflowFailException("❌ ETHERSCAN_API_KEY 환경변수가 설정되지 않았습니다.")

        context = get_current_context()
        logical_date = context["logical_date"]

        date_str = logical_date.strftime("%Y-%m-%d")

        start_block = get_block_number_by_date(date_str, api_key)
        # 🧪 TEST: 블록 범위를 10개로 제한
        end_block = start_block + 9

        print(f"🧪 Test Mode: Processing 10 blocks from {start_block} to {end_block}")
        
        return {
            "start": start_block,
            "end": end_block,
            "date_str": date_str,
        }

    @task.external_python(task_id="extract_blocks_tx_and_receipts", python=ETH_ETL_PYTHON)
    def extract_blocks_tx_and_receipts_task(range_data: Any) -> dict:
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl import export_blocks_and_transactions
        from src.storage.etl.receipts import export_receipts_and_logs

        # 1. 블록 & 트랜잭션 추출 (로컬 저장 후 GCS 업로드)
        blocks_stats = export_blocks_and_transactions(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

        # 2. 동일 로컬 디스크의 임시 tx_file을 읽어 영수증 및 로그 추출
        receipts_stats = export_receipts_and_logs(
            tx_file=blocks_stats["tx_file"],
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )
        return receipts_stats

    @task.external_python(task_id="extract_token_transfer", python=ETH_ETL_PYTHON)
    def extract_token_transfers_and_contracts_task(range_data: Any) -> None:
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
        # if transfer_file:
        #     export_contracts(
        #         transfer_file=transfer_file,
        #         start=range_data["start"],
        #         end=range_data["end"],
        #         date_str=range_data["date_str"],
        #     )

    @task(task_id="quality_chk")
    def quality_check_task(range_data: Any, receipts_stats: Any) -> bool:
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
    def send_summary_report(range_data: Any, receipts_stats: Any) -> str:
        context = get_current_context()
        dag_run = context["dag_run"]
        date_str = range_data["date_str"]

        start_date = dag_run.start_date
        if start_date is not None:
            elapsed = pendulum.now("UTC") - pendulum.instance(start_date)
            elapsed_str = str(elapsed).split(".")[0]  # HH:MM:SS 형식
        else:
            elapsed_str = "N/A"

        whale_count = receipts_stats.get("whale_count", 0)
        receipt_size_mb = receipts_stats.get("receipt_file_size", 0) / (1024 * 1024)

        summary_msg = (
            f"🧪 Ethereum ETL TEST Success Report for {date_str}\n"
            f"• Duration: {elapsed_str}\n"
            f"• Block Range: {range_data['start']} ~ {range_data['end']} (10 Blocks)\n"
            f"• Whale Receipts: {whale_count}건\n"
            f"• Receipt Data Size: {receipt_size_mb:.2f} MB\n"
        )
        print(summary_msg)
        return summary_msg

    # ── Orchestration ──────────────────────────────────────────────────────────

    range_info = calculate_block_range()
    receipts_stats = extract_blocks_tx_and_receipts_task(range_info)
    
    # Token Transfers & Contracts (Merged)
    # ⚠️ Alchemy rate limit 방지를 위해 receipts 완료 후 실행
    transfers = extract_token_transfers_and_contracts_task(range_info)

    qc = quality_check_task(range_info, receipts_stats)
    summary = send_summary_report(range_info, receipts_stats)

    receipts_stats >> transfers >> qc >> summary

ethereum_etl_test_dag()
