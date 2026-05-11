import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference
from airflow.exceptions import AirflowFailException

from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

# pyrefly: ignore [missing-import]
from src.storage.utils.block import get_block_number_by_date
# pyrefly: ignore [missing-import]
from src.storage.etl import (
    export_blocks_and_transactions,
    export_receipts_and_logs,
    export_token_transfers,
    export_contracts,
)


ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

default_args = {
    "owner": "chanspar",
    "depends_on_past": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": task_fail_slack_alert,
}


@dag(
    dag_id="ethereum_etl_to_gcs",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="0 1 * * *",  # 매일 새벽 1시 실행
    catchup=False,
    on_success_callback=task_succ_slack_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=15),
        callback=AsyncCallback(task_fail_slack_alert),
    ),
    tags=["ethereum", "etl", "gcs"],
)
def ethereum_etl_dag():

    @task
    def calculate_block_range() -> dict:
        """실행 날짜에 해당하는 시작/종료 블록 번호 계산"""
        # [FIX] 파라미터로 받는 방식 제거 → get_current_context() 사용
        context = get_current_context()
        logical_date = context["logical_date"]

        date_str = logical_date.strftime("%Y-%m-%d")
        next_date_str = (logical_date + timedelta(days=1)).strftime("%Y-%m-%d")

        start_block = get_block_number_by_date(date_str, ETHERSCAN_API_KEY)
        end_block = get_block_number_by_date(next_date_str, ETHERSCAN_API_KEY) - 1

        return {
            "start": start_block,
            "end": end_block,
            "date_str": date_str,
        }

    @task
    def extract_blocks_and_transactions_task(range_data: dict) -> dict:
        return export_blocks_and_transactions(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

    @task
    def extract_receipts_and_logs_task(tx_file: str, range_data: dict) -> dict:
        return export_receipts_and_logs(
            tx_file=tx_file,
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

    @task
    def extract_token_transfers_task(range_data: dict) -> str:
        return export_token_transfers(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

    @task
    def extract_contracts_task(transfer_file: str, range_data: dict) -> None:
        # [FIX] 명시적 return None으로 반환 타입 명확화
        export_contracts(
            transfer_file=transfer_file,
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

    @task
    def quality_check_task(range_data: dict, blocks_stats: dict, receipts_stats: dict) -> bool:
        """데이터 정합성(1:1:1) 및 0바이트 파일 체크"""
        date_str = range_data["date_str"]
        tx_in_blocks = blocks_stats["total_tx_in_blocks"]
        receipt_count = receipts_stats["receipt_count"]

        zero_byte_files = []
        if blocks_stats["tx_file_size"] == 0:
            zero_byte_files.append("transactions")
        if blocks_stats["block_file_size"] == 0:
            zero_byte_files.append("blocks")
        if receipts_stats["receipt_file_size"] == 0:
            zero_byte_files.append("receipts")

        issues = []
        if tx_in_blocks != receipt_count:
            issues.append(
                f"Consistency Mismatch: Blocks TX Sum ({tx_in_blocks}) != Receipts Count ({receipt_count})"
            )
        if zero_byte_files:
            issues.append(f"Zero-byte Files Detected: {', '.join(zero_byte_files)}")

        if issues:
            error_msg = f"⚠️ Quality Check Failed ({date_str}): " + " | ".join(issues)
            # 재시도 없이 즉시 실패 처리
            raise AirflowFailException(error_msg)

        print(f"✅ Quality Check Passed for {date_str}: {tx_in_blocks} transactions verified.")
        return True

    @task
    def send_summary_report(range_data: dict, blocks_stats: dict, receipts_stats: dict) -> str:
        """작업 완료 리포트 및 소요 시간/비용 추정"""
        # [FIX] dag_run 파라미터 제거 → get_current_context() 사용
        context = get_current_context()
        dag_run = context["dag_run"]

        date_str = range_data["date_str"]

        # 소요 시간 계산
        start_date = dag_run.start_date
        duration = datetime.now(start_date.tzinfo) - start_date
        duration_str = str(duration).split(".")[0]  # HH:MM:SS 형식

        total_size_bytes = (
            blocks_stats["tx_file_size"]
            + blocks_stats["block_file_size"]
            + receipts_stats["receipt_file_size"]
            + receipts_stats["log_file_size"]
        )
        total_size_mb = total_size_bytes / (1024 * 1024)
        est_cost_usd = (total_size_bytes / (1024**3)) * 0.02

        summary_msg = (
            f"✅ Ethereum ETL Success Report for {date_str}\n"
            f"• Duration: {duration_str}\n"
            f"• Total Data: {total_size_mb:.2f} MB\n"
            f"• Est. Monthly Cost: ${est_cost_usd:.6f}"
        )
        print(summary_msg)
        return summary_msg

    # ── Orchestration ──────────────────────────────────────────────────────────

    range_info = calculate_block_range()

    # Blocks & Transactions
    blocks_stats = extract_blocks_and_transactions_task(range_info)

    # Receipts & Logs  (XComArg subscript으로 tx_file 키 직접 참조)
    receipts_stats = extract_receipts_and_logs_task(blocks_stats["tx_file"], range_info)

    # Token Transfers & Contracts
    transfer_file = extract_token_transfers_task(range_info)
    extract_contracts_task(transfer_file, range_info)

    # QC → Summary (QC 통과 후에만 리포트 전송)
    qc = quality_check_task(range_info, blocks_stats, receipts_stats)
    summary = send_summary_report(range_info, blocks_stats, receipts_stats)
    summary << qc  # summary는 qc 성공 이후에만 실행


ethereum_etl_dag()
