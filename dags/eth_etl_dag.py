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
    "depends_on_past": True,
    "retries": 5,                             # 429 에러 대비 재시도 횟수 증가
    "retry_delay": timedelta(minutes=3),      # 레이트 리밋이 풀릴 때까지 3분간 대기
    "on_failure_callback": task_fail_slack_alert,
}


@dag(
    dag_id="ethereum_etl_to_gcs",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="0 10 * * *",  # 매일 오전 10시 실행
    catchup=True,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=12),
        callback=AsyncCallback(task_fail_slack_alert),
    ),
    tags=["ethereum", "etl", "gcs"],
)
def ethereum_etl_dag():

    @task
    def calculate_block_range() -> dict:
        """실행 날짜에 해당하는 시작/종료 블록 번호 계산"""
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
        import time
        sys.path.append("/opt/airflow")
        
        # 🧊 Alchemy Rate Limit 쿨다운: 대용량 작업 직후 잠깐 대기
        print("Waiting 60 seconds for Alchemy rate limit cool-down...")
        time.sleep(60)
        
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
        """데이터 정합성(whale_count == receipt_count) 및 0바이트 파일 체크"""
        date_str = range_data["date_str"]
        
        # [FIX] 100 ETH 필터링으로 인해 전체 트랜잭션 수와 영수증 수는 다를 수 있음
        # 추출한 고래 해시 수와 실제 저장된 영수증 수를 비교해야 함
        whale_count = receipts_stats.get("whale_count", 0)
        receipt_count = receipts_stats["receipt_count"]

        zero_byte_files = []
        if blocks_stats["tx_file_size"] == 0:
            zero_byte_files.append("transactions")
        if blocks_stats["block_file_size"] == 0:
            zero_byte_files.append("blocks")
        if receipts_stats["receipt_file_size"] == 0:
            zero_byte_files.append("receipts")

        issues = []
        # 고래 해시를 추출했는데 영수증 수가 다르면 문제임
        if whale_count != receipt_count:
            issues.append(
                f"Consistency Mismatch: Whale Hashes ({whale_count}) != Receipts Count ({receipt_count})"
            )
        if zero_byte_files:
            issues.append(f"Zero-byte Files Detected: {', '.join(zero_byte_files)}")

        if issues:
            error_msg = f"⚠️ Quality Check Failed ({date_str}): " + " | ".join(issues)
            raise AirflowFailException(error_msg)

        print(f"✅ Quality Check Passed for {date_str}: {receipt_count} whale receipts verified.")
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

    # Token Transfers & Contracts (Merged)
    # ⚠️ 병렬 실행 시 Alchemy 429 에러 방지를 위해 Receipts 작업 완료 후 시작하도록 설정
    transfers = extract_token_transfers_and_contracts_task(range_info)
    transfers << receipts_stats

    # QC → Summary (모든 추출 작업 완료 후에만 진행)
    qc = quality_check_task(range_info, blocks_stats, receipts_stats)
    summary = send_summary_report(range_info, blocks_stats, receipts_stats)
    
    # ⚠️ 모든 작업이 완료된 후 QC와 Summary를 하도록 의존성 명시
    transfers >> qc >> summary


ethereum_etl_dag()
