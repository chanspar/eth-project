import os
# pyrefly: ignore [missing-import]
import pendulum
# pyrefly: ignore [missing-import]
from pendulum import datetime
from typing import Any

# pyrefly: ignore [missing-import, missing-module-attribute]
from airflow.sdk import dag, task, Asset, get_current_context, AsyncCallback, DeadlineAlert, DeadlineReference, Metadata
# pyrefly: ignore [missing-import]
from airflow.sdk.exceptions import AirflowFailException

# pyrefly: ignore [missing-import]
from utils.notifications import task_fail_slack_alert, task_succ_slack_alert

from src.storage.utils.block import get_block_number_by_date

ETH_ETL_PYTHON = "/opt/airflow/eth_etl_venv/bin/python"
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

# Bronze DAG가 생산하는 데이터 자산 — Silver DAG의 Asset 기반 스케줄링 트리거용
BRONZE_COMPLETE = Asset("bronze/ethereum_etl_complete")

default_args = {
    "owner": "chanspar",
    "depends_on_past": True,
    "retries": 5,                             # 429 에러 대비 재시도 횟수 증가
    "retry_delay": pendulum.duration(minutes=3),      # 레이트 리밋이 풀릴 때까지 3분간 대기
    "on_failure_callback": task_fail_slack_alert,
}


@dag(
    dag_id="ethereum_etl_to_gcs",
    default_args=default_args,
    start_date=datetime(2026, 5, 1),
    schedule="0 1 * * *",  # 매일 오전 1시 실행
    catchup=True,
    max_active_runs=1,
    on_success_callback=task_succ_slack_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=pendulum.duration(hours=12),
        callback=AsyncCallback(task_fail_slack_alert),
    ),
    tags=["ethereum", "etl", "gcs"],
)
def ethereum_etl_dag():

    @task
    def calculate_block_range() -> dict:
        """실행 날짜에 해당하는 시작/종료 블록 번호 계산"""
        api_key = os.getenv("ETHERSCAN_API_KEY")
        if not api_key:
            raise AirflowFailException("❌ ETHERSCAN_API_KEY 환경변수가 설정되지 않았습니다.")

        context = get_current_context()
        logical_date = context["logical_date"]

        date_str = logical_date.strftime("%Y-%m-%d")
        next_date_str = pendulum.instance(logical_date).add(days=1).strftime("%Y-%m-%d")

        start_block = get_block_number_by_date(date_str, api_key)
        end_block = get_block_number_by_date(next_date_str, api_key) - 1

        return {
            "start": start_block,
            "end": end_block,
            "date_str": date_str,
        }

    @task.external_python(python=ETH_ETL_PYTHON)
    def extract_blocks_and_transactions_task(range_data: Any) -> dict:
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl import export_blocks_and_transactions
        result = export_blocks_and_transactions(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )
        # Task SDK 로그에서 확인 가능한 디버깅 메시지
        print(f"DEBUG: extract_blocks_and_transactions_task result: {result}")
        return result

    @task.external_python(python=ETH_ETL_PYTHON)
    def export_receipts_task(tx_file: str, range_data: Any) -> dict:
        """영수증 및 로그 추출 (100 ETH 이상 고래 트랜잭션만)"""
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl.receipts import export_receipts_and_logs
        return export_receipts_and_logs(
            tx_file=tx_file,
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"]
        )

    @task.external_python(python=ETH_ETL_PYTHON)
    def extract_token_transfers_and_contracts_task(range_data: Any) -> None:
        """토큰 전송 내역 및 컨트랙트 추출"""
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl import export_token_transfers
        
        # 1. Token Transfers 추출
        export_token_transfers(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )
        
        # 2. 추출된 파일을 이용해 Contracts 추출 (선택 사항)
        # if transfer_file:
        #     export_contracts(transfer_file, range_data["start"], range_data["end"], range_data["date_str"])


    @task
    def quality_check_task(range_data: Any, blocks_stats: Any, receipts_stats: Any) -> bool:
        """데이터 정합성(Whale Hash == Receipt count) 및 0바이트 파일 체크"""
        if range_data is None:
            raise AirflowFailException("❌ Quality Check Failed: 'range_data' is None.")
        if blocks_stats is None:
            raise AirflowFailException("❌ Quality Check Failed: 'blocks_stats' XCom을 찾을 수 없습니다. extract_blocks_and_transactions_task가 성공했는지 확인하세요.")
        if receipts_stats is None:
            raise AirflowFailException("❌ Quality Check Failed: 'receipts_stats' XCom을 찾을 수 없습니다. export_receipts_task가 성공했는지 확인하세요.")

        date_str = range_data["date_str"]
        whale_count = receipts_stats.get("whale_count", 0)
        receipt_count = receipts_stats.get("receipt_count", 0)

        # 0바이트 파일 체크
        zero_byte_files = []
        if blocks_stats["tx_file_size"] == 0:
            zero_byte_files.append("transactions")
        if blocks_stats["block_file_size"] == 0:
            zero_byte_files.append("blocks")

        issues = []
        # 고래 해시 수 == 영수증 수 정합성 검증
        if whale_count != receipt_count:
            issues.append(f"Consistency Mismatch: Whale Hashes ({whale_count}) != Receipts Count ({receipt_count})")
        if zero_byte_files:
            issues.append(f"Zero-byte Files Detected: {', '.join(zero_byte_files)}")

        if issues:
            error_msg = f"⚠️ Quality Check Failed ({date_str}): " + " | ".join(issues)
            raise AirflowFailException(error_msg)

        print(f"✅ Quality Check Passed for {date_str}: {receipt_count} whale receipts verified.")
        return True

    @task
    def send_summary_report(range_data: Any, blocks_stats: Any, receipts_stats: Any) -> str:
        """작업 완료 리포트 및 소요 시간/비용 추정"""
        if range_data is None or blocks_stats is None or receipts_stats is None:
            return "⚠️ Summary Report Skip: Missing input data (None)."

        context = get_current_context()
        dag_run = context["dag_run"]

        date_str = range_data["date_str"]
        start_blk = range_data["start"]
        end_blk = range_data["end"]

        # 1. 소요 시간 계산
        start_date = dag_run.start_date
        start_date = dag_run.start_date
        if start_date is not None:
            elapsed = pendulum.now("UTC") - pendulum.instance(start_date)
            elapsed_str = str(elapsed).split(".")[0]  # HH:MM:SS 형식
        else:
            elapsed_str = "N/A"

        # 2. 데이터 통계
        whale_count = receipts_stats.get("whale_count", 0)
        total_tx_in_blocks = blocks_stats.get("total_tx_in_blocks", 0)
        total_size_bytes = (
            blocks_stats.get("tx_file_size", 0)
            + blocks_stats.get("block_file_size", 0)
            + receipts_stats.get("receipt_file_size", 0)
        )
        total_size_mb = total_size_bytes / (1024 * 1024)

        # 3. 비용 및 리소스 추정
        # Alchemy CU 추정 (영수증 건당 15 CU 소모 기준)
        est_alchemy_cu = whale_count * 15
        # GCS 저장 비용 추정 (Nearline 기준 $0.02 / GB)
        est_gcs_cost_usd = (total_size_bytes / (1024**3)) * 0.02

        summary_msg = (
            f"📊 **Ethereum ETL Success Report ({date_str})**\n"
            f"• **Blocks**: {start_blk:,} ~ {end_blk:,} ({end_blk - start_blk + 1:,} blocks)\n"
            f"• **Total TX in Blocks**: {total_tx_in_blocks:,}\n"
            f"• **Whale TX (100+ ETH)**: {whale_count:,} receipts verified\n"
            f"• **Duration**: {elapsed_str}\n"
            f"• **Data Size**: {total_size_mb:.2f} MB\n"
            f"• **Alchemy Resources**: ~{est_alchemy_cu:,} CU consumed\n"
            f"• **Est. GCS Storage Cost**: ${est_gcs_cost_usd:.6f}"
        )
        
        print(summary_msg)
        return summary_msg

    # 1. 블록 범위 계산
    range_info = calculate_block_range()

    # 2. 블록/트랜잭션 추출
    blocks_stats_arg = extract_blocks_and_transactions_task(range_info)

    # 3. 영수증 추출 (100 ETH 이상 고래 트랜잭션만)
    # pyrefly: ignore [bad-index]
    receipts_stats_arg = export_receipts_task(blocks_stats_arg["tx_file"], range_info)

    # 4. 토큰 전송 추출
    # ⚠️ Alchemy rate limit 방지를 위해 영수증 작업 완료 후 시작
    transfers_arg = extract_token_transfers_and_contracts_task(range_info)

    # 5. 정합성 검사 (Whale Hash == Receipt Count) 및 0바이트 파일 체크
    qc = quality_check_task(
        range_data=range_info, 
        blocks_stats=blocks_stats_arg, 
        receipts_stats=receipts_stats_arg
    )

    # 6. 요약 리포트 전송
    summary = send_summary_report(
        range_data=range_info, 
        blocks_stats=blocks_stats_arg, 
        receipts_stats=receipts_stats_arg
    )
    
    # 7. Bronze 자산 이벤트 발행 (logical_date 메타데이터 포함)
    @task(outlets=[BRONZE_COMPLETE])
    def publish_bronze_asset(range_data: Any):
        """Bronze 레이어 처리 완료 — Asset 이벤트 발행 (logical_date 메타데이터 포함)"""
        date_str = range_data["date_str"]
        print(f"✅ Bronze 처리 완료 ({date_str}). Silver DAG 트리거 준비.")
        yield Metadata(BRONZE_COMPLETE, {"logical_date": date_str})

    publish = publish_bronze_asset(range_info)

    # 의존성: receipts → transfers → QC → summary → publish
    # (range → blocks → receipts 는 데이터 전달로 암시적 연결)
    receipts_stats_arg >> transfers_arg >> qc >> summary >> publish


ethereum_etl_dag()
