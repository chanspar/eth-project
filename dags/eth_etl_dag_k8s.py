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

# Bronze K8s DAG가 생산하는 데이터 자산 — Silver K8s DAG의 Asset 기반 스케줄링 트리거용
BRONZE_K8S_COMPLETE = Asset("bronze/ethereum_etl_k8s_complete")

default_args = {
    "owner": "chanspar",
    "depends_on_past": True,                           # 과거 DAG Run이 성공해야 다음 날짜가 실행됨 (순차 실행 보장)
    "retries": 5,                                      # 429 에러 대비 재시도 횟수 증가
    "retry_delay": pendulum.duration(minutes=3),       # 레이트 리밋이 풀릴 때까지 3분간 대기
    "on_failure_callback": task_fail_slack_alert,
}


@dag(
    dag_id="ethereum_etl_to_gcs_k8s",
    default_args=default_args,
    start_date=datetime(2026, 6, 10, tz="Asia/Seoul"),
    schedule="10 9 * * *",  # 매일 오전 9시 10분 실행
    catchup=True,           # start_date(6월 1일)부터 현재까지 밀린 과거 잡들을 자동으로 예약
    max_active_runs=1,      # 한 번에 하나의 날짜만 실행되도록 제한 (과부하 방지)
    on_success_callback=task_succ_slack_alert,
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=pendulum.duration(hours=12),
        callback=AsyncCallback(task_fail_slack_alert),
    ),
    tags=["ethereum", "etl", "gcs", "k8s"],
)
def ethereum_etl_k8s_dag():

    @task
    def calculate_block_range() -> dict:
        """실행 날짜에 해당하는 시작/종료 블록 번호 계산"""
        # [FIX] API 키를 모듈 레벨이 아닌 태스크 실행 시점에 읽어
        #       키 로테이션 즉시 반영 및 XCom 경유 노출 위험 제거
        api_key = os.getenv("ETHERSCAN_API_KEY")
        if not api_key:
            raise AirflowFailException("❌ ETHERSCAN_API_KEY 환경변수가 설정되지 않았습니다.")

        context = get_current_context()
        logical_date = context["logical_date"]
        # Airflow 3: logical_date는 스케줄 실행 시간이므로, 하루를 빼서 전일자(target_date)를 계산합니다.
        target_date = pendulum.instance(logical_date).subtract(days=1)

        date_str = target_date.strftime("%Y-%m-%d")
        # [FIX] pendulum.instance().add()로 DST-safe한 날짜 덧셈 적용
        next_date_str = pendulum.instance(target_date).add(days=1).strftime("%Y-%m-%d")

        start_block = get_block_number_by_date(date_str, api_key)
        end_block = get_block_number_by_date(next_date_str, api_key) - 1

        return {
            "start": start_block,
            "end": end_block,
            "date_str": date_str,
        }

    @task.external_python(python=ETH_ETL_PYTHON)
    def extract_blocks_tx_and_receipts_task(range_data: Any) -> dict:
        """블록, 트랜잭션, 고래 영수증을 하나의 파드(Pod) 내에서 연속으로 추출 (디스크 공유 및 GCS 업로드)"""
        import sys
        sys.path.append("/opt/airflow")

        from src.storage.etl import export_blocks_and_transactions
        from src.storage.etl.receipts import export_receipts_and_logs

        # 1. 블록 & 트랜잭션 추출 (로컬 저장 후 GCS 업로드)
        print(f"DEBUG: Starting blocks and transactions extraction for range: {range_data}")
        blocks_stats = export_blocks_and_transactions(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )
        print(f"DEBUG: export_blocks_and_transactions result: {blocks_stats}")

        # [FIX] 두 API 호출 사이에 간격을 두어 Alchemy API Rate Limit(429) 방지
        import time
        print("DEBUG: Sleeping for 60 seconds to reset Alchemy API rate limits...")
        time.sleep(60)

        # 2. 동일 로컬 디스크 상에 남겨진 tx_file을 읽어 영수증 및 로그 추출
        #    (완료 후 GCS 업로드하며, export_receipts_and_logs 내부 finally 블록에서
        #     tx_file을 포함한 로컬 임시 파일들이 자동 정리됨)
        print(f"DEBUG: Starting whale receipts extraction using local tx file: {blocks_stats['tx_file']}")
        receipts_stats = export_receipts_and_logs(
            tx_file=blocks_stats["tx_file"],
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )
        print(f"DEBUG: export_receipts_and_logs result: {receipts_stats}")

        return {
            "blocks_stats": blocks_stats,
            "receipts_stats": receipts_stats,
        }

    @task.external_python(python=ETH_ETL_PYTHON)
    def extract_token_transfers_task(range_data: Any) -> dict:
        """토큰 전송 내역 추출

        [FIX] 함수명에서 '_and_contracts' 제거 — 컨트랙트 추출이 미구현 상태이므로
              함수명과 구현 일치. 향후 컨트랙트 추출 추가 시 별도 태스크로 분리 권장.
        """
        import sys
        sys.path.append("/opt/airflow")
        from src.storage.etl import export_token_transfers

        transfers_stats = export_token_transfers(
            start=range_data["start"],
            end=range_data["end"],
            date_str=range_data["date_str"],
        )

        return transfers_stats

    @task
    def quality_check_task(
        range_data: Any,
        extract_stats: Any,
        transfers_stats: Any,
    ) -> bool:
        """데이터 정합성(Whale Hash == Receipt count) 및 0바이트 파일 체크

        [FIX] transfers_stats 파라미터 추가로 토큰 전송 파일 검증 포함
        """
        if range_data is None:
            raise AirflowFailException("❌ Quality Check Failed: 'range_data' is None.")
        if extract_stats is None:
            raise AirflowFailException("❌ Quality Check Failed: 'extract_stats' is None.")

        blocks_stats = extract_stats.get("blocks_stats")
        receipts_stats = extract_stats.get("receipts_stats")

        if blocks_stats is None:
            raise AirflowFailException("❌ Quality Check Failed: 'blocks_stats' XCom을 찾을 수 없습니다.")
        if receipts_stats is None:
            raise AirflowFailException("❌ Quality Check Failed: 'receipts_stats' XCom을 찾을 수 없습니다.")

        date_str = range_data["date_str"]
        whale_count = receipts_stats.get("whale_count", 0)
        receipt_count = receipts_stats.get("receipt_count", 0)

        # 0바이트 파일 체크
        zero_byte_files = []
        if blocks_stats.get("tx_file_size", 0) == 0:
            zero_byte_files.append("transactions")
        if blocks_stats.get("block_file_size", 0) == 0:
            zero_byte_files.append("blocks")
        # [FIX] 토큰 전송 파일 0바이트 체크 추가
        #       키가 없을 경우(구현 미반영)는 None으로 폴백하여 오탐 방지
        transfer_file_size = (transfers_stats or {}).get("transfer_file_size")
        if transfer_file_size is not None and transfer_file_size == 0:
            zero_byte_files.append("token_transfers")

        issues = []
        # 고래 해시 수 == 영수증 수 정합성 검증
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
    def send_summary_report(
        range_data: Any,
        extract_stats: Any,
        transfers_stats: Any,
    ) -> str:
        """작업 완료 리포트 및 소요 시간/비용 추정"""
        if range_data is None or extract_stats is None:
            return "⚠️ Summary Report Skip: Missing input data (None)."

        blocks_stats = extract_stats.get("blocks_stats")
        receipts_stats = extract_stats.get("receipts_stats")

        if blocks_stats is None or receipts_stats is None:
            return "⚠️ Summary Report Skip: Missing blocks_stats or receipts_stats."

        context = get_current_context()
        dag_run = context["dag_run"]

        date_str = range_data["date_str"]
        start_blk = range_data["start"]
        end_blk = range_data["end"]

        # 1. 소요 시간 계산
        # [FIX] 변수명 elapsed로 변경 → pendulum.duration 이름 충돌 완전 해소
        # [FIX] dag_run.start_date None 체크 추가 (DAG 실행 직전 None 가능)
        # [FIX] pendulum.now("UTC") 사용 → tzinfo 객체 직접 전달 방식의 버전 불안정성 해소
        start_date = dag_run.start_date
        if start_date is not None:
            elapsed = pendulum.now("UTC") - pendulum.instance(start_date)
            elapsed_str = str(elapsed).split(".")[0]  # HH:MM:SS 형식
        else:
            elapsed_str = "N/A"

        # 2. 데이터 통계
        whale_count = receipts_stats.get("whale_count", 0)
        total_tx_in_blocks = blocks_stats.get("total_tx_in_blocks", 0)
        # [FIX] transfers_stats 파일 크기도 합산하여 전체 데이터 사이즈 정확도 향상
        total_size_bytes = (
            blocks_stats.get("tx_file_size", 0)
            + blocks_stats.get("block_file_size", 0)
            + receipts_stats.get("receipt_file_size", 0)
            + (transfers_stats or {}).get("transfer_file_size", 0)
        )
        total_size_mb = total_size_bytes / (1024 * 1024)

        # 3. 비용 및 리소스 추정
        # Alchemy CU 추정 (영수증 건당 15 CU 소모 기준)
        est_alchemy_cu = whale_count * 15
        # GCS 저장 비용 추정 (Nearline 기준 $0.02/GB/월)
        # [FIX] 주석에 "월간 보관 비용" 명시 → 일별 비용으로 오해 방지
        est_gcs_monthly_cost_usd = (total_size_bytes / (1024**3)) * 0.02

        summary_msg = (
            f"📊 **Ethereum ETL Success Report (Kubernetes) ({date_str})**\n"
            f"• **Blocks**: {start_blk:,} ~ {end_blk:,} ({end_blk - start_blk + 1:,} blocks)\n"
            f"• **Total TX in Blocks**: {total_tx_in_blocks:,}\n"
            f"• **Whale TX (100+ ETH)**: {whale_count:,} receipts verified\n"
            f"• **Duration**: {elapsed_str}\n"
            f"• **Data Size**: {total_size_mb:.2f} MB\n"
            f"• **Alchemy Resources**: ~{est_alchemy_cu:,} CU consumed\n"
            f"• **Est. GCS Monthly Storage Cost**: ${est_gcs_monthly_cost_usd:.6f}"
        )

        print(summary_msg)
        return summary_msg

    # ─── Task Wiring ────────────────────────────────────────────────────────────

    # 1. 블록 범위 계산
    range_info = calculate_block_range()

    # 2. 블록/트랜잭션/영수증 통합 추출 (동일 파드 내 로컬 파일 공유)
    extract_stats_arg = extract_blocks_tx_and_receipts_task(range_info)

    # 3. 토큰 전송 추출
    #    ⚠️ Alchemy rate limit 방지를 위해 앞선 대용량 추출 작업 완료 후 시작
    #    [FIX] extract_stats_arg >> transfers_arg 로 실행 순서만 명시적으로 보장
    #          (데이터는 넘기지 않고 순서만 제어 — 의도 명확화)
    transfers_arg = extract_token_transfers_task(range_info)
    extract_stats_arg >> transfers_arg

    # 4. 정합성 검사 (Whale Hash == Receipt Count) 및 0바이트 파일 체크
    #    [FIX] transfers_arg를 파라미터로 전달 → TaskFlow가 transfers → qc 의존성 자동 설정
    qc = quality_check_task(
        range_data=range_info,
        extract_stats=extract_stats_arg,
        transfers_stats=transfers_arg,
    )

    # 5. 요약 리포트 전송
    #    [FIX] transfers_arg를 파라미터로 전달 → 데이터 사이즈 합산 정확도 향상
    summary = send_summary_report(
        range_data=range_info,
        extract_stats=extract_stats_arg,
        transfers_stats=transfers_arg,
    )

    # 6. Bronze 자산 이벤트 발행 (logical_date 메타데이터 포함)
    @task(outlets=[BRONZE_K8S_COMPLETE])
    def publish_bronze_asset(range_data: Any):
        """Bronze 레이어 처리 완료 — Asset 이벤트 발행 (logical_date 메타데이터 포함)"""
        date_str = range_data["date_str"]
        print(f"✅ Bronze K8s 처리 완료 ({date_str}). Silver DAG 트리거 준비.")
        yield Metadata(BRONZE_K8S_COMPLETE, {"logical_date": date_str})

    publish = publish_bronze_asset(range_info)

    # QC 통과 후 리포트 전송 → 자산 발행
    qc >> summary >> publish


ethereum_etl_k8s_dag()
