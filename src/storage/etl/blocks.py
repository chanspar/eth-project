from pathlib import Path
import json

from src.config import PROVIDER_URI, GCS_BRONZE_PREFIX, ETL_MAX_WORKERS, ETL_BATCH_SIZE, get_logger
from src.storage.utils.gcs import upload_to_gcs
from src.storage.utils.shell import run_shell, _cleanup

logger = get_logger(__name__)

def export_blocks_and_transactions(start: int, end: int, date_str: str) -> dict:
    """
    블록 및 트랜잭션 추출 → GCS 업로드
    반환값: 메타데이터 및 통계 (정합성 체크용)
    """
    block_file = f"blocks_{start}_{end}.json"
    tx_file    = f"transactions_{start}_{end}.json"

    cmd = (
        f"ethereumetl export_blocks_and_transactions "
        f"--start-block {start} --end-block {end} "
        f"--provider-uri {PROVIDER_URI} "
        f"--blocks-output {block_file} "
        f"--transactions-output {tx_file} "
        f"--max-workers {ETL_MAX_WORKERS} --batch-size {ETL_BATCH_SIZE}"
    )

    try:
        # 1. 셸 명령어 실행
        logger.info(f"블록/트랜잭션 추출 시작 (Block: {start} ~ {end})")
        run_shell(cmd)

        # 2. 파일 생성 확인
        if not Path(block_file).exists() or not Path(tx_file).exists():
            raise FileNotFoundError("ethereumetl 실행 완료 후 결과 파일이 정상적으로 생성되지 않았습니다.")

        # --- KPI 수집 ---
        # blocks 파일에서 transaction_count 합계 구하기
        total_tx_in_blocks = 0
        with open(block_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    total_tx_in_blocks += json.loads(line).get("transaction_count", 0)
        
        # 파일 크기 (Bytes)
        tx_file_size = Path(tx_file).stat().st_size
        block_file_size = Path(block_file).stat().st_size
        # ----------------

        # 3. GCS 업로드
        upload_to_gcs(block_file, f"{GCS_BRONZE_PREFIX}/blocks/dt={date_str}/{block_file}")
        upload_to_gcs(tx_file, f"{GCS_BRONZE_PREFIX}/transactions/dt={date_str}/{tx_file}")

        return {
            "tx_file": tx_file,
            "total_tx_in_blocks": total_tx_in_blocks,
            "tx_file_size": tx_file_size,
            "block_file_size": block_file_size,
            "start_block": start,
            "end_block": end
        }

    except Exception:
        logger.exception(f"블록/트랜잭션 추출 및 업로드 중 오류 발생 ({start}~{end})")
        _cleanup(tx_file)
        raise
        
    finally:
        _cleanup(block_file)
