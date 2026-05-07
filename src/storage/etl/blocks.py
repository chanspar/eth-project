from pathlib import Path

from src.config import PROVIDER_URI, GCS_BRONZE_PREFIX, ETL_MAX_WORKERS, ETL_BATCH_SIZE, get_logger
from src.storage.utils.gcs import upload_to_gcs
from src.storage.utils.shell import run_shell, _cleanup

logger = get_logger(__name__)

def export_blocks_and_transactions(start: int, end: int, date_str: str) -> str:
    """
    블록 및 트랜잭션 추출 → GCS 업로드
    반환값: transactions 파일 경로 (receipts.py 입력으로 사용)
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

        # 2. 파일 생성 확인 (실패 시 예외 발생)
        if not Path(block_file).exists() or not Path(tx_file).exists():
            raise FileNotFoundError("ethereumetl 실행 완료 후 결과 파일이 정상적으로 생성되지 않았습니다.")

        # 3. GCS 업로드
        upload_to_gcs(block_file, f"{GCS_BRONZE_PREFIX}/blocks/dt={date_str}/{block_file}")
        upload_to_gcs(tx_file, f"{GCS_BRONZE_PREFIX}/transactions/dt={date_str}/{tx_file}")

    except Exception:
        logger.exception(f"블록/트랜잭션 추출 및 업로드 중 오류 발생 ({start}~{end})")
        # 실패했을 때는 다음 스텝으로 못 넘어가므로 tx_file도 찌꺼기로 남지 않게 삭제
        _cleanup(tx_file)
        raise
        
    finally:
        # 4. block_file은 성공/실패 여부와 상관없이 항상 삭제
        _cleanup(block_file)

    logger.info(f"작업 성공: tx 파일 보존 완료 ({tx_file})")
    return tx_file
