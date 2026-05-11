from pathlib import Path

from src.storage.config import PROVIDER_URI, GCS_BRONZE_PREFIX, ETL_MAX_WORKERS, ETL_BATCH_SIZE, get_logger
from src.storage.utils.gcs import upload_to_gcs
from src.storage.utils.shell import run_shell, _cleanup

# 로거 생성
logger = get_logger(__name__)

def export_token_transfers(start: int, end: int, date_str: str) -> str:
    """
    ERC20 / ERC721 토큰 전송 내역 추출 → GCS 업로드
    
    반환값: token_transfers 파일 경로 (contracts.py 입력으로 사용)
    ※ 성공 시 파일은 남겨두고 contracts.py로 넘기지만, 실패 시에는 여기서 삭제합니다.
    """
    transfer_file = f"token_transfers_{start}_{end}.json"

    cmd = (
        f"ethereumetl export_token_transfers "
        f"--start-block {start} --end-block {end} "
        f"--provider-uri {PROVIDER_URI} "
        f"--output {transfer_file} "
        f"--max-workers {ETL_MAX_WORKERS} --batch-size {ETL_BATCH_SIZE}"
    )

    try:
        logger.info(f"Token Transfers 추출 시작 (Block: {start} ~ {end})")
        run_shell(cmd)

        if not Path(transfer_file).exists():
            raise FileNotFoundError(f"ETL 결과 파일이 생성되지 않았습니다: {transfer_file}")

        upload_to_gcs(
            transfer_file,
            f"{GCS_BRONZE_PREFIX}/token_transfers/dt={date_str}/{transfer_file}"
        )

        logger.info(f"작업 성공: token_transfers 파일 보존 완료 ({transfer_file})")
        return transfer_file  # 정상 완료 시에만 파일 경로 반환

    except Exception:
        logger.exception(f"Token Transfers 추출 및 업로드 중 오류 발생 ({start}~{end})")
        # ★ 핵심: 실행이나 업로드 중 터졌다면 다음 스텝으로 못 가므로 찌꺼기 즉시 삭제
        _cleanup(transfer_file)
        raise
