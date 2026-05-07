import json
from pathlib import Path

from src.config import PROVIDER_URI, GCS_BRONZE_PREFIX, ETL_MAX_WORKERS, ETL_BATCH_SIZE, get_logger
from src.storage.utils.gcs import upload_to_gcs
from src.storage.utils.shell import run_shell, _cleanup

logger = get_logger(__name__)

def _extract_tx_hashes(tx_file: str) -> str:
    """transactions JSON → 트랜잭션 hash 목록 txt"""
    hash_file = tx_file.replace(".json", "_hashes.txt")
    hashes = []

    try:
        # 인코딩 명시 (Windows 등에서 에러 방지)
        with open(tx_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    hashes.append(json.loads(line)["hash"])

        with open(hash_file, "w", encoding="utf-8") as f:
            f.write("\n".join(hashes))

        logger.info(f"📋 트랜잭션 해시 {len(hashes)}개 추출 완료 → {hash_file}")
        return hash_file
        
    except Exception:
        logger.exception(f"트랜잭션 해시 추출 중 오류 발생 (파일: {tx_file})")
        raise

def export_receipts_and_logs(tx_file: str, start: int, end: int, date_str: str) -> None:
    """
    트랜잭션 파일에서 해시 추출 → 영수증 및 로그 추출 → GCS 업로드

    tx_file: export_blocks_and_transactions()의 반환값
    """
    hash_file    = ""
    receipt_file = f"receipts_{start}_{end}.json"
    log_file     = f"logs_{start}_{end}.json"

    try:
        logger.info(f"Receipts & Logs 추출 시작 (Block: {start} ~ {end})")
        
        # 1. 해시 추출 (이 과정에서 실패하면 아래 로직은 안 탐)
        hash_file = _extract_tx_hashes(tx_file)

        # 2. 셸 명령어 실행
        cmd = (
            f"ethereumetl export_receipts_and_logs "
            f"--transaction-hashes {hash_file} "
            f"--provider-uri {PROVIDER_URI} "
            f"--receipts-output {receipt_file} "
            f"--logs-output {log_file} "
            f"--max-workers {ETL_MAX_WORKERS} --batch-size {ETL_BATCH_SIZE}"
        )
        run_shell(cmd)

        # 3. 파일 생성 검증
        if not Path(receipt_file).exists() or not Path(log_file).exists():
            raise FileNotFoundError("ethereumetl 실행 완료 후 영수증/로그 파일이 정상적으로 생성되지 않았습니다.")

        # 4. GCS 업로드
        upload_to_gcs(receipt_file, f"{GCS_BRONZE_PREFIX}/receipts/dt={date_str}/{receipt_file}")
        upload_to_gcs(log_file, f"{GCS_BRONZE_PREFIX}/logs/dt={date_str}/{log_file}")
        
        logger.info("작업 성공: Receipts & Logs 업로드 완료")

    except Exception:
        logger.exception(f"Receipts & Logs 처리 중 오류 발생 ({start}~{end})")
        raise

    finally:
        # 5. 성공하든 실패하든, 생성된 모든 임시 파일 삭제
        # _cleanup은 단일 경로를 받으므로 리스트를 순회하며 호출합니다.
        files_to_delete = [tx_file, hash_file, receipt_file, log_file]
        for f_path in files_to_delete:
            if f_path:  # 파일 경로가 존재하는 경우에만 (hash_file이 빈 문자열일 수 있으므로)
                _cleanup(f_path)
