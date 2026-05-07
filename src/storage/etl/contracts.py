import json
from pathlib import Path

# logging 모듈 대신 get_logger 임포트
from src.config import PROVIDER_URI, GCS_BRONZE_PREFIX, ETL_MAX_WORKERS, ETL_BATCH_SIZE, get_logger
from src.storage.utils.shell import run_and_upload, _cleanup

logger = get_logger(__name__)

def _extract_unique_addresses(transfer_file: str) -> str:
    """token_transfers JSON → 고유 컨트랙트 주소 txt"""
    addr_file = transfer_file.replace(".json", "_addresses.txt")
    unique_addrs = set()

    try:
        # 인코딩 명시 (Windows 등 환경 호환성)
        with open(transfer_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    unique_addrs.add(json.loads(line)["token_address"])

        with open(addr_file, "w", encoding="utf-8") as f:
            f.write("\n".join(unique_addrs))

        logger.info(f"📋 고유 컨트랙트 주소 {len(unique_addrs)}개 추출 완료 → {addr_file}")
        return addr_file
        
    except Exception:
        logger.exception(f"컨트랙트 주소 추출 중 오류 발생 (파일: {transfer_file})")
        raise


def export_contracts(transfer_file: str, start: int, end: int, date_str: str) -> None:
    """
    토큰 전송 파일에서 컨트랙트 주소 추출 → 컨트랙트 정보 추출 → GCS 업로드
    is_erc20 / is_erc721 필드로 EOA vs CA 구분 가능

    transfer_file: export_token_transfers()의 반환값
    """
    addr_file = ""
    contract_file = f"contracts_{start}_{end}.json"

    try:
        logger.info(f"Contracts 추출 시작 (Block: {start} ~ {end})")
        
        # 1. 주소 추출
        addr_file = _extract_unique_addresses(transfer_file)

        # 2. 실행 및 GCS 업로드 (run_and_upload 내부에서 contract_file 생성 검증 및 삭제 처리됨)
        run_and_upload(
            cmd=(
                f"ethereumetl export_contracts "
                f"--contract-addresses {addr_file} "
                f"--provider-uri {PROVIDER_URI} "
                f"--output {contract_file} "
                f"--max-workers {ETL_MAX_WORKERS} --batch-size {ETL_BATCH_SIZE}"
            ),
            local_file=contract_file,
            gcs_path=f"{GCS_BRONZE_PREFIX}/contracts/dt={date_str}/{contract_file}"
        )

        logger.info("작업 성공: Contracts 업로드 완료")

    except Exception:
        logger.exception(f"Contracts 처리 중 오류 발생 ({start}~{end})")
        raise

    finally:
        # 3. 여기서 발생한 임시 파일(addr_file)과 이전 스텝 파일(transfer_file) 삭제
        # contract_file은 run_and_upload의 finally 구문에서 이미 삭제 처리됩니다.
        files_to_delete = [transfer_file, addr_file]
        for f_path in files_to_delete:
            if f_path:  # 파일 경로가 존재하는 경우에만
                _cleanup(f_path)
