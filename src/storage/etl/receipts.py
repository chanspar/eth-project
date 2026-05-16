import json
from pathlib import Path

from src.config import PROVIDER_URI, GCS_BRONZE_PREFIX, get_logger
from src.storage.utils.gcs import upload_to_gcs
from src.storage.utils.shell import run_shell, _cleanup

logger = get_logger(__name__)

def _extract_tx_hashes(tx_file: str, min_eth: int = 0) -> tuple[str, int]:
    """transactions JSON → (0 ETH 이상) 트랜잭션 hash 목록 txt"""
    hash_file = tx_file.replace(".json", "_tx_hashes.txt")
    hashes = []
    
    # 🌟 0 ETH = 10^18 Wei (이더리움의 기본 단위)
    wei_threshold = min_eth * (10 ** 18)

    try:
        with open(tx_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    tx_data = json.loads(line)
                    
                    # 🌟 value(전송된 ETH 양)가 없는 경우 0으로 처리
                    # 이더리움 트랜잭션 json의 value는 문자열 타입("1000000000...")이므로 정수형으로 변환
                    tx_value = int(tx_data.get("value", "0"))
                    
                    # 기준치(100 ETH) 이상인 트랜잭션 해시만 수집
                    if tx_value >= wei_threshold:
                        hashes.append(tx_data["hash"])

        with open(hash_file, "w", encoding="utf-8") as f:
            f.write("\n".join(hashes))

        # 로그도 간지나게 고래 아이콘 하나 넣어줍니다.
        logger.info(f"🐳 {min_eth} ETH 이상 트랜잭션 해시 {len(hashes)}개 추출 완료 → {hash_file}")
        return hash_file, len(hashes)
        
    except Exception:
        logger.exception(f"트랜잭션 해시 추출 중 오류 발생 (파일: {tx_file})")
        raise


def export_receipts_and_logs(tx_file: str, start: int, end: int, date_str: str) -> dict:
    """
    트랜잭션 파일에서 해시 추출 → 영수증 및 로그 추출 → GCS 업로드
    반환값: 통계 정보
    """
    hash_file    = ""
    receipt_file = f"receipts_{start}_{end}.json"

    try:
        logger.info(f"Receipts 추출 시작 (Block: {start} ~ {end})")
        
        # 1. 해시 추출
        hash_file, whale_count = _extract_tx_hashes(tx_file)

        # 2. 셸 명령어 실행 (Chunking & Throttling)
        CHUNK_SIZE = 100
        import time
        
        # 전체 해시 목록 읽기
        with open(hash_file, "r") as f:
            all_hashes = [line.strip() for line in f if line.strip()]
            
        logger.info(f"총 {len(all_hashes)}개의 해시를 {CHUNK_SIZE}개씩 나눠서 추출합니다 (Throttling 적용)...")
        
        # 기존 파일 있으면 삭제
        if Path(receipt_file).exists(): Path(receipt_file).unlink()
            
        for i in range(0, len(all_hashes), CHUNK_SIZE):
            chunk = all_hashes[i:i + CHUNK_SIZE]
            chunk_hash_file = f"temp_hashes_{i}.txt"
            chunk_receipt_file = f"temp_receipts_{i}.json"
            
            with open(chunk_hash_file, "w") as f:
                f.write("\n".join(chunk))
                
            cmd = (
                f"ethereumetl export_receipts_and_logs "
                f"--transaction-hashes {chunk_hash_file} "
                f"--provider-uri {PROVIDER_URI} "
                f"--receipts-output {chunk_receipt_file} "
                f"--max-workers 1 --batch-size 1"
            )
            
            logger.info(f"[{i}/{len(all_hashes)}] 추출 중...")
            run_shell(cmd)
            
            # 결과 병합
            with open(receipt_file, "a") as rf, open(chunk_receipt_file, "r") as crf:
                rf.write(crf.read())
                
            # 임시 파일 삭제
            for f in [chunk_hash_file, chunk_receipt_file]:
                if Path(f).exists(): Path(f).unlink()
                
            # 🧊 Alchemy가 숨쉴 시간 주기 (330 CU/s 유지)
            if i + CHUNK_SIZE < len(all_hashes):
                time.sleep(2)

        # 3. 파일 생성 검증
        if not Path(receipt_file).exists():
            raise FileNotFoundError("ethereumetl 실행 완료 후 영수증 파일이 정상적으로 생성되지 않았습니다.")

        # --- KPI 수집 ---
        receipt_count = 0
        with open(receipt_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    receipt_count += 1
        
        receipt_file_size = Path(receipt_file).stat().st_size
        # ----------------

        # 4. GCS 업로드
        upload_to_gcs(receipt_file, f"{GCS_BRONZE_PREFIX}/receipts/dt={date_str}/{receipt_file}")
        
        return {
            "whale_count": whale_count,
            "receipt_count": receipt_count,
            "receipt_file_size": receipt_file_size,
            "log_file_size": 0
        }

    except Exception:
        logger.exception(f"Receipts 처리 중 오류 발생 ({start}~{end})")
        raise

    finally:
        # ⚠️ 중요: tx_file은 이전 태스크에서 넘겨받은 원본이므로 여기서 지우지 않습니다.
        # (지우면 재시도 시 FileNotFoundError 발생)
        files_to_delete = [hash_file, receipt_file]
        for f_path in files_to_delete:
            if f_path and Path(f_path).exists():
                _cleanup(f_path)
