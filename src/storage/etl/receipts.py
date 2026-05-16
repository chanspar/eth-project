import os
import requests
import json
import time
from pathlib import Path
from src.config import PROVIDER_URI, GCS_BRONZE_PREFIX, get_logger
from src.storage.utils.gcs import upload_to_gcs
from src.storage.utils.shell import _cleanup

logger = get_logger(__name__)

def map_receipt(r: dict) -> dict:
    """영수증 로우 데이터를 분석용 스키마로 변환"""
    return {
        "transaction_hash": r.get("transactionHash"),
        "transaction_index": int(r.get("transactionIndex", "0x0"), 16),
        "block_hash": r.get("blockHash"),
        "block_number": int(r.get("blockNumber", "0x0"), 16),
        "cumulative_gas_used": int(r.get("cumulativeGasUsed", "0x0"), 16),
        "gas_used": int(r.get("gasUsed", "0x0"), 16),
        "contract_address": r.get("contractAddress"),
        "status": int(r.get("status", "0x0"), 16) if r.get("status") else None,
        "effective_gas_price": int(r.get("effectiveGasPrice", "0x0"), 16)
    }

def fetch_block_receipts(session: requests.Session, block_num: int, retries: int = 5) -> list:
    """단일 블록의 영수증을 가져오며 에러 발생 시 재시도 로직 수행"""
    payload = {
        "jsonrpc": "2.0",
        "id": block_num,
        "method": "eth_getBlockReceipts",
        "params": [hex(block_num)]
    }

    wait_time = 2.0
    for i in range(retries):
        try:
            response = session.post(PROVIDER_URI, json=payload, timeout=30)
            
            if response.status_code == 429:
                logger.warning(f"⚠️ 429 Rate Limit (Block {block_num}). {wait_time}s 대기... ({i+1}/{retries})")
                time.sleep(wait_time)
                wait_time *= 2
                continue
                
            response.raise_for_status()
            return response.json().get("result") or []
            
        except Exception as e:
            if i == retries - 1:
                logger.error(f"❌ 블록 {block_num} 최종 실패: {e}")
                return []
            time.sleep(wait_time)
            wait_time *= 2
            
    return []

def export_receipts_and_logs(start: int, end: int, date_str: str) -> dict:
    receipt_file = f"receipts_{start}_{end}.json"
    receipt_count = 0

    try:
        logger.info(f"🚀 [안정 모드] Receipts 추출 시작 (Block: {start} ~ {end})")
        if Path(receipt_file).exists(): Path(receipt_file).unlink()

        with requests.Session() as session, open(receipt_file, "a", encoding="utf-8") as f:
            last_request_time = 0
            
            for block_num in range(start, end + 1):
                # 1. Alchemy CU 제한 준수를 위한 스로틀링 (최소 1.1초 간격)
                now = time.time()
                elapsed = now - last_request_time
                if elapsed < 1.1:
                    time.sleep(1.1 - elapsed)
                
                # 2. 데이터 추출
                receipts = fetch_block_receipts(session, block_num)
                last_request_time = time.time()
                
                # 3. 데이터 매핑 및 저장
                if receipts:
                    for r in receipts:
                        f.write(json.dumps(map_receipt(r)) + "\n")
                        receipt_count += 1
                
                # 4. 주기적 로그
                if (block_num - start + 1) % 50 == 0 or block_num == end:
                    logger.info(f"⏳ 진행률: {block_num} / {end} 블록 완료 ({receipt_count:,}건 누적)")
                    
                    if (block_num - start + 1) % 100 == 0:
                        logger.info(f"⏳ 진행 중: {block_num} / {end} 블록 완료")

        if not Path(receipt_file).exists() or receipt_count == 0:
            raise FileNotFoundError("영수증 추출 데이터가 없습니다.")

        file_size = Path(receipt_file).stat().st_size
        logger.info(f"📤 GCS 업로드 중: {receipt_file} ({receipt_count:,}건)")
        upload_to_gcs(receipt_file, f"{GCS_BRONZE_PREFIX}/receipts/dt={date_str}/{receipt_file}")
        
        return {"receipt_count": receipt_count, "receipt_file_size": file_size, "log_file_size": 0}

    except Exception:
        logger.exception("Receipts 처리 중 오류 발생")
        raise
    finally:
        _cleanup(receipt_file)
