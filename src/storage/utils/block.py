import httpx
from datetime import datetime, timezone
from src.storage.config import PROVIDER_URI, get_logger

logger = get_logger(__name__)


def get_block_number_by_date(date_str: str, api_key: str, closest: str = "after") -> int:
    """날짜 → 블록 번호 (Etherscan API V2)"""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        timestamp = int(dt.timestamp())
        # 만약 요청한 시점이 현재보다 미래라면, 현재 시간을 기준으로 조회합니다 (NOTOK 방지)
        now = datetime.now(timezone.utc)
        if dt > now:
            logger.info(f"요청 시점({date_str})이 미래이므로 현재 시점({now.strftime('%Y-%m-%d %H:%M:%S')})으로 조정합니다.")
            timestamp = int(now.timestamp())

        logger.info(f"Etherscan 블록 조회 시작 (Timestamp: {timestamp})")
        
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(
                "https://api.etherscan.io/v2/api",
                params={
                    "chainid": 1,
                    "module": "block",
                    "action": "getblocknobytime",
                    "timestamp": timestamp,
                    "closest": closest,
                    "apikey": api_key,
                }
            )
            resp.raise_for_status()
            data = resp.json()

        if data["status"] != "1":
            # 만약 여전히 NOTOK가 난다면 (예: API 키 문제 등), 상세 메시지와 함께 예외 발생
            logger.error(f"Etherscan 응답 오류: {data.get('message')} | Result: {data.get('result')}")
            raise ValueError(f"Etherscan API 오류: {data.get('result', data.get('message'))}")
        
        block_number = int(data["result"])
        logger.info(f"조회 성공: Block #{block_number}")
        return block_number

    except Exception:
        logger.exception(f"블록 번호 조회 중 예외 발생: {date_str}")
        raise


def get_block_date(block_number: int) -> str:
    """블록 번호 → 실제 UTC 날짜 (YYYY-MM-DD)"""
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBlockByNumber",
            "params": [hex(block_number), False],
            "id": 1
        }
        
        # httpx.post 사용
        with httpx.Client(timeout=10.0) as client:
            resp = client.post(PROVIDER_URI, json=payload)
            resp.raise_for_status()
            result = resp.json().get("result")

        if not result:
            logger.error(f"블록 {block_number} 데이터가 존재하지 않습니다.")
            raise ValueError(f"블록 {block_number} 조회 실패")

        timestamp = int(result["timestamp"], 16)
        date_str = datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
        
        logger.info(f"블록 {block_number} 날짜 확인: {date_str}")
        return date_str

    except Exception:
        logger.exception(f"블록 날짜 조회 중 예외 발생: {block_number}")
        raise
