import logging

from src.storage.utils import get_block_date, get_block_number_by_date
from src.storage.etl import (
    export_blocks_and_transactions,
    export_receipts_and_logs,
    export_token_transfers,
    export_contracts
)
from src.config import ETHERSCAN_API_KEY

logger = logging.getLogger(__name__)


def collect_all(start_block: int, end_block: int) -> None:
    """
    전체 ETL 파이프라인 실행

    GCS 저장 구조:
    bronze/
    ├── blocks/dt={date}/
    ├── transactions/dt={date}/
    ├── receipts/dt={date}/
    ├── logs/dt={date}/
    ├── token_transfers/dt={date}/
    └── contracts/dt={date}/
    """
    date_str = get_block_date(start_block)
    logger.info(f"=== 파이프라인 시작 | block {start_block}~{end_block} | {date_str} ===")

    logger.info("--- Step 1: Blocks & Transactions ---")
    tx_file = export_blocks_and_transactions(start_block, end_block, date_str)

    logger.info("--- Step 2: Receipts & Logs ---")
    export_receipts_and_logs(tx_file, start_block, end_block, date_str)

    logger.info("--- Step 3: Token Transfers ---")
    transfer_file = export_token_transfers(start_block, end_block, date_str)

    logger.info("--- Step 4: Contracts ---")
    export_contracts(transfer_file, start_block, end_block, date_str)

    logger.info(f"=== 파이프라인 완료 | block {start_block}~{end_block} ===")


def collect_by_date_range(start_date: str, end_date: str) -> None:
    """
    날짜 범위를 하루씩 쪼개서 파이프라인 실행
    예) start_date="2026-01-01", end_date="2026-01-30"
    """
    from datetime import datetime, timedelta

    current = datetime.strptime(start_date, "%Y-%m-%d")
    end     = datetime.strptime(end_date, "%Y-%m-%d")

    while current <= end:
        date_str   = current.strftime("%Y-%m-%d")
        next_date  = (current + timedelta(days=1)).strftime("%Y-%m-%d")

        start_block = get_block_number_by_date(date_str,   ETHERSCAN_API_KEY, closest="after")
        end_block   = get_block_number_by_date(next_date,  ETHERSCAN_API_KEY, closest="before")

        logger.info(f"=== {date_str} | block {start_block}~{end_block} ===")
        collect_all(start_block, end_block)

        current += timedelta(days=1)
