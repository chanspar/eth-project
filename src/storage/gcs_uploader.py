from src.config import validate_config
from src.storage.pipeline import collect_all, collect_by_date_range

if __name__ == "__main__":
    validate_config()
    # 단일 블록 범위
    # collect_all(15000000, 15000010)

    # 날짜 범위로 실행
    collect_by_date_range("2026-04-01", "2026-04-30")
