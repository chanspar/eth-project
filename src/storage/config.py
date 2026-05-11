import os
import warnings
import logging
from logging.handlers import RotatingFileHandler

warnings.filterwarnings("ignore", category=FutureWarning)

from dotenv import load_dotenv

load_dotenv()

# ── 환경변수 ────────────────────────────────────────────
PROJECT_ID  = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")
ALCHEMY_KEY = os.getenv("ALCHEMY_API_KEY")
NETWORK     = os.getenv("ETHEREUM_NETWORK", "eth-mainnet")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")

PROVIDER_URI = f"https://{NETWORK}.g.alchemy.com/v2/{ALCHEMY_KEY}"

# ── ETL 기본 옵션 ────────────────────────────────────────
ETL_MAX_WORKERS = int(os.getenv("ETL_MAX_WORKERS", "1"))  # 무료 티어: 1
ETL_BATCH_SIZE  = int(os.getenv("ETL_BATCH_SIZE", "1"))   # 무료 티어: 1

# ── GCS 경로 prefix ─────────────────────────────────────
GCS_BRONZE_PREFIX = "bronze"
GCS_SILVER_PREFIX = "silver"


def validate_config() -> None:
    """필수 환경변수 누락 여부를 시작 시 즉시 검증"""
    logger.info("환경 변수 검증을 시작합니다...")
    required = {
        "GCP_PROJECT_ID": PROJECT_ID,
        "GCS_BUCKET_NAME": BUCKET_NAME,
        "ALCHEMY_API_KEY": ALCHEMY_KEY,
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        logger.error(f"필수 환경변수 누락: {', '.join(missing)}")
        raise EnvironmentError(f"필수 환경변수 누락: {', '.join(missing)}")
    
    logger.info(f"환경 변수 검증 완료. (Network: {NETWORK})")


# ── 로깅 설정 ────────────────────────────────────────────
LOG_FILE = "etl_project.log"
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] %(message)s")

# 콘솔 핸들러
console_hdlr = logging.StreamHandler()
console_hdlr.setFormatter(log_formatter)

# 파일 핸들러
# file_hdlr = logging.FileHandler(LOG_FILE, encoding='utf-8')
# file_hdlr.setFormatter(log_formatter)

file_handler = RotatingFileHandler(
    LOG_FILE, 
    maxBytes=5*1024*1024, 
    backupCount=5, 
    encoding='utf-8'
)
file_handler.setFormatter(log_formatter)

# 전체 적용
root = logging.getLogger()
root.setLevel(logging.INFO)
root.addHandler(console_hdlr)
root.addHandler(file_handler)

# 각 파일에서 가져다 쓸 로거 생성기
def get_logger(name: str):
    return logging.getLogger(f"ETL_App.{name}")

logger = get_logger("Config") # config.py 내부용



''' 다른 파일에서 가져다가 쓰는 법

from config import get_logger

# 파일 이름을 넘겨줌 (혹은 __name__ 사용)
# logger = get_logger("gcs_utils")

# 현재 파일의 모듈 경로가 자동으로 이름으로 설정됨
logger = get_logger(__name__)

def upload_to_gcs():
    try:
        logger.info("GCS 업로드 시작...")
        # ... 실제 업로드 코드 ...
        raise Exception("네트워크 불안정") # 에러 가정
    except Exception as e:
        # 에러 메시지뿐만 아니라 몇 번째 줄에서 왜 에러가 났는지 상세히 기록됨
        logger.exception("GCS 업로드 중 치명적 오류 발생!")
'''
