import logging
import os
from pythonjsonlogger import jsonlogger # 로그를 JSON 형식으로 예쁘게 말아서 출력
from logging.handlers import RotatingFileHandler # 로그 파일 크기 제한 및 백업 관리

# Ensure logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOGS_DIR, "app.log")

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times if setup_logger is called repeatedly
    if not logger.handlers:
        # File handler for JSON logs (10MB max size, keep up to 5 old backups)
        file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=10*1024*1024, backupCount=5)
        # Use python-json-logger to format log records as JSON
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"}
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        # Also output to stdout for local debugging (standard text format)
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s"
        )
        stream_handler.setFormatter(stream_formatter)
        logger.addHandler(stream_handler)

    return logger

# Global app logger instance
logger = setup_logger("eth-backend")
