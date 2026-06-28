import logging
import os
from pythonjsonlogger import jsonlogger
from logging.handlers import RotatingFileHandler

# Docker 컨테이너 내부에서는 /app/logs/ 경로에 기록됨
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOGS_DIR, "consumer.log")

def setup_logger(name: str) -> logging.Logger:
    _logger = logging.getLogger(name)
    _logger.setLevel(logging.INFO)

    if not _logger.handlers:
        # JSON 파일 핸들러 (ELK 수집용 — Filebeat가 이 파일을 읽어감)
        file_handler = RotatingFileHandler(LOG_FILE_PATH, maxBytes=10*1024*1024, backupCount=5)
        formatter = jsonlogger.JsonFormatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            rename_fields={"asctime": "timestamp", "levelname": "level"}
        )
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)

        # 콘솔 핸들러 (docker logs 확인용)
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(message)s"
        )
        stream_handler.setFormatter(stream_formatter)
        _logger.addHandler(stream_handler)

    return _logger

logger = setup_logger("eth-consumer")
