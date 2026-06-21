import logging
import os
from pythonjsonlogger import jsonlogger

# Ensure logs directory exists
LOGS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))), "logs")
os.makedirs(LOGS_DIR, exist_ok=True)
LOG_FILE_PATH = os.path.join(LOGS_DIR, "app.log")

def setup_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times if setup_logger is called repeatedly
    if not logger.handlers:
        # File handler for JSON logs
        file_handler = logging.FileHandler(LOG_FILE_PATH)
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
