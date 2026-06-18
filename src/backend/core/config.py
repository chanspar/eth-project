from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    # 환경 변수 이름과 매핑 (기본적으로 대소문자 무시)
    KAFKA_BROKER: str = "localhost:9094"
    POSTGRES_DSN: str = "postgresql://user:password@localhost:5432/eth_data"
    CONSUMER_GROUP_ID: str = "eth-etl-consumer-group"
    DB_POOL_MIN_CONN: int = 1
    DB_POOL_MAX_CONN: int = 10
    BATCH_SIZE: int = 100
    WHALE_ALERTS_TOPIC: str = "whale-alerts"
    WHALE_ALERTS_GROUP_PREFIX: str = "whale-alerts-group"
    WHALE_THRESHOLD_WEI: int = 100 * 10**18
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    ALLOWED_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://localhost:8000",
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]

    # extra="ignore": 클래스에 정의되지 않은 환경 변수가 .env에 있더라도 그냥 무시하고 넘어갑니다.
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
