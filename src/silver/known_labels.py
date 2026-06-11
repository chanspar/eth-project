from pyspark.sql import SparkSession
from pathlib import Path

# 파일 경로 정의 (현재 파일 위치 기반으로 절대 경로 계산)
BASE_DIR = Path(__file__).resolve().parent.parent  # src 폴더
DATA_DIR = BASE_DIR / "data"
KNOWN_LABELS_PATH = DATA_DIR / "known_labels.parquet"
TOKEN_META_PATH   = DATA_DIR / "top1000_erc20_tokens.parquet"

# ERC-20 전송 이벤트 시그니처 (Transfer topic0)
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# 파일에 없지만 분석에 꼭 필요한 특수 주소들 (수동 관리)
MANUAL_LABELS = {
    "0x000000000000000000000000000000000000dead": ("Dead Address", "Burn"),
    "0x0000000000000000000000000000000000000000": ("Null Address", "Burn"),
}

def load_address_labels(spark: SparkSession):
    """
    수집된 주소 라벨 + 수동 라벨(MANUAL_LABELS)을 합쳐서 반환
    """
    # 1. 수동 라벨 DF 생성
    manual_rows = [(addr.lower(), name, cat) for addr, (name, cat) in MANUAL_LABELS.items()]
    manual_df = spark.createDataFrame(manual_rows, ["address", "label_name", "label_category"])

    # 2. 파일 라벨 로드 (있는 경우)
    if not KNOWN_LABELS_PATH.exists():
        return manual_df
        
    file_df = spark.read.parquet(str(KNOWN_LABELS_PATH)) \
        .selectExpr("address", "name as label_name", "category as label_category")

    # 3. 합치기 (중복 시 수동 라벨 우선은 아니지만, Union 후 필요시 drop_duplicates 가능)
    return file_df.unionByName(manual_df).dropDuplicates(["address"])


def load_token_metadata(spark: SparkSession):
    """
    수집된 토큰 메타데이터(심볼, 데시멀 등) 데이터를 Spark DataFrame으로 로드
    컬럼: address, symbol, name, decimals, market_cap_usd
    """
    if not TOKEN_META_PATH.exists():
        print(f"⚠ Warning: {TOKEN_META_PATH} 파일이 없습니다. 빈 DF를 반환합니다.")
        return spark.createDataFrame([], "address string, symbol string, name string, decimals int")

    return spark.read.parquet(str(TOKEN_META_PATH)) \
        .selectExpr("address as token_address", "symbol", "name as token_name", "decimals")


def load_token_meta_df(spark: SparkSession):
    return load_token_metadata(spark)
