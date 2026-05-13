from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, ArrayType, BooleanType


block_schema = StructType([
    StructField("number", LongType(), True),
    StructField("hash", StringType(), True),
    StructField("parent_hash", StringType(), True),
    StructField("nonce", StringType(), True),  # Hex 값이므로 String 처리
    StructField("sha3_uncles", StringType(), True),
    StructField("logs_bloom", StringType(), True),
    StructField("transactions_root", StringType(), True),
    StructField("state_root", StringType(), True),
    StructField("receipts_root", StringType(), True),
    StructField("miner", StringType(), True),
    StructField("difficulty", LongType(), True),
    StructField("total_difficulty", StringType(), True), # 주의: 누적 난이도는 Long 범위도 초과할 수 있어 String 권장
    StructField("size", IntegerType(), True),
    StructField("extra_data", StringType(), True),
    StructField("gas_limit", LongType(), True),
    StructField("gas_used", LongType(), True),
    StructField("timestamp", LongType(), True), # Unix timestamp
    StructField("transaction_count", IntegerType(), True),
    StructField("base_fee_per_gas", LongType(), True)
])


transaction_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("nonce", LongType(), True),
    StructField("block_hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("transaction_index", IntegerType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    # value는 Wei 단위이며 LongType 한계를 쉽게 초과하므로 StringType 필수!
    StructField("value", StringType(), True),
    StructField("gas", LongType(), True),
    StructField("gas_price", LongType(), True), # 가스비 관련은 보통 LongType 내에 수용 가능
    StructField("input", StringType(), True),
    StructField("block_timestamp", LongType(), True), # Unix Timestamp
    StructField("max_fee_per_gas", LongType(), True),
    StructField("max_priority_fee_per_gas", LongType(), True),
    StructField("transaction_type", IntegerType(), True)
])


receipt_schema = StructType([
    StructField("transaction_hash", StringType(), True),
    StructField("transaction_index", IntegerType(), True),
    StructField("block_hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("cumulative_gas_used", LongType(), True),
    StructField("gas_used", LongType(), True),
    StructField("contract_address", StringType(), True), # 일반 송금일 경우 null
    StructField("root", StringType(), True),             # EIP-658 이전 레거시 필드 (주로 null)
    StructField("status", IntegerType(), True),          # 1: 성공, 0: 실패
    StructField("effective_gas_price", LongType(), True) # 실제 지불된 가스 단가 (Wei)
])


log_schema = StructType([
    StructField("log_index", LongType(), True),
    StructField("transaction_hash", StringType(), True),
    StructField("transaction_index", LongType(), True),
    StructField("block_hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("address", StringType(), True),
    StructField("data", StringType(), True),
    StructField("topics", ArrayType(StringType()), True)
])


token_transfer_schema = StructType([
    StructField("token_address", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("value", StringType(), True), 
    StructField("transaction_hash", StringType(), True),
    StructField("log_index", LongType(), True),
    StructField("block_number", LongType(), True)
])


smart_contract_schema = StructType([
    StructField("address", StringType(), True),
    StructField("bytecode", StringType(), True),
    # 여러 개의 함수 시그니처 해시값이 배열 형태로 들어오므로 ArrayType 지정
    StructField("function_sighashes", ArrayType(StringType()), True),
    StructField("is_erc20", BooleanType(), True),
    StructField("is_erc721", BooleanType(), True),
    # 예시에서는 null이지만 실제 데이터가 들어올 것을 대비해 LongType 지정 (null 허용)
    StructField("block_number", LongType(), True)
])
