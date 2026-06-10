from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, BooleanType, DateType, DoubleType, IntegerType


# Silver 레이어: txn_enriched 테이블 스키마
enriched_transaction_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("block_timestamp", LongType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("value_eth", DecimalType(38, 18), True),
    StructField("is_success", BooleanType(), True),
    StructField("dt", DateType(), True)
])


# Silver 레이어: whale_txns 테이블 스키마
whale_txn_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("block_timestamp", LongType(), True),
    StructField("hour", IntegerType(), True),
    StructField("dt", DateType(), True),
    StructField("from_address", StringType(), True),
    StructField("from_label", StringType(), True),
    StructField("from_entity", StringType(), True),
    StructField("from_category", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("to_label", StringType(), True),
    StructField("to_entity", StringType(), True),
    StructField("to_category", StringType(), True),
    StructField("value_eth", DecimalType(38, 18), True),
    StructField("cumul_sent_eth", DecimalType(38, 18), True),
    StructField("cumul_tx_count", LongType(), True),
    StructField("cumul_recv_eth", DecimalType(38, 18), True),
    StructField("whale_tier", StringType(), True),
    StructField("flow_type", StringType(), True),
    StructField("is_private_transaction", BooleanType(), True),
    StructField("flag_high_freq", BooleanType(), True)
])


# Silver 레이어: token_flow 테이블 스키마
token_flow_schema = StructType([
    StructField("transaction_hash", StringType(), True),
    StructField("block_timestamp", LongType(), True),
    StructField("hour", IntegerType(), True),
    StructField("dt", DateType(), True),
    StructField("token_address", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("token_name", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("from_label", StringType(), True),
    StructField("from_category", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("to_label", StringType(), True),
    StructField("to_category", StringType(), True),
    StructField("amount", DoubleType(), True),
])
