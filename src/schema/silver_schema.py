from pyspark.sql.types import StructType, StructField, StringType, LongType, DecimalType, BooleanType, DateType, DoubleType


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
    StructField("dt", DateType(), True),
    StructField("from_address", StringType(), True),
    StructField("from_label", StringType(), True),
    StructField("from_category", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("to_label", StringType(), True),
    StructField("to_category", StringType(), True),
    StructField("value_eth", DecimalType(38, 18), True),
    StructField("from_cumul_sent_eth", DecimalType(38, 18), True),
    StructField("from_cumul_tx_count", LongType(), True),
    StructField("to_cumul_recv_eth", DecimalType(38, 18), True),
    StructField("to_cumul_tx_count", LongType(), True),
    StructField("flag_cex_deposit", BooleanType(), True),
    StructField("flag_cex_withdrawal", BooleanType(), True),
    StructField("flag_cex_to_cex", BooleanType(), True),
    StructField("flag_dex_swap", BooleanType(), True),
    StructField("flag_high_freq_sender", BooleanType(), True),
    StructField("has_flag", BooleanType(), True)
])


# Silver 레이어: token_flow 테이블 스키마
token_flow_schema = StructType([
    StructField("transaction_hash", StringType(), True),
    StructField("block_timestamp", LongType(), True),
    StructField("dt", DateType(), True),
    StructField("token_address", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("value_normalized", DoubleType(), True),
    StructField("from_dex", StringType(), True),
    StructField("to_dex", StringType(), True)
])
