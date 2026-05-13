from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, DecimalType, BooleanType, DateType, DoubleType


# Silver 레이어: txn_enriched 테이블 스키마
enriched_transaction_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("block_timestamp", LongType(), True),
    StructField("transaction_index", IntegerType(), True),
    StructField("from_address", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("contract_address", StringType(), True),
    StructField("value_eth", DecimalType(38, 18), True),
    StructField("gas", LongType(), True),
    StructField("gas_used", DecimalType(38, 0), True),
    StructField("effective_gas_price", DecimalType(38, 0), True),
    StructField("tx_fee_eth", DecimalType(38, 18), True),
    StructField("miner", StringType(), True),
    StructField("transaction_type", IntegerType(), True),
    StructField("tx_type_label", StringType(), True),
    StructField("is_success", BooleanType(), True),
    StructField("is_contract_call", BooleanType(), True),
    StructField("is_contract_deploy", BooleanType(), True),
    StructField("input", StringType(), True),
    StructField("dt", DateType(), True)
])


# Silver 레이어: whale_txns 테이블 스키마
whale_txn_schema = StructType([
    StructField("hash", StringType(), True),
    StructField("block_number", LongType(), True),
    StructField("block_timestamp", LongType(), True),
    StructField("dt", DateType(), True),
    StructField("from_address", StringType(), True),
    StructField("from_label", StringType(), True),
    StructField("from_category", StringType(), True),
    StructField("to_address", StringType(), True),
    StructField("to_label", StringType(), True),
    StructField("to_category", StringType(), True),
    StructField("value_eth", DecimalType(38, 18), True),
    StructField("tx_fee_eth", DecimalType(38, 18), True),
    StructField("from_cumul_sent_eth", DoubleType(), True),
    StructField("from_cumul_tx_count", LongType(), True),
    StructField("to_cumul_recv_eth", DoubleType(), True),
    StructField("to_cumul_tx_count", LongType(), True),
    StructField("flag_cex_deposit", BooleanType(), True),
    StructField("flag_cex_withdrawal", BooleanType(), True),
    StructField("flag_cex_to_cex", BooleanType(), True),
    StructField("flag_dex_swap", BooleanType(), True),
    StructField("flag_high_freq_sender", BooleanType(), True),
    StructField("has_flag", BooleanType(), True)
])
