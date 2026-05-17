from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame, Window
from src.silver.spark_config import get_spark_session, read_silver
from src.schema.silver_schema import whale_txn_schema
from src.gold.utils import write_gold, write_gold_to_bq
from src.config import get_logger


# flow_type → 시장 신호 매핑
FLOW_SIGNAL_MAP = {
    "CEX_DEPOSIT":    "SELL_PRESSURE",    # 거래소 입금 → 매도 준비
    "CEX_WITHDRAWAL": "BUY_PRESSURE",     # 거래소 출금 → 셀프 커스터디 / 매수
    "DEX_TRADE":      "DEX_TRADE",
    "BRIDGE_MOVE":    "BRIDGE_MOVE",
    "PRIVATE_MOVE":   "PRIVATE_MOVE",
    "OTHER":          "OTHER_MOVE",
}


def build_signal_expr() -> F.Column:
    """flow_type → market_signal 컬럼 변환"""
    expr = None
    for flow, signal in FLOW_SIGNAL_MAP.items():
        cond = F.when(F.col("flow_type") == flow, signal)
        expr = cond if expr is None else expr.when(F.col("flow_type") == flow, signal)
    return expr.otherwise("UNKNOWN")


def build_market_flow_hourly(spark: SparkSession, dt: str) -> DataFrame:
    """
    Gold Layer: market_flow_hourly.py
    ==================================
    실버 레이어 whale_txns를 시간 단위로 집계하여 시장 압력 지수를 생성합니다.
    "지금 고래들이 거래소로 입금 중인가, 출금 중인가?"
    
    분석 단위: dt + hour + flow_type
    """
    logger = get_logger("Build Market Flow")
    logger.info(f"[{dt}] Market Flow Hourly 데이터 집계 파이프라인 시작")
    
    logger.info("Silver Layer에서 'whale_txns' 데이터 읽어오는 중...")
    df = read_silver(spark, "whale_txns", dt, schema=whale_txn_schema)
    
    date_str = dt.replace("dt=", "")
    df = df.withColumn("dt", F.to_date(F.lit(date_str)))
    df = df.withColumn("market_signal", build_signal_expr())
    
    logger.info("시간(hour) 및 flow_type 기준으로 1차 기본 집계 수행 중...")
    agg = (
        df.groupBy("dt", "hour", "flow_type", "market_signal")
        .agg(
            F.round(F.sum("value_eth"), 4).alias("total_eth"),
            F.count("hash").alias("tx_count"),
            F.count_distinct("from_address").alias("active_whale_count"),
            F.round(F.avg("value_eth"), 4).alias("avg_tx_eth"),
            F.round(F.max("value_eth"), 4).alias("max_single_tx_eth"),
            F.round(
                F.sum(F.col("is_private_transaction").cast("int")) / F.count("hash"),
                4,
            ).alias("private_tx_ratio"),
            F.sum(
                F.when(F.col("whale_tier") == "Humpback", 1).otherwise(0)
            ).alias("humpback_count"),

        )
    )
    
    logger.info("Window 함수를 활용하여 시간대별 흐름 점유율(Share Pct) 계산 중...")
    window_hour = Window.partitionBy("dt", "hour")
    agg = (
        agg
        .withColumn(
            "hour_total_eth",
            F.sum("total_eth").over(window_hour)
        ).withColumn(
            "flow_share_pct",
            F.round(F.col("total_eth") / F.col("hour_total_eth") * 100, 2)
        )
    )
    
    logger.info("거래소(CEX) 입출금 비율 기반 Market Pressure 지수 산출 중...")
    cex_pivot = (
        df.filter(F.col("flow_type").isin("CEX_DEPOSIT", "CEX_WITHDRAWAL"))
        .groupBy("dt", "hour")
        .agg(
            F.round(
                F.sum(F.when(F.col("flow_type") == "CEX_DEPOSIT", F.col("value_eth")).otherwise(0)), 4
            ).alias("cex_deposit_eth"),
            F.round(
                F.sum(F.when(F.col("flow_type") == "CEX_WITHDRAWAL", F.col("value_eth")).otherwise(0)), 4
            ).alias("cex_withdrawal_eth"),
        )
        .withColumn(
            # > 1 : 입금 우세 (매도 압력), < 1 : 출금 우세 (매수 압력)
            "deposit_withdrawal_ratio",
            F.round(
                F.col("cex_deposit_eth") / F.when(
                    F.col("cex_withdrawal_eth") == 0, F.lit(0.0001)
                ).otherwise(F.col("cex_withdrawal_eth")),
                4,
            ),
        )
        .withColumn(
            "pressure_label",
            F.when(F.col("deposit_withdrawal_ratio") > 1.5, "STRONG_SELL_PRESSURE")
            .when(F.col("deposit_withdrawal_ratio") > 1.0, "MILD_SELL_PRESSURE")
            .when(F.col("deposit_withdrawal_ratio") < 0.67, "STRONG_BUY_PRESSURE")
            .when(F.col("deposit_withdrawal_ratio") < 1.0, "MILD_BUY_PRESSURE")
            .otherwise("NEUTRAL"),
        )
    )
    
    logger.info("기본 집계 데이터와 Market Pressure 지수 병합(Left Join) 중...")
    final = agg.join(cex_pivot, on=["dt", "hour"], how="left")
    
    
    output_cols = [
        "dt",
        "hour",
        "flow_type",
        "market_signal",
        # 물량 지표
        "total_eth",
        "tx_count",
        "active_whale_count",
        "avg_tx_eth",
        "max_single_tx_eth",
        "humpback_count",
        # 비중 지표
        "flow_share_pct",
        "private_tx_ratio",
        # CEX 압력 지수 (flow_type이 CEX인 행에만 유효)
        "cex_deposit_eth",
        "cex_withdrawal_eth",
        "deposit_withdrawal_ratio",
        "pressure_label",
        "hour_total_eth",
    ]

    logger.info("Market Flow Hourly 데이터프레임 구성 완료!")
    return final.select(*output_cols)

def main():
    import argparse
    import time
    
    start_time = time.time()
    
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    logger = get_logger("Main")
    spark = get_spark_session("Gold: Market Flow Hourly")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"

    # 실행
    flow_df = build_market_flow_hourly(spark, dt_val)
    
    # 저장
    logger.info(f"Gold Layer에 데이터 저장 시작: {dt_val}")
    write_gold(flow_df, "market_flow_hourly")
    write_gold_to_bq(flow_df, "market_flow_hourly")
    logger.info("데이터 저장 완료!")
    
    flow_df.show(48, truncate=False) # 24시간 * flow_types
    
    
    duration = time.time() - start_time
    logger.info(f"✅ 전체 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")
    
    logger.info("Spark Session 종료")
    spark.stop()

if __name__ == "__main__":
    """uv run src/gold/transform/market_flow_hourly.py --date 2026-05-01"""
    main()
