from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from src.silver.spark_config import read_bronze, get_spark_session, get_logger
from src.silver.utils import write_silver
from src.schema.bronze_schema import token_transfer_schema, block_schema, receipt_schema
from src.silver.known_labels import load_token_meta_df, load_address_labels


def build_token_flow(spark: SparkSession, dt: str):
    logger = get_logger("Build Token Flow")

    logger.info(f"[build_token_flow] - [{dt}] 데이터 로드 시작")

    # 1. 소스 데이터 로드 (contracts는 제외)
    transfers = read_bronze(spark, "token_transfers", dt, token_transfer_schema)
    blocks = read_bronze(spark, "blocks", dt, block_schema)
    receipts = read_bronze(spark, "receipts", dt, receipt_schema)
    
    # 2. 마스터 데이터(Dimension) 로드
    token_meta = load_token_meta_df(spark)
    address_labels = load_address_labels(spark)

    # 데이터 가공 및 조인
    blocks_slim = blocks.select(
        F.col("number").alias("block_number"),
        F.col("timestamp").alias("block_timestamp")
    )

    receipts_slim = receipts.select(
        F.col("transaction_hash"),
        F.col("status")
    )

    # 토큰 정보 조인 (Inner Join: 우리가 관리하는 주요 토큰들만 분석 대상)
    # decimals가 있어야 정확한 수량 계산이 가능하므로 필수 조인
    flow = transfers.join(
        F.broadcast(token_meta),
        on="token_address",
        how="inner"
    )

    # 블록 시간 조인
    flow = flow.join(F.broadcast(blocks_slim), on="block_number", how="left")

    # 영수증(Receipts) 조인 및 성공한 트랜잭션만 필터링
    flow = flow.join(
        receipts_slim,
        on="transaction_hash",
        how="inner"
    ).filter(F.col("status") == 1)

    # 기초 인텔리전스 (시간, 수량 계산)
    flow = (
        flow
        .withColumn("hour", F.hour(F.from_unixtime("block_timestamp").cast("timestamp")))
        .withColumn("dt", F.to_date(F.from_unixtime("block_timestamp")))
        .withColumn(
            "amount",
            (F.col("value") / F.pow(F.lit(10.0), F.col("decimals").cast(DoubleType()))).cast(DoubleType())
        )
    )

    # 4. 주소 라벨링 (보낸 쪽/받는 쪽)
    # 주소 라벨 데이터셋이 매우 작으므로(약 440KB) Broadcast Join을 적용하여 불필요한 셔플 방지
    labels_bc = F.broadcast(address_labels)

    # 발신자 라벨
    flow = flow.join(
        labels_bc.selectExpr("address as from_address", "label_name as from_label", "label_category as from_category"),
        on="from_address", how="left"
    ).fillna({"from_label": "Unknown", "from_category": "Unknown"})

    # 수신자 라벨
    flow = flow.join(
        labels_bc.selectExpr("address as to_address", "label_name as to_label", "label_category as to_category"),
        on="to_address", how="left"
    ).fillna({"to_label": "Unknown", "to_category": "Unknown"})

    # 5. 최종 컬럼 선택
    final_cols = [
        "transaction_hash", "status", "block_timestamp", "hour", "dt",
        "token_address", "symbol", "token_name",
        "from_address", "from_label", "from_category",
        "to_address", "to_label", "to_category",
        "amount"
    ]

    return flow.select(*final_cols)

def main():
    import argparse
    import time

    logger = get_logger("Token_Flow_Pipeline")
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_spark_session("Token Flow Silver")
    dt_val = args.date if "dt=" in args.date else f"dt={args.date}"
    start_time = time.time()

    try:
        logger.info(f"🚀 Token Flow 처리 시작: {args.date}")
        df = build_token_flow(spark, dt_val)
        
        # [성능 최적화] 중복 연산을 방지하기 위해 데이터프레임을 메모리에 캐싱
        df.cache()
        
        # 데이터 확인용 (상위 5건 출력)
        print(f"\n📊 [{args.date}] 처리 데이터 샘플 (상위 5건):")
        df.show(5, truncate=False)

        write_silver(df, "token_flow")
        
        # 메모리 확보를 위해 캐시 해제
        df.unpersist()
        
        logger.info(f"✅ [{args.date}] 처리 및 저장 완료")

    except Exception as e:
        logger.exception(f"❌ 오류 발생: {e}")
    finally:
        spark.stop()
        duration = time.time() - start_time
        logger.info(f"✅ 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")

if __name__ == "__main__":
    """uv run src/silver/transform/token_flow.py --date 2026-05-01"""
    main()
