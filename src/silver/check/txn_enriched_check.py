import argparse
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType
from src.silver.spark_config import get_spark_session, get_logger, read_silver, read_bronze
from src.schema.silver_schema import enriched_transaction_schema
from src.schema.bronze_schema import transaction_schema

def run_kpi_check(spark: SparkSession, dt: str):
    logger = get_logger("Silver-KPI-Check")
    
    dt_partition = dt if "dt=" in dt else f"dt={dt}"
    logger.info(f"🔍 Silver 레이어 데이터 검증 시작 (날짜: {dt_partition})")
    
    try:
        # Silver 데이터 읽기
        df = read_silver(spark, "txn_enriched", dt_partition, schema=enriched_transaction_schema)
        silver_count = df.count()
        total_count = silver_count # 기존 로직 호환용
        
        # Bronze 원천 데이터 읽기 (전체 트랜잭션과 1:1 대응 확인)
        bronze_df = read_bronze(spark, "transactions", dt_partition, transaction_schema)
        bronze_count = bronze_df.count()
    except Exception as e:
        logger.error(f"❌ 데이터를 읽는 도중 에러가 발생했습니다: {e}")
        return

    # 🚨 [Critical Check] 데이터 유실 검증 (Bronze vs Silver)
    # 이제 모든 트랜잭션을 살렸으므로, 원천 데이터와 100% 일치해야 함
    retention_rate = (silver_count / bronze_count) if bronze_count > 0 else 0
    logger.info(f"📊 데이터 보존율 (Retention): {retention_rate*100:.2f}% ({silver_count:,} / {bronze_count:,})")

    if retention_rate < 0.99: # 1% 이상 유실되면 에러
        error_msg = f"Data Loss Detected! Retention: {retention_rate*100:.2f}% (Silver: {silver_count} / Bronze: {bronze_count})"
        logger.error(f"❌ {error_msg}")
        raise ValueError(error_msg)

    if silver_count == 0:
        logger.warning(f"⚠️ {dt_partition} 날짜에 데이터가 존재하지 않습니다.")
        return

    logger.info("=" * 60)
    logger.info(f"📊 [Ethereum KPI Report] 날짜: {dt_partition}")
    logger.info(f"📈 1. TX Count           : {silver_count:,}건")

    # 집계 연산
    aggs = df.select(
        F.avg(F.col("is_success").cast("int")).alias("success_rate"),
        F.avg(F.col("tx_fee_eth").cast(DoubleType())).alias("avg_fee"),
        F.sum(F.col("value_eth").cast(DoubleType())).alias("total_eth"),
        F.sum(F.col("is_contract_call").cast("int")).alias("call_count"),
        F.sum(F.col("is_contract_deploy").cast("int")).alias("deploy_count")
    ).collect()[0]

    # Active Addrs (from_address와 to_address의 unique 합집합)
    active_addrs = df.select(F.explode(F.array("from_address", "to_address"))).distinct().count()

    # EIP-1559 비율 (tx_type_label 기반)
    type_counts = df.groupBy("tx_type_label").count().collect()
    type_map = {row['tx_type_label']: row['count'] for row in type_counts}
    eip1559_count = type_map.get('eip1559', 0)
    eip1559_pct = (eip1559_count / total_count) * 100 if total_count > 0 else 0

    # 출력
    logger.info(f"✅ 2. Success %          : {aggs['success_rate']*100:.2f}%")
    logger.info(f"💰 3. Avg Fee ETH        : {aggs['avg_fee']:.8f} ETH")
    logger.info(f"💎 4. Total ETH          : {aggs['total_eth']:.4f} ETH")
    logger.info(f"🛠️ 5. Contract Call %     : {(aggs['call_count']/total_count)*100:.2f}% ({aggs['call_count']:,}건)")
    logger.info(f"🚀 6. Deploys            : {aggs['deploy_count']:,}건")
    logger.info(f"⚡ 7. EIP-1559 %         : {eip1559_pct:.2f}% ({eip1559_count:,}건)")
    logger.info(f"👥 8. Active Addrs       : {active_addrs:,}명")
    logger.info("=" * 60)

    # 🚨 [Critical Check] 자동 품질 검증 로직
    critical_errors = []
    
    # 1. 트랜잭션 성공률 검증 (평상시 90% 이상, 80% 미만은 심각한 조인/로직 에러 의심)
    if aggs['success_rate'] < 0.8:
        critical_errors.append(f"Low Success Rate: {aggs['success_rate']*100:.1f}%")

    # 2. 데이터 유실 검증 (최소 건수 미달)
    if total_count < 1000:
        critical_errors.append(f"Too Few Transactions: {total_count:,}건")

    # 3. 수수료 단위 변환 오류 검증 (평균 1 ETH는 현실적으로 불가능한 수치)
    if aggs['avg_fee'] > 1.0:
        critical_errors.append(f"Abnormal Avg Fee: {aggs['avg_fee']:.4f} ETH")

    if critical_errors:
        error_msg = " | ".join(critical_errors)
        logger.error(f"❌ [데이터 품질 검증 실패] {error_msg}")
        raise ValueError(f"Data Quality Gate Failed: {error_msg}")
    
    logger.info("✅ 모든 품질 기준을 통과했습니다.")

def main():
    """
    생성된 데이터를 바탕으로 KPI 리포트 출력 및 임계치 검증
    uv run python src/silver/check/txn_enriched_check.py --date 2026-05-01
    """
    parser = argparse.ArgumentParser(description="Ethereum Silver Layer KPI Checker")
    parser.add_argument("--date", required=True, help="검증할 날짜 (YYYY-MM-DD 또는 dt=YYYY-MM-DD)")
    args = parser.parse_args()

    spark = get_spark_session("KPI-Check")
    try:
        run_kpi_check(spark, args.date)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
