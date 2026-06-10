"""
Silver 3종 품질 체크 통합 잡 (Spark on K8s 전용)
- SparkKubernetesOperator에서 mainApplicationFile로 직접 실행
- txn_enriched_check + token_flow_check + whale_txns_check 를 하나의 Spark 세션에서 수행
- 하나라도 실패하면 즉시 예외를 발생시켜 Airflow에 실패를 전파
"""
import argparse
import sys
import time


def main():
    parser = argparse.ArgumentParser(description="Silver Layer 3종 품질 체크 (K8s 통합 잡)")
    parser.add_argument("--date", required=True, help="검증할 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()

    # sys.path를 추가하여 src 모듈 import 가능하도록 설정
    # K8s에서는 git-sync initContainer가 /opt/spark/work-dir 에 코드를 clone
    sys.path.insert(0, "/opt/spark/work-dir")

    from src.silver.spark_config import get_spark_session, get_logger
    from src.silver.check.txn_enriched_check import run_kpi_check
    from src.silver.check.token_flow_check import run_token_flow_kpi_check
    from src.silver.check.whale_txns_check import run_whale_kpi_check

    logger = get_logger("Silver-Quality-Check-K8s")
    spark = get_spark_session("K8s-Silver-Quality-Check")

    start_time = time.time()
    errors = []

    try:
        # 1. 트랜잭션 유실 체크 (Bronze vs Silver 99% Retention)
        logger.info(f"🔍 [1/3] txn_enriched 품질 체크 시작 ({args.date})")
        try:
            run_kpi_check(spark, args.date)
            logger.info("✅ [1/3] txn_enriched 품질 체크 통과")
        except Exception as e:
            errors.append(f"txn_enriched: {e}")
            logger.error(f"❌ [1/3] txn_enriched 품질 체크 실패: {e}")

        # 2. 토큰 전송 유실 체크
        logger.info(f"🔍 [2/3] token_flow 품질 체크 시작 ({args.date})")
        try:
            run_token_flow_kpi_check(spark, args.date)
            logger.info("✅ [2/3] token_flow 품질 체크 통과")
        except Exception as e:
            errors.append(f"token_flow: {e}")
            logger.error(f"❌ [2/3] token_flow 품질 체크 실패: {e}")

        # 3. 고래 트랜잭션 품질 체크
        logger.info(f"🔍 [3/3] whale_txns 품질 체크 시작 ({args.date})")
        try:
            run_whale_kpi_check(spark, args.date)
            logger.info("✅ [3/3] whale_txns 품질 체크 통과")
        except Exception as e:
            errors.append(f"whale_txns: {e}")
            logger.error(f"❌ [3/3] whale_txns 품질 체크 실패: {e}")

    finally:
        spark.stop()
        duration = time.time() - start_time
        logger.info(f"⏱️ 품질 체크 총 소요 시간: {int(duration // 60)}분 {duration % 60:.2f}초")

    # 하나라도 실패했으면 예외 발생 → Airflow 태스크 FAILED 처리
    if errors:
        error_summary = " | ".join(errors)
        raise ValueError(f"❌ Silver Quality Check Failed ({len(errors)}/3): {error_summary}")

    logger.info(f"✅ Silver 3종 품질 체크 모두 통과 ({args.date})")


if __name__ == "__main__":
    main()
