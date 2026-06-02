"""
Spark on K8s 연동 테스트용 PySpark 잡
- SparkSession 생성 → 간단한 DataFrame 연산 → 결과 출력
- GCS 연동까지 확인하려면 아래 GCS_TEST 블록 주석 해제
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = SparkSession.builder \
        .appName("spark-k8s-test") \
        .getOrCreate()

    print("=" * 60)
    print("✅ SparkSession 생성 성공!")
    print(f"   Spark Version : {spark.version}")
    print(f"   App Name      : {spark.sparkContext.appName}")
    print(f"   Master        : {spark.sparkContext.master}")
    print("=" * 60)

    # ── 간단한 DataFrame 연산 테스트 ──────────────────────────────────────
    data = [
        ("0x1234...abcd", "0x5678...efgh", 1.5,  "2026-06-01"),
        ("0x5678...efgh", "0x9abc...ijkl", 0.3,  "2026-06-01"),
        ("0x9abc...ijkl", "0x1234...abcd", 10.0, "2026-06-01"),
    ]
    columns = ["from_address", "to_address", "value_eth", "date"]

    df = spark.createDataFrame(data, columns)
    df.show()

    # 집계 테스트
    result = df.agg(
        F.count("*").alias("tx_count"),
        F.sum("value_eth").alias("total_eth"),
    ).collect()[0]

    print(f"📊 트랜잭션 수: {result['tx_count']}, 총 ETH: {result['total_eth']}")

    # ── (선택) GCS 연동 테스트 ────────────────────────────────────────────
    # 아래 주석을 해제하면 GCS 버킷에 실제로 쓰기/읽기를 테스트할 수 있음
    import os
    bucket = os.getenv("GCS_BUCKET_NAME", "your-bucket")
    test_path = f"gs://{bucket}/test/spark_k8s_test_output"
    df.write.mode("overwrite").parquet(test_path)
    print(f"✅ GCS 쓰기 성공: {test_path}")
    df_read = spark.read.parquet(test_path)
    df_read.show()
    print(f"✅ GCS 읽기 성공: {df_read.count()} rows")

    print("=" * 60)
    print("🎉 Spark on K8s 테스트 완료!")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
