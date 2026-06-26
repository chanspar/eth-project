#!/bin/bash
# ============================================================
# P (Producer Throughput per Partition) 벤치마크
# Kafka 컨테이너 내장 도구 kafka-producer-perf-test 사용
# ============================================================

set -e

TOPIC="bench-producer-test"
NUM_RECORDS=100000     # 10만 건 전송
RECORD_SIZE=500        # 500 bytes (이더리움 트랜잭션 JSON 평균 크기)
BOOTSTRAP="localhost:9092"

echo "============================================"
echo "  P (Producer Throughput) 벤치마크 시작"
echo "============================================"
echo ""

# 1. 기존 테스트 토픽 삭제 (있으면)
echo "🧹 기존 테스트 토픽 정리..."
docker exec kafka kafka-topics \
  --delete \
  --topic $TOPIC \
  --bootstrap-server $BOOTSTRAP 2>/dev/null || true

sleep 2

# 2. 파티션 1개짜리 테스트 토픽 생성
echo "📦 테스트 토픽 생성 (파티션 1개)..."
docker exec kafka kafka-topics \
  --create \
  --topic $TOPIC \
  --partitions 1 \
  --replication-factor 1 \
  --bootstrap-server $BOOTSTRAP

sleep 1

# 3. Producer 성능 테스트 실행
echo ""
echo "🚀 Producer 성능 테스트 실행 중... (${NUM_RECORDS}건, ${RECORD_SIZE}B/건)"
echo "   → throughput=-1 (속도 제한 없이 최대 속도로 전송)"
echo ""

docker exec kafka kafka-producer-perf-test \
  --topic $TOPIC \
  --num-records $NUM_RECORDS \
  --record-size $RECORD_SIZE \
  --throughput -1 \
  --producer-props bootstrap.servers=$BOOTSTRAP acks=1

echo ""
echo "============================================"
echo "  결과 해석 가이드"
echo "============================================"
echo "  • 'records/sec' → 이것이 P 값입니다!"
echo "  • 예: 50000 records/sec → P = 50,000"
echo "  • 공식: 파티션 수 ≥ T / P = 1500 / P"
echo "============================================"
echo ""

# 4. 정리
echo "🧹 테스트 토픽 삭제..."
docker exec kafka kafka-topics \
  --delete \
  --topic $TOPIC \
  --bootstrap-server $BOOTSTRAP 2>/dev/null || true

echo "✅ P 벤치마크 완료!"
