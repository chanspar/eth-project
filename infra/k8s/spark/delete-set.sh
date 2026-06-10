#!/bin/bash
NAMESPACE_SPARK="spark"
NAMESPACE_OPERATOR="spark-operator"

echo "🧹 Spark 로컬 환경 리소스 정리(Teardown) 시작..."

# 1. 실행 중인 SparkApplication 자원 먼저 삭제 (오퍼레이터가 살아있을 때 파이널라이저 정리 유도)
echo "⚡ 실행 중인 SparkApplication 제거 중..."
kubectl delete sparkapplication --all -n $NAMESPACE_SPARK --timeout=30s 2>/dev/null || echo "   - 제거할 SparkApplication이 없습니다."

# 2. Helm Release (Spark Operator) 삭제
echo "📦 Helm 차트 (spark-operator) 삭제 중..."
helm uninstall spark-operator -n $NAMESPACE_OPERATOR 2>/dev/null || echo "   - 설치된 Helm 차트가 없거나 이미 삭제되었습니다."

# 3. Namespace 삭제 (비동기 처리로 터미널 블로킹 방지)
echo "🗑️ Namespace ($NAMESPACE_SPARK, $NAMESPACE_OPERATOR) 삭제 요청 전송..."
kubectl delete namespace $NAMESPACE_SPARK --wait=false 2>/dev/null
kubectl delete namespace $NAMESPACE_OPERATOR --wait=false 2>/dev/null

echo ""
echo "===================================================================="
echo "✅ 리소스 정리 요청이 완료되었습니다 (네임스페이스는 백그라운드에서 삭제됩니다)!"
echo "📌 상태 확인: kubectl get ns"
echo "📌 다시 배포하시려면 'setup-local.sh'를 실행하세요."
echo "===================================================================="
