#!/bin/bash
NAMESPACE_SPARK="spark"
NAMESPACE_OPERATOR="spark-operator"

echo "🧹 GKE Spark 환경 리소스 정리(Teardown) 시작..."

# 1. 실행 중인 SparkApplication 자원 먼저 삭제 (오퍼레이터가 살아있을 때 파이널라이저 정리 유도)
echo "⚡ 실행 중인 SparkApplication 제거 중..."
kubectl delete sparkapplication --all -n $NAMESPACE_SPARK --timeout=30s 2>/dev/null || echo "   - 제거할 SparkApplication이 없습니다."

# 2. Helm Release (Spark Operator) 삭제
echo "📦 Helm 차트 (spark-operator) 삭제 중..."
helm uninstall spark-operator -n $NAMESPACE_OPERATOR 2>/dev/null || echo "   - 설치된 Helm 차트가 없거나 이미 삭제되었습니다."

# ⚠️ GKE 버전에서는 Namespace를 지우지 않습니다!
# (Namespace를 지우면 GCP 키 등 세팅해 둔 시크릿이 다 날아갑니다)
echo "🛡️ 시크릿(비밀번호, 키) 유지를 위해 Namespace는 삭제하지 않습니다."

echo ""
echo "===================================================================="
echo "✅ GKE Spark 리소스가 깔끔하게 정리되어 할당량이 반환되었습니다!"
echo "📌 다시 배포하시려면 'bash infra/k8s/spark/setup-gke.sh'를 실행하세요."
echo "===================================================================="
