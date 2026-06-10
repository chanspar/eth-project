#!/bin/bash
set -euo pipefail 

# 스크립트 경로 기준 프로젝트 루트 디렉토리 탐색
PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE_SPARK="spark"

echo "🔐 Spark on Kubernetes - Secrets 및 RBAC 적용 시작"

# 1. Kubernetes Namespace 생성
echo "📁 Namespace '$NAMESPACE_SPARK' 생성 중..."
kubectl create namespace $NAMESPACE_SPARK --dry-run=client -o yaml | kubectl apply -f -

# 2. 로컬 .env 파일을 기반으로 환경변수 Secret 생성
echo "🔐 공통 환경변수 Secret(spark-env) 생성 중..."
if [ -f "$PROJECT_ROOT/.env" ]; then
  kubectl create secret generic spark-env \
    --namespace $NAMESPACE_SPARK \
    --from-env-file="$PROJECT_ROOT/.env" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "✅ spark-env 생성 완료!"
else
  echo "⚠️  경고: 루트 경로에 .env 파일이 없습니다. 기본 환경 변수로 계속 진행합니다."
fi

# 3. GCP 인증용 gcp-key.json Secret 생성
echo "🔑 GCP Credentials Secret(gcp-key) 생성 중..."
if [ -f "$PROJECT_ROOT/gcp-key.json" ]; then
  kubectl create secret generic gcp-key \
    --namespace $NAMESPACE_SPARK \
    --from-file=gcp-key.json="$PROJECT_ROOT/gcp-key.json" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "✅ gcp-key 생성 완료!"
else
  echo "⚠️  경고: 루트 경로에 gcp-key.json 파일이 없습니다. GCP 연결이 필요한 Spark 잡 실행 시 실패할 수 있습니다."
fi

# 4. 작성해 두신 RBAC YAML 설정들 일괄 적용
echo "🛡️ 작성된 RBAC YAML 설정 적용 중..."
RBAC_DIR="$PROJECT_ROOT/infra/k8s/spark/rbac"

if [ -f "$RBAC_DIR/spark-rbac.yaml" ]; then
  kubectl apply -f "$RBAC_DIR/spark-rbac.yaml"
  echo "✅ spark-rbac.yaml 적용 완료!"
else
  echo "❌ 에러: $RBAC_DIR/spark-rbac.yaml 파일을 찾을 수 없습니다."
  exit 1
fi

if [ -f "$RBAC_DIR/airflow-spark-rbac.yaml" ]; then
  kubectl apply -f "$RBAC_DIR/airflow-spark-rbac.yaml"
  echo "✅ airflow-spark-rbac.yaml 적용 완료!"
else
  echo "❌ 에러: $RBAC_DIR/airflow-spark-rbac.yaml 파일을 찾을 수 없습니다."
  exit 1
fi

echo "✅ 모든 Secrets 및 RBAC 설정 완료!"
