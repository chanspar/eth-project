#!/bin/bash
set -euo pipefail 

# 스크립트 경로 기준 프로젝트 루트 디렉토리 탐색
PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE_SPARK="spark"
NAMESPACE_OPERATOR="spark-operator"

# 레지스트리 및 이미지 변수 (VM1 로컬 레지스트리 활용)
MASTER_IP="192.168.34.3"
REGISTRY="${MASTER_IP}:5000"
IMAGE_NAME="eth-project/spark-custom"
IMAGE_TAG="3.4.0"

echo "🚀 Spark on Kubernetes - 로컬 환경 셋업 시작"

# 1. Kubernetes Namespaces 미리 생성
kubectl create namespace $NAMESPACE_SPARK --dry-run=client -o yaml | kubectl apply -f -
kubectl create namespace $NAMESPACE_OPERATOR --dry-run=client -o yaml | kubectl apply -f -

# 2. 커스텀 Spark 도커 이미지 로컬 빌드 및 Push
echo "🔨 커스텀 Spark 이미지 빌드 중..."
docker build -t ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} \
  -f "$PROJECT_ROOT/infra/k8s/spark/Dockerfile.spark" "$PROJECT_ROOT"

echo "📡 로컬 레지스트리로 Push 중..."
docker push ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}
echo "✅ 이미지 Push 완료!"

# 3. 워커 노드 라벨 검증 (Spark 드라이버/익스큐터 실행 타겟용)
echo "🔍 워커 노드 라벨 검사 중 (role=spark)..." 
if ! kubectl get nodes -l role=spark | grep -q "Ready"; then 
  echo "❌ 'role=spark' 라벨을 가진 노드를 찾을 수 없거나 노드가 Ready 상태가 아닙니다." 
  echo "👉 파드가 Pending 상태에 빠지는 것을 방지하기 위해 스크립트를 중단합니다." 
  echo "👉 실행 방법: kubectl label nodes <VM3-노드이름> role=spark" 
  exit 1 
else 
  echo "✅ 적합한 Spark 워커 노드가 존재합니다." 
fi

# 4. 필수 Secrets 존재 여부 확인 및 생성 안내
echo "🔍 필수 Secrets 존재 여부 검사 중..."
REQUIRED_SECRETS=("spark-env" "gcp-key")
MISSING_SECRET=0

for secret in "${REQUIRED_SECRETS[@]}"; do
  if ! kubectl get secret "$secret" -n $NAMESPACE_SPARK >/dev/null 2>&1; then
    echo "❌ 필수 Secret '$secret'이(가) 존재하지 않습니다."
    MISSING_SECRET=1
  fi
done

if [ $MISSING_SECRET -eq 1 ]; then
  echo "⚠️ 일부 필수 Secret이 없습니다. 먼저 'setup-secrets.sh' 스크립트를 실행하여 생성해 주세요."
  echo "👉 실행 방법: bash \$(dirname \"\$0\")/setup-secrets.sh"
  exit 1
else
  echo "✅ 모든 필수 Secrets이 존재합니다. 배포를 계속합니다."
fi

# 5. Helm 차트 배포 (--wait 포함)
echo "📦 Spark Operator Helm 차트 배포 중..."
helm repo add spark-operator https://kubeflow.github.io/spark-operator --force-update
helm repo update spark-operator

echo "⏳ Pod가 정상적으로 뜰 때까지 대기합니다 (최대 10분)..."
helm upgrade --install spark-operator spark-operator/spark-operator \
  --namespace $NAMESPACE_OPERATOR \
  --create-namespace \
  -f "$PROJECT_ROOT/infra/k8s/spark/spark-operator-values.yaml" \
  --timeout 10m

# 6. 상태 출력
echo ""
echo "⏳ Pod 상태 확인 (모든 Pod이 Running 상태가 될 때까지 감시):"
kubectl get pods -n $NAMESPACE_OPERATOR

echo ""
echo "===================================================================="
echo "✅ 배포 명령이 전송되었습니다!"
echo ""
echo "📌 1. VM3 노드의 insecure registry 설정 상태 및 containerd 재시작 확인 필수"
echo "📌 2. Spark 구동 테스트 실행:"
echo "   kubectl apply -f infra/k8s/spark/test/test-spark.yaml"
echo "   kubectl get sparkapplication -n spark"
echo "===================================================================="
