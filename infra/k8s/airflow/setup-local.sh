#!/bin/bash
set -euo pipefail 

# 스크립트 경로 기준 프로젝트 루트 디렉토리 탐색
PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="airflow"

# 레지스트리 변수
MASTER_IP="192.168.34.3" # 본인 마스터 IP
REGISTRY="${MASTER_IP}:5000"
IMAGE_NAME="eth-project/airflow-custom"
IMAGE_TAG="latest"


echo "🚀 Airflow on Kubernetes - 로컬 환경 셋업 시작"

# 1. Kubernetes Namespace 생성
echo "📁 Namespace '$NAMESPACE' 생성 중..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# 2. 커스텀 Airflow 도커 이미지 로컬 빌드 및 worker1 전송
echo "🔨 커스텀 Airflow 이미지 빌드 중..."
docker build -t ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} \
  -f "$PROJECT_ROOT/infra/k8s/airflow/Dockerfile" "$PROJECT_ROOT"

echo "📡 로컬 레지스트리로 Push 중..."
docker push ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}
echo "✅ 이미지 Push 완료!"


echo "🔍 워커 노드 라벨 검사 중 (role=airflow)..." 
if ! kubectl get nodes -l role=airflow | grep -q "Ready"; then 
  echo "❌ 'role=airflow' 라벨을 가진 노드를 찾을 수 없거나 노드가 Ready 상태가 아닙니다." 
  echo "👉 파드가 Pending 상태에 빠지는 것을 방지하기 위해 스크립트를 중단합니다." 
  echo "👉 실행 방법: kubectl label nodes <노드이름> role=airflow" 
  exit 1 
else 
  echo "✅ 적합한 워커 노드가 존재합니다." 
fi

# 3. [신규] 필수 Secrets 존재 여부 확인 및 생성 안내
echo "🔍 필수 Secrets 존재 여부 검사 중..."
REQUIRED_SECRETS=("eth-project-env" "airflow-ssh-git-secret" "airflow-api-secret" "airflow-jwt-secret")
MISSING_SECRET=0

for secret in "${REQUIRED_SECRETS[@]}"; do
  if ! kubectl get secret "$secret" -n $NAMESPACE >/dev/null 2>&1; then
    echo "❌ 필수 Secret '$secret'이(가) 존재하지 않습니다."
    MISSING_SECRET=1
  fi
done

if [ $MISSING_SECRET -eq 1 ]; then
  echo "⚠️  일부 필수 Secret이 없습니다. 먼저 'setup-secrets.sh' 스크립트를 실행하여 생성해 주세요."
  echo "👉 실행 방법: bash \$(dirname \"\$0\")/setup-secrets.sh"
  exit 1
else
  echo "✅ 모든 필수 Secrets이 존재합니다. 배포를 계속합니다."
fi

# 6. ✅ Helm 차트 배포 (force-update, --wait 추가)
echo "📦 Airflow Helm 차트 배포 중..."
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update apache-airflow

echo "⏳ Pod가 정상적으로 뜰 때까지 대기합니다 (최대 10분 소요)..."
helm upgrade --install airflow apache-airflow/airflow \
  --namespace $NAMESPACE \
  -f "$PROJECT_ROOT/infra/k8s/airflow/values-base.yaml" \
  -f "$PROJECT_ROOT/infra/k8s/airflow/values-local.yaml" \
  --timeout 10m

# 7. 상태 출력
echo ""
echo "⏳ Pod 상태 확인 (모든 Pod이 Running 상태가 될 때까지 약 2~3분 소요):"
kubectl get pods -n $NAMESPACE

echo ""
echo "===================================================================="
echo "✅ 배포 명령이 전송되었습니다!"
echo ""
echo "📌 1. Airflow 웹 UI 접속 (로컬 포트 포워딩 실행):"
echo "   kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow"
echo "   접속 주소: http://localhost:8080 (기본 ID: admin / PW: admin)"
echo ""
echo "📌 2. Pod 실시간 상태 감시:"
echo "   kubectl get pods -n $NAMESPACE -w"
echo "===================================================================="

# ```
# # 1. 헬름 삭제 (방금 안내해 드린 명령어)
# helm uninstall airflow --namespace airflow

# # 2. 내장 PostgreSQL이 사용하던 잔여 볼륨(저장소) 삭제
# kubectl delete pvc -l release=airflow -n airflow

# # 3. 이전에 수동으로 생성했던 .env 시크릿 및 구글 키 시크릿들 청소 (필요 시)
# kubectl delete secret eth-project-env gcp-credentials gcp-credentials-env -n airflow
# ```
