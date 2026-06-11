#!/bin/bash
set -euo pipefail 

# 스크립트 경로 기준 프로젝트 루트 디렉토리 탐색
PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="airflow"

# GKE/GCP 전용 변수 설정
# 아래 REPO_URL은 사용자 환경에 맞게 수정하세요.
# 예시: asia-northeast3-docker.pkg.dev/my-eth-project-498908/eth-repo
REPO_URL="asia-northeast3-docker.pkg.dev/my-eth-project-498908/eth-repo"
IMAGE_NAME="airflow-custom"
IMAGE_TAG="latest"
FULL_IMAGE="${REPO_URL}/${IMAGE_NAME}:${IMAGE_TAG}"

echo "🚀 Airflow on GKE - 프로덕션 환경 셋업 시작"

# 1. Kubernetes Namespace 생성
echo "📁 Namespace '$NAMESPACE' 생성 중..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# 2. 커스텀 Airflow 도커 이미지 로컬 빌드 및 GCP 전송
echo "🔨 커스텀 Airflow 이미지 빌드 중..."
docker build --platform linux/amd64 -t ${FULL_IMAGE} \
  -f "$PROJECT_ROOT/infra/k8s/airflow/Dockerfile" "$PROJECT_ROOT"

echo "📡 Google Artifact Registry로 Push 중..."
docker push ${FULL_IMAGE}
echo "✅ 이미지 Push 완료!"

# GKE Autopilot에서는 nodeSelector를 사용하지 않으므로, 라벨 검사 로직을 제거했습니다.

# 3. 필수 Secrets 존재 여부 확인 및 생성 안내
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
  echo "⚠️  일부 필수 Secret이 없습니다. 현재 kubectl 컨텍스트가 GKE를 바라보고 있는지 확인한 후, 'setup-secrets.sh' 스크립트를 실행하여 생성해 주세요."
  echo "👉 실행 방법: bash \$(dirname \"\$0\")/setup-secrets.sh"
  exit 1
else
  echo "✅ 모든 필수 Secrets이 존재합니다. 배포를 계속합니다."
fi

# 4. Helm 차트 배포 (force-update, --wait 추가)
echo "📦 Airflow Helm 차트 배포 중..."
helm repo add apache-airflow https://airflow.apache.org --force-update
helm repo update apache-airflow

echo "⏳ Pod가 정상적으로 뜰 때까지 대기합니다 (Autopilot 프로비저닝에 최대 10~15분 소요될 수 있습니다)..."
helm upgrade --install airflow apache-airflow/airflow \
  --namespace $NAMESPACE \
  -f "$PROJECT_ROOT/infra/k8s/airflow/values-base.yaml" \
  -f "$PROJECT_ROOT/infra/k8s/airflow/values-gke.yaml" \
  --timeout 15m

# 5. 상태 출력
echo ""
echo "⏳ Pod 상태 확인:"
kubectl get pods -n $NAMESPACE

echo ""
echo "===================================================================="
echo "✅ 배포 명령이 전송되었습니다!"
echo ""
echo "📌 1. Airflow 웹 UI 접속 (포트 포워딩):"
echo "   kubectl port-forward svc/airflow-api-server 8080:8080 -n airflow"
echo "   접속 주소: http://localhost:8080 (기본 ID: admin / PW: admin)"
echo ""
echo "📌 2. Pod 실시간 상태 감시:"
echo "   kubectl get pods -n $NAMESPACE -w"
echo "===================================================================="
