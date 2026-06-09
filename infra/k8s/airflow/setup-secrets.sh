#!/bin/bash
set -euo pipefail 

# 스크립트 경로 기준 프로젝트 루트 디렉토리 탐색
PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="airflow"

echo "🔐 Airflow on Kubernetes - Secrets 생성 시작"

# 1. Kubernetes Namespace 생성
echo "📁 Namespace '$NAMESPACE' 생성 중..."
kubectl create namespace $NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

# 2. 로컬 .env 파일을 기반으로 환경변수 Secret 생성
echo "🔐 공통 환경변수 Secret(eth-project-env) 생성 중..."
if [ -f "$PROJECT_ROOT/.env" ]; then
  kubectl create secret generic eth-project-env \
    --namespace $NAMESPACE \
    --from-env-file="$PROJECT_ROOT/.env" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "✅ eth-project-env 생성 완료!"
else
  echo "⚠️  경고: 루트 경로에 .env 파일이 없습니다. 기본 환경 변수로 계속 진행합니다."
fi

# 3. GCP 인증용 gcp-key.json Secret 생성
echo "🔑 GCP Credentials Secret 생성 중..."
if [ -f "$PROJECT_ROOT/gcp-key.json" ]; then
  kubectl create secret generic gcp-credentials \
    --namespace $NAMESPACE \
    --from-file=gcp-key.json="$PROJECT_ROOT/gcp-key.json" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "✅ GCP Credentials Secret 생성 완료!"
else
  echo "⚠️  경고: 루트 경로에 gcp-key.json 파일이 없습니다. GCP 연결이 필요한 DAG 실행 시 실패할 수 있습니다."
fi

# GOOGLE_APPLICATION_CREDENTIALS 환경 변수를 K8s 마운트 경로로 지정하는 Secret 생성
kubectl create secret generic gcp-credentials-env \
  --namespace $NAMESPACE \
  --from-literal=GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/gcp-key.json \
  --dry-run=client -o yaml | kubectl apply -f -

# 4. Git SSH 인증용 Secret 생성 (DAG 동기화 등 목적)
echo "🔑 Git SSH 인증용 Secret 생성 중..."
SSH_KEY_PATH="$PROJECT_ROOT/id_rsa" 

if [ -f "$SSH_KEY_PATH" ]; then
  kubectl create secret generic airflow-ssh-git-secret \
    --namespace $NAMESPACE \
    --from-file=gitSshKey="$SSH_KEY_PATH" \
    --dry-run=client -o yaml | kubectl apply -f -
  echo "✅ Git SSH Secret 생성 완료!"
else
  echo "⚠️  경고: $SSH_KEY_PATH 경로에 id_rsa 파일이 없습니다. Git 동기화(gitSync) 설정 시 실패할 수 있습니다."
fi

# 5. 보안 Secret (API Secret Key, JWT Secret) 자동 생성
# 이미 존재할 경우 덮어쓰지 않고 유지하여 로그인 풀림 방지
echo "🔐 보안 비밀키(API/JWT) 자동 생성 및 체크 중..."

if ! kubectl get secret airflow-api-secret -n $NAMESPACE >/dev/null 2>&1; then
  echo "🔑 신규 API Secret Key 생성 중..."
  API_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
  kubectl create secret generic airflow-api-secret \
    --namespace $NAMESPACE \
    --from-literal=api-secret-key="$API_KEY"
else
  echo "✅ 기존 API Secret Key가 이미 존재하여 그대로 유지합니다."
fi

if ! kubectl get secret airflow-jwt-secret -n $NAMESPACE >/dev/null 2>&1; then
  echo "🔑 신규 JWT Secret 생성 중..."
  JWT_KEY=$(python3 -c 'import secrets; print(secrets.token_hex(16))')
  kubectl create secret generic airflow-jwt-secret \
    --namespace $NAMESPACE \
    --from-literal=jwt-secret="$JWT_KEY"
else
  echo "✅ 기존 JWT Secret이 이미 존재하여 그대로 유지합니다."
fi

echo "✅ 모든 Secrets 생성 완료!"
