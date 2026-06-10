#!/bin/bash

# 스크립트 경로 기준 프로젝트 루트 디렉토리 탐색
PROJECT_ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
NAMESPACE="airflow"

# 레지스트리 변수
MASTER_IP="192.168.34.3"
REGISTRY="${MASTER_IP}:5000"
IMAGE_NAME="eth-project/airflow-custom"
IMAGE_TAG="latest"

echo "🧹 Airflow 로컬 환경 리소스 정리(Teardown) 시작..."

# 1. Helm Release (Airflow) 삭제
echo "📦 Helm 차트 (airflow) 삭제 중..."
helm uninstall airflow -n $NAMESPACE 2>/dev/null || echo "   - 설치된 Helm 차트가 없거나 이미 삭제되었습니다."

# 2. 잔여 PVC (Persistent Volume Claim) 삭제
# 내장 PostgreSQL DB의 이전 데이터가 다음 배포에 영향을 주지 않도록 볼륨을 날립니다.
echo "💾 잔여 볼륨(PVC) 삭제 중..."
kubectl delete pvc --all -n $NAMESPACE 2>/dev/null || echo "   - 삭제할 볼륨이 없습니다."

# 3. [선택] Namespace 및 Secret 삭제
# 주의: 네임스페이스를 날리면 공들여 만든 GCP 키, SSH 키 등 모든 시크릿이 함께 날아갑니다.
# 시크릿을 남겨두고 싶다면 이 부분을 주석 처리(#) 하세요.
echo "🗑️ Namespace ($NAMESPACE) 삭제 중 (약 1~2분 소요될 수 있습니다)..."
kubectl delete namespace $NAMESPACE 2>/dev/null || echo "   - 네임스페이스가 이미 삭제되었습니다."

# 4. [선택] 로컬 도커 이미지 삭제
# 디스크 용량 확보가 목적이라면 주석을 해제하세요.
# echo "🐳 로컬 Docker 이미지 정리 중..."
# docker rmi ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG} 2>/dev/null || echo "   - 삭제할 이미지가 없습니다."

echo ""
echo "===================================================================="
echo "✅ 리소스가 모두 깔끔하게 정리되었습니다!"
echo "📌 다시 배포하시려면 'setup.sh'를 실행하세요."
echo "===================================================================="
