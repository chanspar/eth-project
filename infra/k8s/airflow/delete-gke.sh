#!/bin/bash

NAMESPACE="airflow"

echo "🧹 GKE Airflow 환경 리소스 정리(Teardown) 시작..."

# 1. Helm Release 삭제 (가장 중요: CPU를 차지하고 있는 파드들을 날립니다)
echo "📦 Helm 차트 (airflow) 삭제 중..."
helm uninstall airflow -n $NAMESPACE 2>/dev/null || echo "   - 설치된 Helm 차트가 없거나 이미 삭제되었습니다."

# 2. 잔여 PVC 삭제 (DB 등)
echo "💾 잔여 볼륨(PVC) 삭제 중..."
kubectl delete pvc --all -n $NAMESPACE 2>/dev/null || echo "   - 삭제할 볼륨이 없습니다."

# ⚠️ GKE 버전에서는 Namespace를 지우지 않습니다!
# (Namespace를 지우면 GCP 키, Github SSH 키 등 시크릿이 다 날아가서 다시 세팅해야 합니다)
echo "🛡️ 시크릿(비밀번호, 키) 유지를 위해 Namespace는 삭제하지 않습니다."

echo ""
echo "===================================================================="
echo "✅ GKE 리소스가 깔끔하게 정리되어 CPU 할당량이 반환되었습니다!"
echo "📌 다시 배포하시려면 'bash infra/k8s/airflow/setup-gke.sh'를 실행하세요."
echo "===================================================================="
