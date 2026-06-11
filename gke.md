# Ethereum Data Pipeline: GKE Migration Guide

이 문서는 로컬 환경(VirtualBox)에서 구글 클라우드의 GKE(Google Kubernetes Engine) 환경으로 파이프라인을 마이그레이션하기 위한 가이드라인입니다.

아래 명령어들을 순서대로 터미널에 복사/붙여넣기 하시면, 인프라 생성부터 컨테이너 배포까지 손쉽게 진행할 수 있습니다.

---

## 0단계: 사전 준비물 (Prerequisites)

K8s 배포 및 시크릿(Secret) 생성을 위해, 스크립트를 실행하는 마스터 노드(터미널)의 현재 디렉터리에 반드시 아래 파일들이 준비되어 있어야 합니다.

1. **`gcp-key.json`**: 구글 클라우드(BigQuery, GCS 등) 접근을 위한 서비스 어카운트 키 파일
2. **`.env`**: DB 비밀번호 등 환경변수가 담긴 파일
3. **`id_rsa` (또는 기타 SSH Private Key)**: Airflow가 Github 레포지토리에서 코드를 긁어올 때(Git-sync) 사용할 비밀키

위 파일들이 존재하는 위치(프로젝트 루트)에서 아래 단계들을 진행해 주세요.

---

## 0.5단계: gcloud CLI 설치 및 인증 (필수)

구글 클라우드에 명령을 내리기 위해 `gcloud` 도구가 필요합니다. 실행하시는 환경에 맞게 설치해 주세요.

**[옵션 A] 우분투(Ubuntu) 리눅스 환경 (공식 apt-get 방식)**
```bash
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates gnupg curl

curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

sudo apt-get update && sudo apt-get install -y google-cloud-cli

# 설치 후 로그인
gcloud auth login
```

**[옵션 B] 윈도우(Windows) 로컬 환경**
1. [구글 클라우드 SDK 윈도우 설치 파일(exe)](https://dl.google.com/dl/cloudsdk/channels/rapid/GoogleCloudSDKInstaller.exe) 다운로드 및 설치
2. 설치 완료 후 열리는 터미널 창(또는 PowerShell)에서 자동 팝업되는 브라우저를 통해 구글 로그인 진행

로그인이 완료되면 터미널(PowerShell)에 아래 명령어들을 차례로 입력하여 기본 프로젝트 설정 및 GKE 인증 플러그인을 설치합니다.
```bash
# 기본 프로젝트 설정
gcloud config set project my-eth-project-498908

# GKE 인증 플러그인 설치 (K8s v1.26 이상 필수)
gcloud components install gke-gcloud-auth-plugin

# 리눅스는 이거
sudo apt-get install google-cloud-cli-gke-gcloud-auth-plugin
```

[Google Cloud CLI 설치 공식 문서](https://docs.cloud.google.com/sdk/docs/install-sdk?hl=ko)

---

## 1단계: GCP 접속 및 인프라 생성

인프라 생성은 터미널에서 명령어(`gcloud`)를 치는 방법과, 구글 클라우드 웹 콘솔(UI)에서 마우스로 클릭하는 방법 두 가지가 있습니다.

### 💻 방법 A. 터미널 명령어로 생성하기 (가장 빠름)
아래 명령어로 GKE Autopilot 클러스터와 Google Artifact Registry(GAR)를 한 번에 생성합니다. (기본 리전: 서울 `asia-northeast3`)

```bash
# 1. GKE Autopilot 클러스터 생성 (약 5~10분 소요)
gcloud container clusters create-auto eth-cluster --region=asia-northeast3

# 2. 로컬 kubectl을 방금 만든 GKE 클러스터에 연결
gcloud container clusters get-credentials eth-cluster --region=asia-northeast3

# 3. GAR 저장소 (eth-repo) 생성
gcloud artifacts repositories create eth-repo \
  --repository-format=docker \
  --location=asia-northeast3 \
  --description="Docker repository for ETH project"

# 4. 내 로컬 도커가 GAR에 Push 할 수 있도록 권한 부여
gcloud auth configure-docker asia-northeast3-docker.pkg.dev
```

### 🖱️ 방법 B. 구글 클라우드 웹 콘솔(UI)에서 생성하기 (초보자 추천)
명령어가 부담스럽다면 마우스 클릭만으로도 완벽하게 똑같이 생성할 수 있습니다!

#### 1 & 2. GKE Autopilot 클러스터 생성 및 연결
참고: UI에서 클러스터를 생성한 후, 로컬 터미널과 연결(kubectl)하기 위한 인증 명령어를 UI가 직접 화면에 띄워줍니다.

- Google Cloud 콘솔 상단 검색창에 Kubernetes Engine을 검색하거나, 왼쪽 메뉴에서 Kubernetes Engine > 클러스터(Clusters)로 이동합니다.

- 상단의 [➕ 만들기 (CREATE)] 버튼을 클릭합니다.

- Autopilot(콘솔에서 권장함) 섹션에서 [구성 (CONFIGURE)]을 클릭합니다.

- 클러스터 이름(`eth-cluster`)을 입력하고, 리전을 `asia-northeast3` (서울)로 선택한 뒤 아래 [만들기 (CREATE)]를 누릅니다. (5~10분 소요)

- 생성이 완료되면 클러스터 목록 오른쪽에 있는 점 3개 버튼(작업 더보기)을 누르고 [연결 (Connect)]을 클릭합니다.

- 화면에 `gcloud container clusters get-credentials...` 명령어가 팝업으로 나타나며, 옆의 복사 버튼을 눌러 로컬 터미널에 붙여넣기 하시면 2번 과정이 완료됩니다.
#### 3. Artifact Registry (GAR) 저장소 생성
- 콘솔 상단 검색창에 Artifact Registry를 검색하여 이동합니다.

- 상단의 [➕ 저장소 만들기 (CREATE REPOSITORY)] 버튼을 클릭합니다.

- 설정을 다음과 같이 입력합니다:

  - 이름: eth-repo

  - 형식: Docker

  - 모드: 표준 (Standard)

  - 위치 유형: 리전 (Region) -> asia-northeast3 (서울)

- 하단의 [만들기 (CREATE)]를 누르면 저장소가 즉시 생성됩니다.

#### 4. 로컬 Docker 인증 구성 (gcloud auth)
> ⚠️ 주의: 이 부분은 '내 로컬 PC의 Docker 프로그램'에 Google Cloud 접근 권한을 심어주는 단계이기 때문에, 최종 인증 명령은 로컬 터미널에서 실행하셔야 합니다. 대신 명령어 작성을 UI가 도와줍니다.

- 방금 생성한 Artifact Registry 페이지로 이동하여 eth-repo 저장소 이름을 클릭합니다.

- 최상단에 있는 [⚙️ 설정 안내 (SETUP INSTRUCTIONS)] 버튼을 클릭합니다.

- 팝업창이 뜨면서 로컬 터미널에 입력해야 하는 인증 명령어(gcloud auth configure-docker asia-northeast3-docker.pkg.dev)를 그대로 보여줍니다. 이를 복사해서 로컬 터미널에 한 번만 실행해 주시면 됩니다.

---

## 2단계: Github 코드 푸시 및 K8s 시크릿 생성

### 2-1. Github 코드 푸시
Airflow는 Git-sync를 통해 K8s 내부에서 코드를 자동으로 당겨갑니다. 따라서, 배포를 진행하기 전에 새롭게 생성한 GKE 전용 DAG 파일들을 반드시 Github에 반영해야 합니다.

```bash
git add .
git commit -m "feat: setup GKE migration files"
git push origin main
```

### 2-2. K8s 시크릿 배포
GCP 및 Github 통신 등에 필요한 비밀번호와 키 파일들을 GKE 클러스터에 등록합니다. 기존 로컬 셋업 시 사용했던 시크릿 생성 스크립트를 재사용합니다.

```bash
bash infra/k8s/airflow/setup-secrets.sh
bash infra/k8s/spark/setup-secrets.sh
```

---

## 3단계: GKE 전용 셋업 스크립트 실행

GKE 배포 전용으로 작성된 스크립트를 실행합니다. 
해당 스크립트는 **커스텀 이미지 빌드 ➔ GAR 저장소 Push ➔ GKE Helm 배포** 과정을 한 번에 수행합니다.

```bash
# Airflow 배포 (약 10~15분 소요될 수 있음)
bash infra/k8s/airflow/setup-gke.sh

# Spark Operator 배포
bash infra/k8s/spark/setup-gke.sh
```

---

## 4단계: 접속 및 모니터링

배포가 성공적으로 완료되면, Airflow 3의 `apiServer`가 LoadBalancer를 통해 공인 IP(External IP)를 발급받습니다. 아래 명령어로 접속할 IP를 확인하세요.

```bash
# Airflow 웹 UI 공인 IP 확인
kubectl get svc airflow-api-server -n airflow
```
👉 `EXTERNAL-IP` 란에 나오는 주소(예: `http://34.123.45.67:8080`)로 팀원들과 함께 바로 접속하실 수 있습니다! (기본 ID: admin / PW: admin)
※ 만약 `EXTERNAL-IP`가 `<pending>`으로 나온다면 구글이 IP를 할당 중인 것이므로 1~2분 뒤에 다시 확인해 주세요.

파드(Pod)들의 실시간 상태가 궁금하다면 다음 명령어를 사용하세요:
```bash
# Airflow 파드 상태 확인
kubectl get pods -n airflow -w

# Spark 파드 상태 확인
kubectl get pods -n spark-operator -w
```

> **문제 해결 (Troubleshooting)**
> GCP 클러스터를 띄우는 중 권한 오류나 리소스 부족 관련 에러가 발생한다면, 에러 로그를 확인하여 GCP 콘솔에서 API 권한 및 결제 계정 상태를 체크해 주세요.

---

## Appendix A: 쿠버네티스 Context(주파수) 변경 가이드

명령어(`kubectl`, `helm`)는 단지 '리모컨'일 뿐이며, 내 주소록 파일(`.kube/config`)에 맞춰진 타겟을 향해 명령을 보냅니다.
하나의 터미널(가상머신 등)에서 로컬 환경과 구글 클라우드 환경을 자유자재로 오가며 조종하고 싶을 때 아래 명령어들을 사용하세요.

1. **현재 등록된 주파수(클러스터) 목록 보기**
   ```bash
   kubectl config get-contexts
   ```
   *목록 중 `*` 기호가 붙어 있는 곳이 현재 명령어가 날아가는 타겟입니다.*

2. **주파수 타겟 변경하기**
   ```bash
   # 로컬 가상머신(Minikube)으로 돌아가기
   kubectl config use-context minikube
   
   # 다시 구글 클라우드(GKE)로 변경하기
   kubectl config use-context gke_my-eth-project-498908_asia-northeast3_eth-cluster
   ```

---

## Appendix B: GKE 리소스 싹쓸이 (Teardown) 가이드

GKE 클러스터에 배포된 파드들이 리소스 한도(Quota)를 초과하여 스케줄링 무한 대기(Deadlock)에 빠지거나, 환경을 초기화하고 싶을 때 사용하는 방법입니다.

GKE에서는 **네임스페이스(Namespace)를 삭제하면 안 됩니다!** (GCP 키나 SSH 키 등 시크릿이 함께 삭제되기 때문입니다). 따라서 안전하게 Helm 차트와 잔여 볼륨만 날려주는 전용 스크립트를 사용해야 합니다.

1. **꽉 막힌 기존 시스템 날리기 (할당량 즉시 반환)**
   ```bash
   bash infra/k8s/airflow/delete-gke.sh
   ```

2. **(필요시) 새로운 세팅으로 빈 땅에 다시 배포**
   ```bash
   bash infra/k8s/airflow/setup-gke.sh
   ```

---

## Appendix C: Workload Identity (Keyless 인증 아키텍처 전환 가이드)

향후 `gcp-key.json` 같은 하드코딩된 자격 증명 파일 없이, **완벽한 클라우드 네이티브 보안 환경**을 구축하고 싶을 때 사용하는 방법입니다. GKE의 Workload Identity를 사용하면 K8s 파드(Service Account)가 Google Cloud의 IAM 권한을 자동으로 상속받습니다.

### 1. IAM 바인딩 (터미널 명령어)
GCP 서비스 어카운트(GSA)와 K8s 서비스 어카운트(KSA)를 연결합니다. Spark와 Airflow 각각에 대해 바인딩이 필요합니다.
```bash
# 변수 설정
PROJECT_ID="my-eth-project-498908"
GSA_NAME="eth-service-account"
GSA_EMAIL="${GSA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

# 1. KSA(Kubernetes Service Account)가 GSA를 대신할 수 있도록 IAM 역할 부여 (Spark)
gcloud iam service-accounts add-iam-policy-binding $GSA_EMAIL \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:${PROJECT_ID}.svc.id.goog[spark/spark]"

# 2. Spark KSA에 어노테이션 추가
kubectl annotate serviceaccount spark \
    --namespace spark \
    iam.gke.io/gcp-service-account=$GSA_EMAIL

# 3. Airflow 파드용 KSA에도 동일하게 역할 부여 (예: default SA를 사용할 경우)
gcloud iam service-accounts add-iam-policy-binding $GSA_EMAIL \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:${PROJECT_ID}.svc.id.goog[airflow/airflow-worker]"

kubectl annotate serviceaccount airflow-worker \
    --namespace airflow \
    iam.gke.io/gcp-service-account=$GSA_EMAIL
# (필요에 따라 scheduler, triggerer 등에도 동일하게 적용하거나, 공통 SA를 사용하도록 Helm 차트에서 설정합니다.)
```

### 2. 코드에서 키 파일 마운트 설정 제거

**A. Spark 설정 (`dags/utils/spark_spec_gke.py`)**
* **지울 내용 1**: `_VOLUME_MOUNTS`와 `_VOLUMES` 리스트에서 `gcp-key` 관련 항목 전체 삭제
* **지울 내용 2**: `hadoopConf` 안에 있는 `google.cloud.auth.service.account.json.keyfile` 줄 삭제

**B. Airflow 설정 (`infra/k8s/airflow/values-base.yaml` 또는 `values-gke.yaml`)**
* **지울 내용**: `apiServer`, `scheduler`, `dagProcessor`, `triggerer`, `workers` 등 모든 블록에 들어가 있는 `extraVolumes` 및 `extraVolumeMounts` (즉, `gcp-key`를 마운트하는 코드) 전체 삭제!

이렇게 세팅만 바꿔두면 파드가 뜰 때 구글 클라우드가 백그라운드에서 임시 토큰을 파드에 꽂아주기 때문에, K8s Secret(`gcp-key.json`)이 유출될 위험이 0%가 됩니다!
