"""
Spark on Kubernetes 테스트 DAG
- Spark 이미지 내장 pi.py 예제로 K8s 클러스터 연동을 검증
- infra/spark-test.yaml 을 SparkApplication으로 제출
"""
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator



# ── 인라인 SparkApplication 스펙 ──────────────────────────────────────────
# spark-application.yaml을 참고하되, 테스트용으로 최소 리소스 + 간단한 파이썬 코드
'''
SPARK_TEST_SPEC = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "spark-k8s-test",
        "namespace": "spark",
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "192.168.34.3:5000/eth-project/spark-custom:latest",
        "imagePullPolicy": "Always",
        # 이미지 내부에 있는 PySpark 예제 스크립트 사용 (별도 코드 필요 없음)
        "mainApplicationFile": "local:///opt/spark/examples/src/main/python/pi.py",
        "arguments": ["100"],  # 파이 계산 파티션 수
        "restartPolicy": {"type": "Never"},
        "timeToLiveSeconds": 120,
        "sparkConf": {
            "spark.ui.port": "4045",
        },
        "driver": {
            "cores": 1,
            "memory": "512m",
            "serviceAccount": "spark",
            "nodeSelector": {"role": "spark"},
        },
        "executor": {
            "cores": 1,
            "instances": 1,
            "memory": "512m",
            "nodeSelector": {"role": "spark"},
        },
    },
}
'''

@dag(
    dag_id="spark_k8s_test",
    start_date=datetime(2026, 6, 1),
    schedule=None,          # 수동 트리거 전용
    catchup=False,
    tags=["test", "spark", "kubernetes"],
)
def spark_k8s_test_dag():

    # ── Spark 잡 제출 ─────────────────────────────────────────────────────
    # infra/spark-test.yaml 파일을 그대로 K8s에 apply
    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_spark_pi_job",
        namespace="spark",
        application_file="infra/spark-application.yaml",
        kubernetes_conn_id="kubernetes_default",
        get_logs=True,
    )

    @task
    def report_result():
        print("✅ Spark on K8s 테스트 성공! 클러스터 연동이 정상 동작합니다.")

    submit_spark_job >> report_result()


spark_k8s_test_dag()
