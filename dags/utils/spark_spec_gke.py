"""
SparkApplication CRD 스펙 빌더 (GKE Autopilot 전용)

수정 사항:
  - SPARK_IMAGE를 GAR(Google Artifact Registry) 경로로 변경
  - GKE Autopilot 환경에 맞춰 nodeSelector 제거 (GKE가 알아서 스케줄링)
"""

SPARK_IMAGE = "asia-northeast3-docker.pkg.dev/my-eth-project-498908/eth-repo/spark-custom:3.4.0"
SPARK_NAMESPACE = "spark"
K8S_CONN_ID = "kubernetes_default"
SPARK_VERSION = "3.4.0"

_GIT_SYNC_INIT = {
    "name": "git-sync-init",
    "image": "registry.k8s.io/git-sync/git-sync:v4.1.0",
    "env": [
        {"name": "GITSYNC_REPO",    "value": "https://github.com/chanspar/eth-project.git"},
        {"name": "GITSYNC_BRANCH",  "value": "main"},
        {"name": "GITSYNC_ONE_TIME","value": "true"},
        {"name": "GITSYNC_ROOT",    "value": "/git"},
        {"name": "GITSYNC_LINK",    "value": "current"},
    ],
    "volumeMounts": [{"name": "spark-code", "mountPath": "/git"}],
}

_VOLUME_MOUNTS = [
    {"name": "spark-code", "mountPath": "/opt/spark/work-dir"},
    {"name": "gcp-key",    "mountPath": "/etc/secrets/gcp", "readOnly": True},
]

_VOLUMES = [
    {"name": "spark-code", "emptyDir": {}},
    {"name": "gcp-key",    "secret": {"secretName": "gcp-key"}},
]


def build_spark_spec(
    app_name: str,
    main_file: str,
    arguments: list[str] | None = None,
    driver_memory: str = "1g",
    executor_memory: str = "2g",
    executor_instances: int = 1,
    executor_cores: int = 1,
) -> dict:
    """
    SparkApplication CRD 스펙을 동적으로 생성합니다. (GKE 전용)
    """
    # driver/executor 공통 파드 설정
    # GKE Autopilot에서는 nodeSelector("role": "spark")가 필요 없습니다.
    pod_common = {
        "initContainers": [_GIT_SYNC_INIT],
        "volumeMounts": _VOLUME_MOUNTS,
        "envFrom": [{"secretRef": {"name": "spark-env"}}],
    }

    return {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": app_name,
            "namespace": SPARK_NAMESPACE,
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "sparkVersion": SPARK_VERSION,
            "image": SPARK_IMAGE,
            "imagePullPolicy": "Always",
            "mainApplicationFile": f"local:///opt/spark/work-dir/current/{main_file}",
            "arguments": arguments or [],
            "restartPolicy": {"type": "Never"},
            "timeToLiveSeconds": 3600,
            "hadoopConf": {
                "fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
                "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
                "google.cloud.auth.service.account.enable": "true",
                "google.cloud.auth.service.account.json.keyfile": "{{ '/etc/secrets/gcp/gcp-key.json' }}",
            },
            "sparkConf": {
                "spark.ui.port": "4045",
                "spark.sql.sources.partitionOverwriteMode": "dynamic",
                "spark.sql.session.timeZone": "UTC",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.kubernetes.driverEnv.PYTHONPATH": "/opt/spark/work-dir/current",
                "spark.executorEnv.PYTHONPATH": "/opt/spark/work-dir/current",
            },
            "volumes": _VOLUMES,
            "driver": {
                "cores": 1,
                "memory": driver_memory,
                "serviceAccount": "spark",
                **pod_common,
            },
            "executor": {
                "cores": executor_cores,
                "instances": executor_instances,
                "memory": executor_memory,
                **pod_common,
            },
        },
    }
