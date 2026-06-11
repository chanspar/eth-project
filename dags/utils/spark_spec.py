"""
SparkApplication CRD 스펙 빌더 — 모든 레이어 DAG 공통 사용

수정 사항:
  - sparkVersion 추가 (CRD 필수 필드)
  - git-sync v4 경로 수정: mainApplicationFile, PYTHONPATH에 current/ 포함
  - AQE(Adaptive Query Execution) 활성화
  - timeToLiveSeconds: 300 → 3600
"""

SPARK_IMAGE = "192.168.34.3:5000/eth-project/spark-custom:3.4.0"
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
        # GITSYNC_ROOT=/git, GITSYNC_LINK=current
        # → 볼륨 내 구조: <mountPath>/current/ (심볼릭 링크 → 실제 커밋 디렉터리)
        # → 코드 실제 경로: /opt/spark/work-dir/current/src/...
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
    SparkApplication CRD 스펙을 동적으로 생성합니다.

    Args:
        app_name: SparkApplication 이름 (Jinja 템플릿 사용 가능, e.g. 'silver-{{ ds_nodash }}')
        main_file: 실행할 Python 파일 경로 (git 저장소 루트 기준, e.g. 'src/silver/txn_enriched.py')
        arguments: spark-submit arguments 리스트 (e.g. ['--date', '{{ ds }}'])
        driver_memory: Driver 메모리 (e.g. '3g')
        executor_memory: Executor 메모리 (e.g. '4g')
        executor_instances: Executor 인스턴스 수
        executor_cores: Executor 코어 수

    git-sync v4 경로 구조:
        initContainer: /git/{hash}/ + symlink /git/current → /git/{hash}/
        주 컨테이너:   spark-code 볼륨을 /opt/spark/work-dir 에 마운트
        결과 경로:     /opt/spark/work-dir/current/src/... (symlink 경유)
    """
    # driver/executor 공통 파드 설정
    pod_common = {
        "nodeSelector": {"role": "spark"},
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
