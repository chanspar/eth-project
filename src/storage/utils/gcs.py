from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError
from src.config import PROJECT_ID, BUCKET_NAME, get_logger

logger = get_logger(__name__)


def upload_to_gcs(local_path: str, gcs_path: str) -> None:
    """로컬 파일을 GCS로 업로드"""
    try:
        # 만약 나중에 수천 개의 파일을 루프 돌면서 올려야 한다면, client = storage.Client()를 함수 밖으로 빼서 딱 한 번만 생성하게 바꾸기
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        blob = bucket.blob(gcs_path)
        logger.info(f"GCS 업로드 시작: {local_path} → gs://{BUCKET_NAME}/{gcs_path}")
        blob.upload_from_filename(local_path)
        logger.info(f"✅ 업로드 성공: gs://{BUCKET_NAME}/{gcs_path}")
    except GoogleCloudError:
        logger.exception(f"❌ GCS 업로드 실패 (경로: {gcs_path})")
        raise
    except FileNotFoundError:
        logger.error(f"❌ 업로드할 로컬 파일이 없습니다: {local_path}")
        raise
