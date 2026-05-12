import subprocess
import os
from pathlib import Path
from src.config import get_logger
from src.storage.utils.gcs import upload_to_gcs

logger = get_logger(__name__)


def run_shell(command: str) -> None:
    """터미널 명령어 실행 (실패 시 상세 로그와 함께 예외 발생)"""
    try:
        logger.info(f"명령어 실행 시작: {command}")
        
        # 가상환경의 bin 폴더를 PATH에 추가하여 실행 파일(ethereumetl 등)을 찾을 수 있게 함
        env = os.environ.copy()
        venv_bin = "/opt/airflow/eth_etl_venv/bin"
        env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"

        result = subprocess.run(
            command, 
            shell=True, 
            capture_output=True, 
            text=True,
            check=True,
            env=env
        )
        
        if result.stdout:
            logger.debug(f"명령어 출력(stdout):\n{result.stdout.strip()}")
            
        logger.info("명령어 실행 성공.")

    except subprocess.CalledProcessError as e:
        logger.error(f"명령어 실행 실패 (Exit Code: {e.returncode})")
        logger.error(f"에러 메시지(stderr):\n{e.stderr.strip()}")
        raise
    except Exception:
        logger.exception(f"명령어 실행 중 예상치 못한 예외 발생: {command}")
        raise


def run_and_upload(cmd: str, local_file: str, gcs_path: str) -> None:
    """ETL 실행 → GCS 업로드 → 로컬 파일 정리 공통 처리"""
    try:
        run_shell(cmd)
        
        p = Path(local_file)
        if not p.exists():
            logger.error(f"결과 파일이 생성되지 않았습니다: {local_file}")
            raise FileNotFoundError(f"ETL 결과 파일 없음: {local_file}")
            
        logger.info(f"GCS 업로드 시작: {local_file} -> {gcs_path}")
        upload_to_gcs(local_file, gcs_path)
        logger.info("GCS 업로드 완료.")
        
    except Exception:
        logger.exception(f"실행 및 업로드 과정 중 오류 발생: {local_file}")
        raise
    finally:
        _cleanup(local_file)


def _cleanup(file_path: str) -> None:
    """임시 로컬 파일 삭제"""
    p = Path(file_path)
    if p.exists():
        try:
            os.remove(p)
            logger.info(f"임시 파일 삭제 완료: {file_path}")
        except Exception:
            logger.warning(f"임시 파일 삭제 실패: {file_path}")
