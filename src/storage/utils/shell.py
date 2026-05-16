import subprocess
import os
from pathlib import Path
from src.config import get_logger
from src.storage.utils.gcs import upload_to_gcs

logger = get_logger(__name__)


def run_shell(command: str) -> None:
    """터미널 명령어 실행 (실시간 로그 출력 및 실패 시 예외 발생)"""
    try:
        logger.info(f"명령어 실행 시작: {command}")
        
        env = os.environ.copy()
        venv_bin = "/opt/airflow/eth_etl_venv/bin"
        env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"

        # Popen을 사용하여 실시간 스트리밍 모드로 실행
        # stderr를 stdout으로 통합하여 모든 메시지를 순서대로 캡처
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            env=env,
            bufsize=1  # 라인 버퍼링 활성화
        )

        # 출력을 한 줄씩 읽어서 실시간 로깅
        if process.stdout:
            for line in process.stdout:
                clean_line = line.strip()
                if clean_line:
                    # ethereumetl의 진행 상황이나 에러 메시지를 즉시 출력
                    logger.info(f"[Shell] {clean_line}")

        # 프로세스가 끝날 때까지 대기
        return_code = process.wait()
        
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, command)
            
        logger.info("명령어 실행 성공.")

    except subprocess.CalledProcessError as e:
        logger.error(f"명령어 실행 실패 (Exit Code: {e.returncode})")
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
