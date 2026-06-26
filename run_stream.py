import os
import subprocess
import sys
import json
import requests
from pathlib import Path
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# 환경 변수에서 설정값 가져오기 (없으면 에러 발생 혹은 기본값)
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
if not ALCHEMY_API_KEY:
    print("❌ ALCHEMY_API_KEY가 .env 파일에 설정되어 있지 않습니다.")
    print("💡 .env 파일에 ALCHEMY_API_KEY=당신의_API_키 값을 추가해주세요.")
    sys.exit(1)

PROVIDER_URI = f"https://eth-mainnet.g.alchemy.com/v2/{ALCHEMY_API_KEY}"
KAFKA_OUTPUT = os.getenv("KAFKA_OUTPUT", "kafka/host.docker.internal:9094")

# 기본 명령어 리스트 구성
cmd = [
    "ethereumetl", "stream",
    "--provider-uri", PROVIDER_URI,
    "--lag", "2",
    "--output", KAFKA_OUTPUT,
    "--entity-types", "block,transaction,token_transfer",
    "--max-workers", "1",
    "--batch-size", "1"
]

# last_synced_block.txt 가 있는지 확인
start_block = None
if Path("last_synced_block.txt").exists():
    print("✅ last_synced_block.txt 파일이 존재합니다. 해당 체크포인트부터 이어서 시작합니다.")
else:
    print("⚠️ last_synced_block.txt 파일이 없습니다. 현재 네트워크의 최신 블록을 조회합니다...")
    try:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_blockNumber",
            "params": [],
            "id": 1
        }
        response = requests.post(PROVIDER_URI, json=payload)
        response.raise_for_status()
        latest_block_hex = response.json().get("result")
        if latest_block_hex:
            start_block = str(int(latest_block_hex, 16))
            print(f"🔥 조회된 최신 블록 넘버: {start_block}")
            cmd.extend(["--start-block", start_block])
        else:
            print("❌ 최신 블록 넘버를 가져오지 못했습니다. 기본값 24985609를 사용합니다.")
            cmd.extend(["--start-block", "24985609"])
    except Exception as e:
        print(f"❌ 최신 블록 조회 중 에러 발생: {e}. 기본값 24985609를 사용합니다.")
        cmd.extend(["--start-block", "24985609"])

# 실행
print("🚀 Ethereum ETL 스트리밍을 시작합니다 (Docker 환경 내부)...")
print(f"실행 명령어: {' '.join([c if 'alchemy.com' not in c else 'https://eth-mainnet.g.alchemy.com/v2/****' for c in cmd])}")

try:
    # 서브프로세스로 ethereumetl 실행
    subprocess.run(cmd, check=True)
except KeyboardInterrupt:
    print("\n🛑 사용자에 의해 스트리밍이 중단되었습니다.")
except subprocess.CalledProcessError as e:
    print(f"\n❌ ethereumetl 실행 중 오류가 발생했습니다. (종료 코드: {e.returncode})")
except FileNotFoundError:
    print("\n❌ 'ethereumetl' 명령어를 찾을 수 없습니다.")
