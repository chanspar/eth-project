import time
import requests
import concurrent.futures

# 테스트 설정
URL = "http://127.0.0.1:8000/api/v1/metrics/gas"
NUM_REQUESTS = 500          # 총 요청 횟수
CONCURRENT_WORKERS = 50     # 동시에 보낼 스레드 수

def fetch_gas(req_id):
    start = time.time()
    try:
        resp = requests.get(URL, timeout=5)
        return resp.status_code, time.time() - start
    except Exception as e:
        return 0, time.time() - start

def run_test():
    print(f"🚀 부하 테스트 시작: {CONCURRENT_WORKERS}개의 스레드가 총 {NUM_REQUESTS}개의 요청을 전송합니다...")
    start_time = time.time()
    
    success_count = 0
    error_count = 0
    
    # ThreadPoolExecutor를 사용해 동시 요청 발송
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
        # req_id를 넘겨서 작업 제출
        futures = [executor.submit(fetch_gas, i) for i in range(NUM_REQUESTS)]
        
        for future in concurrent.futures.as_completed(futures):
            status, duration = future.result()
            if status == 200:
                success_count += 1
            else:
                error_count += 1
                
    total_time = time.time() - start_time
    print("\n--- 📊 테스트 결과 ---")
    print(f"총 소요 시간: {total_time:.2f} 초")
    print(f"초당 처리량 (TPS): {NUM_REQUESTS / total_time:.2f} req/s")
    print(f"성공한 요청: {success_count} 건")
    print(f"실패/에러 요청: {error_count} 건")
    
    if total_time > 0:
        print("\n💡 해석 가이드:")
        print("캐시가 없다면 DB가 매번 무거운 집계 쿼리를 처리하느라 속도가 느리고 에러가 날 수 있습니다.")
        print("캐시를 적용한 후 다시 테스트하면 '총 소요 시간'이 엄청나게 단축되는 것을 볼 수 있습니다!")

if __name__ == "__main__":
    run_test()
