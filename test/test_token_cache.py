import requests
import time
import concurrent.futures

URL = "http://127.0.0.1:8000/api/v1/tokens/trending?limit=10&hours=24"
TOTAL_REQUESTS = 500
CONCURRENT_THREADS = 50

def fetch():
    try:
        response = requests.get(URL, timeout=10)
        return response.status_code == 200
    except Exception as e:
        return False

def run_load_test():
    print(f"🚀 토큰 트렌딩 부하 테스트 시작: {CONCURRENT_THREADS}개의 스레드가 총 {TOTAL_REQUESTS}개의 요청을 전송합니다...")
    start_time = time.time()
    
    success_count = 0
    fail_count = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_THREADS) as executor:
        futures = [executor.submit(fetch) for _ in range(TOTAL_REQUESTS)]
        for future in concurrent.futures.as_completed(futures):
            if future.result():
                success_count += 1
            else:
                fail_count += 1
                
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\n--- 📊 토큰 랭킹 API 캐시 부하 테스트 결과 ---")
    print(f"총 소요 시간: {total_time:.2f} 초")
    print(f"초당 처리량 (TPS): {TOTAL_REQUESTS / total_time:.2f} req/s")
    print(f"성공한 요청: {success_count} 건")
    print(f"실패/에러 요청: {fail_count} 건")
    print(f"\n💡 해석 가이드:")
    print(f"캐시와 락이 정상 작동한다면 첫 1건만 DB 연산을 수행하고,")
    print(f"나머지 499건은 0.001초 만에 Redis 캐시에서 가져오므로 총 소요 시간이 1~2초 내외로 매우 짧아야 합니다!")

if __name__ == "__main__":
    run_load_test()
