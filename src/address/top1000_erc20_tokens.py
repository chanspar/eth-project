"""
시총 상위 1000개 이더리움 ERC20 토큰 수집
─────────────────────────────────────────
출력: top1000_erc20_tokens.csv
컬럼: rank, name, symbol, address, decimals, market_cap_usd

사용법:
    pip install requests pandas
    python fetch_top200_tokens.py
"""

import time
import requests
import pandas as pd

import os

OUTPUT_DIR    = "src/data"
OUTPUT_PATH   = os.path.join(OUTPUT_DIR, "top1000_erc20_tokens.csv")
PARQUET_PATH  = os.path.join(OUTPUT_DIR, "top1000_erc20_tokens.parquet")
TARGET_COUNT  = 200

MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
LIST_URL    = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"


# 분석에 필수적이지만 시총 순위(Rank)가 누락되기 쉬운 주요 토큰 ID
MANDATORY_COIN_IDS = [
    "weth", "staked-ether", "wrapped-bitcoin", "rocket-pool-eth", 
    "frax-ether", "mantle-staked-ether", "binance-eth"
]

def fetch_with_retry(url: str, params: dict = None, timeout: int = 30) -> list:
    """429 에러 시 재시도 로직이 포함된 안전한 요청 함수"""
    max_retries = 5
    base_wait = 30 

    for i in range(max_retries):
        try:
            res = requests.get(url, params=params, timeout=timeout)
            if res.status_code == 429:
                wait_time = base_wait * (i + 1)
                print(f"  ⚠ [429 Error] API 한도 초과! {wait_time}초 후 재시도합니다... ({i+1}/{max_retries})")
                time.sleep(wait_time)
                continue
            res.raise_for_status()
            return res.json()
        except Exception as e:
            if i == max_retries - 1: raise e
            print(f"  ⚠ 네트워크 오류 발생: {e}. 10초 후 재시도...")
            time.sleep(10)
    return []


def main():
    print(f"{'='*60}")
    print("🚀 CoinGecko API 안정 모드: 주요 토큰(WETH 등) 강제 포함 및 다중 탐색")
    print(f"{'='*60}\n")

    rows = []
    seen_addresses = set()
    
    try:
        # Step 1. 전체 주소 맵 가져오기
        platforms_list = fetch_with_retry(LIST_URL)
        if not platforms_list: return
        platforms_map = {c["id"]: c.get("platforms", {}) for c in platforms_list}
        
        # Step 2. 필수 토큰(WETH, stETH 등) 먼저 수집
        print(f"Step 1. 주요 토큰(WETH, stETH 등) 우선 수집 중...")
        mandatory_markets = fetch_with_retry(MARKETS_URL, params={
            "vs_currency": "usd",
            "ids": ",".join(MANDATORY_COIN_IDS)
        })
        
        for coin in mandatory_markets:
            coin_id = coin["id"]
            address = platforms_map.get(coin_id, {}).get("ethereum")
            if address and address.startswith("0x"):
                addr_lower = address.lower().strip()
                rows.append({
                    "rank"          : 0, # 우선순위
                    "market_rank"   : coin.get("market_cap_rank"), # None인 경우 그대로 None 유지
                    "name"          : coin["name"],
                    "symbol"        : coin["symbol"].upper(),
                    "address"       : addr_lower,
                    "decimals"      : 18,
                    "market_cap_usd": int(coin.get("market_cap", 0)) if coin.get("market_cap") else 0,
                })
                seen_addresses.add(addr_lower)
        
        print(f"  → 주요 토큰 {len(rows)}개 확보 완료.")

        # Step 3. 시총 순위별 탐색
        rank_counter = len(rows)
        page = 1
        max_pages = 20 # 시총 5,000위까지 탐색 (250개 * 20페이지)
        TARGET_COUNT = 1000 # 목표 수집 개수

        print(f"\nStep 2. 시총 순위별 탐색 시작 (최대 5,000위 / 목표 1,000개)...")
        
        while page <= max_pages and len(rows) < TARGET_COUNT:
            print(f"  → 페이지 {page:2d}/20 요청 중... (현재 {len(rows)}개 발견)")
            markets = fetch_with_retry(MARKETS_URL, params={
                "vs_currency": "usd",
                "order"      : "market_cap_desc",
                "per_page"   : 250,
                "page"       : page,
                "sparkline"  : False,
            })
            
            if not markets: break

            for coin in markets:
                if len(rows) >= TARGET_COUNT:
                    break

                coin_id = coin["id"]
                address = platforms_map.get(coin_id, {}).get("ethereum")
                
                if address and address.startswith("0x"):
                    addr_lower = address.lower().strip()
                    if addr_lower not in seen_addresses:
                        rank_counter += 1
                        rows.append({
                            "rank"          : rank_counter,
                            "market_rank"   : coin.get("market_cap_rank"),
                            "name"          : coin["name"],
                            "symbol"        : coin["symbol"].upper(),
                            "address"       : addr_lower,
                            "decimals"      : 18,
                            "market_cap_usd": int(coin.get("market_cap", 0)) if coin.get("market_cap") else 0,
                        })
                        seen_addresses.add(addr_lower)

            page += 1
            time.sleep(3.0)

    except KeyboardInterrupt:
        print("\n\n⏹ 사용자에 의해 중단되었습니다.")
    except Exception as e:
        print(f"\n❌ 심각한 오류 발생: {e}")

    if rows:
        # 출력 디렉토리 생성
        os.makedirs(OUTPUT_DIR, exist_ok=True)

        # 시총 순서대로 재정렬 (WETH 등이 중간에 섞이도록)
        df = pd.DataFrame(rows)
        df = df.sort_values(by="market_cap_usd", ascending=False).reset_index(drop=True)
        df["rank"] = df.index + 1
        
        # 저장
        df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        df.to_parquet(PARQUET_PATH, index=False)
        
        print(f"\n{'='*60}")
        print(f"✅ 저장 완료!")
        print(f"  - CSV: {OUTPUT_PATH}")
        print(f"  - Parquet: {PARQUET_PATH}")
        print(f"총 수집된 ERC-20 토큰 수: {len(df)}")
        print(f"{'='*60}")
        print(df[["rank", "market_rank", "name", "symbol", "address"]].head(20).to_string(index=False))
    else:
        print("\n❗ 수집된 데이터가 없습니다.")


if __name__ == "__main__":
    main()
