"""
CEX / DEX / Bridge 알려진 주소 수집
─────────────────────────────────────
출처: github.com/brianleect/etherscan-labels
      data/etherscan/combined/combinedAllLabels.json

데이터 형태:
  { "0xABCD": {"name": "Binance 14", "labels": ["exchange", "binance"]} }

출력: known_labels.csv / known_labels.parquet
컬럼: address, name, category

사용법:
    pip install requests pandas pyarrow
    python fetch_known_labels.py
"""

import requests
import pandas as pd

import os

OUTPUT_DIR     = "src/data"
OUTPUT_CSV     = os.path.join(OUTPUT_DIR, "known_labels.csv")
OUTPUT_PARQUET = os.path.join(OUTPUT_DIR, "known_labels.parquet")

GITHUB_URL = (
    "https://raw.githubusercontent.com/brianleect/etherscan-labels"
    "/main/data/etherscan/combined/combinedAllLabels.json"
)

# ─────────────────────────────────────────
# labels 태그 → 카테고리 매핑
# 리스트 앞쪽이 우선순위 높음
# ─────────────────────────────────────────
TAG_TO_CATEGORY = {
    # CEX
    "exchange"  : "CEX",
    "binance"   : "CEX",
    "coinbase"  : "CEX",
    "kraken"    : "CEX",
    "okex"      : "CEX",
    "kucoin"    : "CEX",
    "bybit"     : "CEX",
    "bitfinex"  : "CEX",
    "bithumb"   : "CEX",
    "upbit"     : "CEX",
    "huobi"     : "CEX",
    "gate-io"   : "CEX",
    "gemini"    : "CEX",
    "bitstamp"  : "CEX",
    # DEX
    "dex"           : "DEX",
    "uniswap"       : "DEX",
    "sushiswap"     : "DEX",
    "curve-fi"      : "DEX",
    "balancer"      : "DEX",
    "1inch"         : "DEX",
    "pancakeswap"   : "DEX",
    "dydx"          : "DEX",
    "paraswap"      : "DEX",
    "cowswap"       : "DEX",
    "0x-protocol"   : "DEX",
    "kyberswap"     : "DEX",
    # Bridge
    "bridge"            : "Bridge",
    "optimism-bridge"   : "Bridge",
    "hop-protocol"      : "Bridge",
    "stargate"          : "Bridge",
    "wormhole"          : "Bridge",
    "multichain"        : "Bridge",
    "celer-network"     : "Bridge",
    "across"            : "Bridge",
    "synapse"           : "Bridge",
    # DeFi
    "defi"      : "DeFi",
    "aave"      : "DeFi",
    "compound"  : "DeFi",
    "makerdao"  : "DeFi",
    "lido"      : "DeFi",
    "yearn"     : "DeFi",
    "convex"    : "DeFi",
    "lending"   : "DeFi",
    # Mixer / Sanction
    "tornado-cash"  : "Mixer",
    "phish-hack"    : "Sanctioned",
    "heist"         : "Sanctioned",
}

# 이 태그만 있으면 제외 (노이즈)
SKIP_ONLY_TAGS = {
    "contract-deployer", "donate", "old-contract",
    "deprecated", "delegate", "factory-contract",
}

# 우리가 원하는 카테고리만
TARGET_CATEGORIES = {"CEX", "DEX", "Bridge", "DeFi", "Mixer", "Sanctioned"}


from typing import Optional

def resolve_category(labels: list) -> Optional[str]:
    tag_set = set(labels)

    # 노이즈 태그만 있으면 제외
    if tag_set.issubset(SKIP_ONLY_TAGS):
        return None

    # 앞쪽 태그 우선으로 카테고리 결정
    for tag in labels:
        cat = TAG_TO_CATEGORY.get(tag)
        if cat and cat in TARGET_CATEGORIES:
            return cat

    return None


def fetch_labels() -> pd.DataFrame:
    print("GitHub에서 combinedAllLabels.json 다운로드 중...")
    res = requests.get(GITHUB_URL, timeout=30)
    res.raise_for_status()
    data = res.json()
    print(f"  → 전체 {len(data):,}개 주소 로드됨\n")

    rows = []
    for address, info in data.items():
        if not isinstance(info, dict):
            continue

        addr   = address.lower().strip()
        name   = info.get("name", "")
        labels = info.get("labels", [])

        # 유효한 이더리움 주소 체크
        if not addr.startswith("0x") or len(addr) != 42:
            continue

        category = resolve_category(labels)
        if category is None:
            continue

        rows.append({
            "address" : addr,
            "name"    : name,
            "category": category,
        })

    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["address"])
    df = df.sort_values(["category", "name"]).reset_index(drop=True)
    return df


def main():
    df = fetch_labels()

    # 결과 요약
    print("카테고리별 수집 결과:")
    print("─" * 30)
    for cat, cnt in df["category"].value_counts().items():
        print(f"  {cat:<12}: {cnt:,}개")
    print("─" * 30)
    print(f"  총계        : {len(df):,}개\n")

    # 저장
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8-sig")
    df.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"✅ CSV     저장: {OUTPUT_CSV}")
    print(f"✅ Parquet 저장: {OUTPUT_PARQUET}")


if __name__ == "__main__":
    main()
