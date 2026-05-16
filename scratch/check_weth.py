import requests

url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
res = requests.get(url)
data = res.json()

weth = [c for c in data if c['id'] == 'weth']
print(f"WETH data: {weth}")

steth = [c for c in data if c['id'] == 'staked-ether']
print(f"stETH data: {steth}")
