import requests

url = "https://api.coingecko.com/api/v3/coins/markets"
res = requests.get(url, params={
    "vs_currency": "usd",
    "ids": "weth,staked-ether,tether"
})
data = res.json()
for coin in data:
    print(f"{coin['id']}: Market Cap = {coin['market_cap']}, Rank = {coin['market_cap_rank']}")
