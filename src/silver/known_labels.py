from pyspark.sql import SparkSession


KNOWN_LABELS = {
    # --- CEX (중앙화 거래소) ---
    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": ("Binance", "CEX"),
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": ("Binance Cold", "CEX"),
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": ("Binance Hot", "CEX"),
    "0x56eddb7aa87536c09ccc2793473599fd21a8b17f": ("Binance US", "CEX"),
    "0x8894e0a0c962cb723c1976a4421c95949be2d4e3": ("Binance Bridge", "CEX"),
    
    # Coinbase
    "0x0681d8db095565fe8a346fa0277bffde9c0edbbf": ("Coinbase", "CEX"),
    "0x503828976d22510aad0201ac7ec88293211d23da": ("Coinbase 2", "CEX"),
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": ("Coinbase 3", "CEX"),
    "0x3cd751e6b0078be393132286c442345e5dc49699": ("Coinbase 4", "CEX"),
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": ("Coinbase 5", "CEX"),
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": ("Coinbase 6", "CEX"),
    
    # OKX & KuCoin & Kraken
    "0x77696bb39917c91a0c3d2f859fd1afaa0b2b5bdb": ("OKX", "CEX"),
    "0x98ec059dc3adfbdd63429454aeb0c990fba4a128": ("OKX 2", "CEX"),
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": ("OKX 3", "CEX"),
    "0x2b5634c42055806a59e9107ed44d43c426e58258": ("KuCoin", "CEX"),
    "0x689c56aef474df92d44a1b70850f808488f9769c": ("KuCoin 2", "CEX"),
    "0xd6216fc19db775df9774a6e33526131da7d19a2c": ("KuCoin 3", "CEX"),
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": ("Kraken", "CEX"),
    "0xc6bed363b30df7f35b601a5547fe56cd31ec63da": ("Kraken 2", "CEX"),
    "0x267be1c1d684f78cb4f6a176c4911b741e4ffdc0": ("Kraken 3", "CEX"),

    # 국내 거래소 (추가됨)
    "0x3f5ce5fb1162781181f7d1be1816ed04e6e000ef": ("Upbit Hot Wallet", "CEX"),
    "0x3282772591605330a84e9a7e0a2d592a832f0857": ("Bithumb", "CEX"),

    # --- DEX / DeFi 프로토콜 ---
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": ("Uniswap V2 Router", "DEX"),
    "0xe592427a0aece92de3edee1f18e0157c05861564": ("Uniswap V3 Router", "DEX"),
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": ("Uniswap V3 Router2", "DEX"),
    "0xc36442b4a4522e871399cd717abdd847ab11fe88": ("Uniswap V3 Positions", "DEX"),
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": ("SushiSwap Router", "DEX"),
    "0x1111111254fb6c44bac0bed2854e76f90643097d": ("1inch V3", "DEX"),
    "0x1111111254eeb25477b68fb85ed929f73a960582": ("1inch V4", "DEX"),
    "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9": ("Aave V2 Lending Pool", "DeFi"),
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84": ("Lido stETH", "DeFi"),

    # --- 핵심 토큰 컨트랙트 (추가됨) ---
    "0xdac17f958d2ee523a2206206994597c13d831ec7": ("Tether USD (USDT)", "Token"),
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": ("Circle USDC", "Token"),
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": ("Wrapped Ether (WETH)", "Token"),

    # --- 브리지 / 레이어2 ---
    "0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf": ("Polygon Bridge", "Bridge"),
    "0x8484ef722627bf18ca5ae6bcf031c23e6e922b30": ("Polygon Bridge 2", "Bridge"),
    "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1": ("Optimism Bridge", "Bridge"),
    "0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f": ("Arbitrum Bridge", "Bridge"),

    # --- 기타 인프라 ---
    "0x00000000219ab540356cbb839cbe05303d7705fa": ("ETH2 Deposit", "Staking"),
    "0xd1669ac6044269b59fa12c5822439f609ca54f41": ("ETH Burn", "Burn"),
    "0x0000000000000000000000000000000000000000": ("Null Address", "Burn"),
}


def load_label_df(spark: SparkSession):
	"""KNOWN_LABELS dict -> Spark DataFrame (브로드캐스트 조인용)"""
	rows = [
		(addr.lower(), name, category)
		for addr, (name, category) in KNOWN_LABELS.items()
    ]
	return spark.createDataFrame(rows, ["address", "label_name", "label_category"])
