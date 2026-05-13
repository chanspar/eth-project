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


DEX_ADDRESSES = {

    # -------------------------------------------------------------------------
    # Uniswap
    # -------------------------------------------------------------------------
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": "Uniswap V2: Router",
    "0xe592427a0aece92de3edee1f18e0157c05861564": "Uniswap V3: Router",
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": "Uniswap V3: Router2",
    "0xef1c6e67703c7bd7107eed8303fbe6ec2554bf6b": "Uniswap: Universal Router (Old)",
    "0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD": "Uniswap: Universal Router",
    "0x000000000004444c5dc75cb358380d2e3de08a90": "Uniswap V4: PoolManager", 

    # -------------------------------------------------------------------------
    # SushiSwap
    # -------------------------------------------------------------------------
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": "SushiSwap: Router",

    # -------------------------------------------------------------------------
    # PancakeSwap (Ethereum)
    # -------------------------------------------------------------------------
    "0x13f4ea83d0bd40e75c8222255bc855a974568dd4": "PancakeSwap V3: Router",

    # -------------------------------------------------------------------------
    # Curve
    # -------------------------------------------------------------------------
    "0x99a58482bd75cbab83b27ec03ca68ff489b5788f": "Curve: Router",
    "0xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7": "Curve: 3pool (DAI/USDC/USDT)",    
    "0xdc24316b9ae028f1497c275eb9192a3ea0f67022": "Curve: stETH Pool",              

    # -------------------------------------------------------------------------
    # Balancer
    # -------------------------------------------------------------------------
    "0xba12222222228d8ba445958a75a0704d566bf2c8": "Balancer: Vault",

    # -------------------------------------------------------------------------
    # Fraxswap
    # -------------------------------------------------------------------------
    "0xc14d550632db8592d1243edc8b95b0ad06703867": "Fraxswap: Router",               

    # -------------------------------------------------------------------------
    # 1inch
    # -------------------------------------------------------------------------
    "0x1111111254fb6c44bac0bed2854e76f90643097d": "1inch: Aggregation Router V4",
    "0x1111111254eeb25477b68fb85ed929f73a960582": "1inch: Aggregation Router V5",

    # -------------------------------------------------------------------------
    # 0x Protocol
    # -------------------------------------------------------------------------
    "0xdef1c0ded9bec7f1a1670819833240f027b25eff": "0x: Exchange Proxy",

    # -------------------------------------------------------------------------
    # ParaSwap
    # -------------------------------------------------------------------------
    "0xdef171fe48cf0115b1d80b88dc8eab59176fee57": "ParaSwap: Augustus Swapper",

    # -------------------------------------------------------------------------
    # KyberSwap
    # -------------------------------------------------------------------------
    "0x6131b5fae19ea4f9d964eac0408e4408b66337b5": "KyberSwap: Meta Aggregation Router V2", 

    # -------------------------------------------------------------------------
    # Odos
    # -------------------------------------------------------------------------
    "0xcf5540fffcdc3d510b18bfca6d2b9987b0772559": "Odos: Router V2",                 

    # -------------------------------------------------------------------------
    # OpenOcean
    # -------------------------------------------------------------------------
    "0x6352a56caadc4f1e25cd6c75970fa768a3304e64": "OpenOcean: Exchange Proxy",   
}

TOKEN_META = {
    # --- Native & Wrapped Assets ---
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": ("WETH",   "Wrapped Ether",              18),
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": ("WBTC",   "Wrapped BTC",                 8),

    # --- Stablecoins (USDT/USDC/PYUSD는 6자리, 나머지는 18자리) ---
    "0xdac17f958d2ee523a2206206994597c13d831ec7": ("USDT",   "Tether USD",                  6),
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": ("USDC",   "USD Coin",                    6),
    "0x6b175474e89094c44da98b954eedeac495271d0f": ("DAI",    "Dai Stablecoin",             18),
    "0x853d955acef822db058eb8505911ed77f175b99e": ("FRAX",   "Frax",                       18),
    "0x0000000000085d4780b73119b644ae5ecd22b376": ("TUSD",   "TrueUSD",                    18),
    "0x6c3ea9036406852006290770BEdFcAbA0e23A0e8": ("PYUSD",  "PayPal USD",                  6),
    "0x4c9edd5852cd905f086c759e8383e09bff1e68b3": ("USDe",   "Ethena USDe",               18),  # ✨ 추가

    # --- Liquid Staking (LSD) ---
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84": ("stETH",  "Liquid staked Ether",        18),
    "0xae78736cd615f374d3085123a210448e74fc6393": ("rETH",   "Rocket Pool ETH",            18),
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704": ("cbETH",  "Coinbase Wrapped ETH",       18),
    "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee": ("weETH",  "Wrapped eETH (ether.fi)",   18),  # ✨ 추가

    # --- DeFi Bluechips ---
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": ("UNI",    "Uniswap",                    18),
    "0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9": ("AAVE",   "Aave",                       18),
    "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2": ("MKR",    "Maker",                      18),
    "0x514910771af9ca656af840dff83e8264ecf986ca": ("LINK",   "Chainlink",                  18),
    "0xd533a949740bb3306d119cc777fa900ba034cd52": ("CRV",    "Curve DAO Token",            18),
    "0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f": ("SNX",    "Synthetix Network",          18),
    "0xba100000625a3754423978a60c9317c58a424e3d": ("BAL",    "Balancer",                   18),
    "0xc00e94cb662c3520282e6f5717214004a7f26888": ("COMP",   "Compound",                   18),  # ✨ 추가
    "0x6b3595068778dd592e39a122f4f5a5cf09c90fe2": ("SUSHI",  "SushiSwap",                  18),  # ✨ 추가
    "0x4e3fbd56cd56c3e72c1403e103b45db9da5b9d2b": ("CVX",    "Convex Finance",             18),  # ✨ 추가
    "0x5a98fcbea516cf06857215779fd812ca3bef1b32": ("LDO",    "Lido DAO",                   18),  # ✨ 추가
    "0xc18360217d8f7ab5e7c516566761ea12ce7f9d72": ("ENS",    "Ethereum Name Service",      18),  # ✨ 추가
    "0xc944e90c64b2c07662a292be6244bdf05cda44a7": ("GRT",    "The Graph",                  18),  # ✨ 추가
    "0x57e114b691db790c35207b2e685d4a43181e6061": ("ENA",    "Ethena",                     18),  # ✨ 추가

    # --- L2 & Infrastructure ---
    "0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0": ("MATIC",  "Polygon (Old)",              18),
    "0x455e53cbb86018ac2b8092fdcd39d8444affc3f6": ("POL",    "Polygon Ecosystem Token",   18),
    "0xb50721bcf8d664c30412cfbc6cf7a15145234ad1": ("ARB",    "Arbitrum",                   18),
    "0xbbbbca6a901c926f240b89eacb641d8aec7aeafd": ("LRC",    "Loopring",                   18),  # ✅ 주소 수정
    "0x111111111117dc0aa78b770fa6a738034120c302": ("1INCH",  "1inch",                      18),
    "0xd33526068d116ce69f19a9ee46f0bd304f21a51f": ("RPL",    "Rocket Pool",                18),  # ✨ 추가

    # --- AI & Memecoins ---
    "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce": ("SHIB",   "Shiba Inu",                  18),
    "0x6982508145454ce325ddbe47a25d4ec3d2311933": ("PEPE",   "Pepe",                       18),
    "0x4d224452801aced8b2f0aebe155379bb5d594381": ("APE",    "ApeCoin",                    18),
    "0x4e15361fd6b4bb609fa63c81a2be19d873717870": ("FTM",    "Fantom",                     18),

    # --- Exchange Tokens ---
    "0xb8c77482e45f1f44de1745f52c74426c631bdd52": ("BNB",    "BNB",                        18),
}

# ERC-20 전송 이벤트 시그니처 (Transfer topic0)
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

def load_token_meta_df(spark: SparkSession):
    """TOKEN_META dict → Spark DataFrame (브로드캐스트 조인용)"""
    rows = [
        (addr.lower(), symbol, name, decimals)
        for addr, (symbol, name, decimals) in TOKEN_META.items()
    ]
    return spark.createDataFrame(
        rows, ["token_address", "symbol", "token_name", "decimals"]
    )


def load_dex_df(spark: SparkSession):
    """DEX_ADDRESSES dict → Spark DataFrame"""
    rows = [(addr.lower(), label) for addr, label in DEX_ADDRESSES.items()]
    return spark.createDataFrame(rows, ["address", "dex_name"])
