from pyspark.sql import SparkSession


KNOWN_LABELS = {

    # =========================================================
    # ================= CENTRALIZED EXCHANGES =================
    # =========================================================

    # Binance
    "0x28c6c06298d514db089934071355e5743bf21d60": ("Binance 14", "CEX"),
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549": ("Binance 15", "CEX"),
    "0xdfd5293d8e347dfe59e90efd55b2956a1343963d": ("Binance 16", "CEX"),
    "0xf977814e90da44bfa03b6295a0616a897441acec": ("Binance Hot Wallet", "CEX"),
    "0x5a52e96bacdabb82fd05763e25335261b270efcb": ("Binance 28", "CEX"),

    # Coinbase
    "0x71660c4005ba85c37ccec55d0c4493e66fe775d3": ("Coinbase 1", "CEX"),
    "0xa9d1e08c7793af67e9d92fe308d5697fb81d3e43": ("Coinbase 10", "CEX"),
    "0x503828976d22510aad0201ac7ec88293211d23da": ("Coinbase 12", "CEX"),
    "0xddfabcdc4d8ffc6d5beaf154f18b778f892a0740": ("Coinbase 23", "CEX"),
    "0x3cd751e6b0078be393132286c442345e5dc49699": ("Coinbase 33", "CEX"),

    # Kraken
    "0x2910543af39aba0cd09dbb2d50200b3e800a63d2": ("Kraken 1", "CEX"),
    "0xc6bed363b30df7f35b601a5547fe56cd31ec63da": ("Kraken 8", "CEX"),

    # OKX
    "0x6cc5f688a315f3dc28a7781717a9a798a59fda7b": ("OKX 1", "CEX"),
    "0x5041ed759dd4afc3a72b8192c143f72f4724081a": ("OKX 7", "CEX"),

    # KuCoin
    "0x2b5634c42055806a59e9107ed44d43c426e58258": ("KuCoin 1", "CEX"),

    # Bitfinex
    "0x742d35cc6634c0532925a3b844bc454e4438f44e": ("Bitfinex Cold Wallet", "CEX"),

    # Bybit
    "0xf89d7b9c864f589bbf53a82105107622b35eaa40": ("Bybit Hot Wallet", "CEX"),

    # Gate.io
    "0x0d0707963952f2fba59dd06f2b425ace40b492fe": ("Gate.io Hot Wallet", "CEX"),

    # HTX (Huobi)
    "0xab5c66752a9e8167967685f1450532fb96d5d24f": ("HTX Hot Wallet", "CEX"),

    # Upbit
    "0xcc6411d9501a4e21a24d5ba1860d5ddaf2bc6de2": ("Upbit 2", "CEX"),

    # Bithumb
    "0xb2ebc9b3a788afb1e942ed65b59e9e49a1ee500d": ("Bithumb 1", "CEX"),
    "0x3282772591605330a84e9a7e0a2d592a832f0857": ("Bithumb 3", "CEX"),


    # =========================================================
    # ====================== DEX / DEFI =======================
    # =========================================================

    # Uniswap
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d": ("Uniswap V2 Router", "DEX"),
    "0xe592427a0aece92de3edee1f18e0157c05861564": ("Uniswap V3 Router", "DEX"),
    "0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45": ("Uniswap Universal Router", "DEX"),
    "0xc36442b4a4522e871399cd717abdd847ab11fe88": ("Uniswap V3 Position Manager", "DEX"),

    # SushiSwap
    "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f": ("SushiSwap Router", "DEX"),

    # 1inch
    "0x1111111254fb6c44bac0bed2854e76f90643097d": ("1inch Router V4", "DEX"),
    "0x1111111254eeb25477b68fb85ed929f73a960582": ("1inch Router V5", "DEX"),

    # PancakeSwap (Ethereum)
    "0xeff92a263d31888d860bd50809a8d171709b7b1c": ("PancakeSwap Router", "DEX"),

    # Balancer
    "0xba12222222228d8ba445958a75a0704d566bf2c8": ("Balancer Vault", "DEX"),

    # Curve
    "0xd533a949740bb3306d119cc777fa900ba034cd52": ("Curve DAO Token", "DeFi"),

    # Aave
    "0x7d2768de32b0b80b7a3454c06bdac94a69ddc7a9": ("Aave V2 Lending Pool", "DeFi"),

    # Compound
    "0x3d9819210a31b4961b30ef54be2aed79b9c9cd3b": ("Compound Comptroller", "DeFi"),

    # MakerDAO
    "0x35d1b3f3d7966a1dfe207aa4514c12a259a0492b": ("MakerDAO Proxy Registry", "DeFi"),

    # Lido
    "0xae7ab96520de3a18e5e111b5eaab095312d7fe84": ("Lido stETH", "DeFi"),


    # =========================================================
    # ===================== TOKEN CONTRACTS ===================
    # =========================================================

    # Stablecoins
    "0xdac17f958d2ee523a2206206994597c13d831ec7": ("Tether USD (USDT)", "Token"),
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": ("USD Coin (USDC)", "Token"),
    "0x6b175474e89094c44da98b954eedeac495271d0f": ("DAI Stablecoin", "Token"),

    # Wrapped Assets
    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": ("Wrapped Ether (WETH)", "Token"),
    "0x2260fac5e5542a773aa44fbcfedf7c193bc2c599": ("Wrapped BTC (WBTC)", "Token"),

    # Governance / Utility
    "0x514910771af9ca656af840dff83e8264ecf986ca": ("Chainlink Token (LINK)", "Token"),
    "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": ("Uniswap Token (UNI)", "Token"),


    # =========================================================
    # ==================== BRIDGE / LAYER2 ====================
    # =========================================================

    # Polygon
    "0x40ec5b33f54e0e8a33a975908c5ba1c14e5bbbdf": ("Polygon ERC20 Bridge", "Bridge"),

    # Optimism
    "0x99c9fc46f92e8a1c0dec1b1747d010903e884be1": ("Optimism Gateway", "Bridge"),

    # Arbitrum
    "0x4dbd4fc535ac27206064b68ffcf827b0a60bab3f": ("Arbitrum ERC20 Bridge", "Bridge"),

    # Hop
    "0x3666f603cc164936c1b87e207f36beba4ac5f18a": ("Hop Protocol Bridge", "Bridge"),

    # Wormhole
    "0x3ee18b2214aff97000d974cf647e7c347e8fa585": ("Wormhole Token Bridge", "Bridge"),

    # Stargate
    "0x8731d54e9d02c286767d56ac03e8037c07e01e98": ("Stargate Router", "Bridge"),


    # =========================================================
    # ====================== STAKING ==========================
    # =========================================================

    # ETH2 Deposit
    "0x00000000219ab540356cbb839cbe05303d7705fa": ("ETH2 Deposit Contract", "Staking"),

    # Rocket Pool
    "0xae78736cd615f374d3085123a210448e74fc6393": ("Rocket Pool rETH", "Staking"),

    # Coinbase Wrapped Staked ETH
    "0xbe9895146f7af43049ca1c1ae358b0541ea49704": ("Coinbase Wrapped Staked ETH", "Staking"),


    # =========================================================
    # ======================= MEV =============================
    # =========================================================

    # Flashbots
    "0x000000000000bbbbb8f225d8f3e2c72c129c28f": ("Flashbots Builder", "MEV"),

    # BeaverBuild
    "0x95222290dd7278aa3ddd389cc1e1d165cc4bafe5": ("BeaverBuild", "MEV"),

    # Wintermute
    "0x00000000ae347930bd1e7b0f35588b92280f9e75": ("Wintermute", "MarketMaker"),


    # =========================================================
    # ======================= MIXER ===========================
    # =========================================================

    # Tornado Cash
    "0xd90e2f925da726b50c4ed8d0fb90ad053324f31b": ("Tornado Cash Router", "Mixer"),


    # =========================================================
    # ======================= TREASURY ========================
    # =========================================================

    # Tether Treasury
    "0x5754284f345afc66a98fbb0a0afe71e0f007b949": ("Tether Treasury", "Treasury"),

    # Circle Treasury
    "0x55fe002aeff02f77364de339a1292923a15844b8": ("Circle Treasury", "Treasury"),


    # =========================================================
    # ======================== ORACLE =========================
    # =========================================================

    # Chainlink ETH/USD
    "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419": ("Chainlink ETH/USD Oracle", "Oracle"),


    # =========================================================
    # ========================= NFT ===========================
    # =========================================================

    # OpenSea Wyvern
    "0x7be8076f4ea4a4ad08075c2508e481d6c946d12b": ("OpenSea Wyvern Exchange", "NFT"),

    # OpenSea Seaport
    "0x00000000006c3852cbef3e08e8df289169ede581": ("OpenSea Seaport", "NFT"),

    # Blur
    "0x39da41747a83aee658334415666f3ef92dd0d541": ("Blur Exchange", "NFT"),
    "0x000000000000ad05ccc4f10045630fb830b95127": ("Blur Pool", "NFT"),

    # ENS
    "0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85": ("ENS NFT", "NFT"),


    # =========================================================
    # ========================= INFRA =========================
    # =========================================================

    # Gnosis Safe
    "0xa6b71e26c5e0845f74c812102ca7114b6a896ab2": ("Gnosis Safe Proxy Factory", "Infra"),


    # =========================================================
    # ====================== BURN / NULL ======================
    # =========================================================

    "0x000000000000000000000000000000000000dead": ("Dead Address", "Burn"),
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
