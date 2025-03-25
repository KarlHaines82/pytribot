# config.py

DB_CONFIG = {
    "dbname": "karl",
    "user": "karl",
    "password": "b0mb3r",
    "host": "localhost",
    "port": ""
}

EXCHANGES = [
    "binanceus",
    "kraken",
    "coinbase",
    # Add more exchanges as needed
]

TRADING_PAIRS = [
    "BTC/USDT",
    "ETH/USDT",
    "LTC/BTC",
    # Add more trading pairs as needed
]

# Set trading parameters
TRADE_AMOUNT = 0.01  # BTC
PROFIT_THRESHOLD = 0.5  # 0.5% profit thresholdS
