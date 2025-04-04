import ccxt

__name__ = "bot_config"

sentiment_api_url = "https://api.somecryptonerds.ai/"

exchanges = {
    "binanceus": ccxt.binanceus(
        {
            "apiKey": "YOUR_BINANCE_API_KEY",
            "secret": "YOUR_BINANCE_SECRET_KEY",
        }
    ),
    "coinbase": ccxt.coinbase(
        {
            "papiKey": "YOUR_COINBASEPRO_API_KEY",
            "psecret": "YOUR_COINBASEPRO_SECRET_KEY",
            "password": "YOUR_COINBASEPRO_PASSWORD",
        }
    ),
    "kraken": ccxt.kraken(
        {
            "apiKey": "YOUR_KRAKEN_API_KEY",
            "secret": "YOUR_KRAKEN_SECRET_KEY",
        }
    ),
}

db_params = {
    "dbname": "karl",
    "user": "karl",
    "password": "b0mb3r5824",
    "host": "localhost",
    "port": "5432",
}

min_profit_percentage = 0.2
pair1 = "BTC/USDT"
pair2 = "ETH/BTC"
pair3 = "ETH/USDT"
