import ccxt
import psycopg2
import logging
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Initialize exchanges
exchanges = ["binance", "kraken", "bitfinex"]
exchange_instances = {ex: getattr(ccxt, ex)() for ex in exchanges}

# Database connection
conn = psycopg2.connect("dbname=trading user=postgres password=yourpassword")
cursor = conn.cursor()

# Fetch Prices and Fees
def fetch_prices_and_fees():
    prices, fees = {}, {}
    for name, exchange in exchange_instances.items():
        try:
            ticker = exchange.fetch_ticker('BTC/USDT')
            prices[name] = ticker['last']
            fees[name] = exchange.fees['trading']['maker']
        except Exception as e:
            logging.error(f"Error fetching data from {name}: {e}")
    return prices, fees

# Define Arbitrage Calculation Functions
def calculate_simple_arbitrage_with_fees(prices, fees):
    best_buy = min(prices, key=prices.get)
    best_sell = max(prices, key=prices.get)
    buy_price, sell_price = prices[best_buy], prices[best_sell]
    buy_fee, sell_fee = fees[best_buy], fees[best_sell]
    profit = (sell_price * (1 - sell_fee)) - (buy_price * (1 + buy_fee))
    return profit, best_buy, best_sell

def execute_trade(buy_exchange, sell_exchange, trading_pair, amount):
    try:
        buy_exchange.create_market_order(trading_pair, 'buy', amount)
        sell_exchange.create_market_order(trading_pair, 'sell', amount)
        logging.info(f"Trade executed: Buy from {buy_exchange.id}, Sell on {sell_exchange.id}")
    except Exception as e:
        logging.error(f"Trade execution failed: {e}")

# Portfolio Update
def update_portfolio(exchange, trading_pair, position, cost_basis, unrealized_pnl, realized_pnl):
    cursor.execute(
        """
        INSERT INTO portfolio (exchange, trading_pair, position, cost_basis, unrealized_pnl, realized_pnl)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (exchange, trading_pair) DO UPDATE
        SET position = EXCLUDED.position, cost_basis = EXCLUDED.cost_basis,
            unrealized_pnl = EXCLUDED.unrealized_pnl, realized_pnl = EXCLUDED.realized_pnl;
        """,
        (exchange, trading_pair, position, cost_basis, unrealized_pnl, realized_pnl)
    )
    conn.commit()

# Main Loop
while True:
    try:
        prices, fees = fetch_prices_and_fees()
        profit, buy_exchange, sell_exchange = calculate_simple_arbitrage_with_fees(prices, fees)
        if profit > 0:
            execute_trade(exchange_instances[buy_exchange], exchange_instances[sell_exchange], 'BTC/USDT', 0.01)
        time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")
        break
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
