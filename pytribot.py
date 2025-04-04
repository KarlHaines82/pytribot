import ccxt
import ccxt.async_support as ccxt_async
import asyncio
import psycopg2
import pandas as pd
# import numpy as np
import matplotlib.pyplot as plt
import aiohttp  # For async HTTP requests
import json  # For parsing JSON
from datetime import datetime, timedelta
from logger_config import setup_logger

# get config
from bot_config import exchanges, db_params, sentiment_api_url, min_profit_percentage
from bot_config import pair1, pair2, pair3

# Initialize logger
logger = setup_logger()


class Triobot:
    def __init__(self, exchanges_config, db_params_config, sentiment_url, min_profit):
        """Initialize the Triobot with required parameters."""
        logger.info("Initializing Triobot")
        self.db_params = db_params_config
        self.sentiment_api_url = sentiment_url
        self.min_profit_percentage = min_profit
        self.exchanges = {}  # Synchronous instances (optional, for sync tasks if any)
        self.async_exchanges = {}  # Asynchronous instances for core logic

        # Initialize exchange instances (sync and async)
        for exchange_name in exchanges_config:
            try:
                # Sync setup (optional, keep if needed for sync tasks)
                # exchange_class = getattr(ccxt, exchange_name)
                # self.exchanges[exchange_name] = exchange_class()
                # logger.info(f"Initialized sync {exchange_name} exchange")

                # Async setup
                if not hasattr(ccxt_async, exchange_name):
                    logger.error(
                        f"Async support not found for exchange: {exchange_name}. Skipping."
                    )
                    continue
                async_exchange_class = getattr(ccxt_async, exchange_name)
                self.async_exchanges[exchange_name] = async_exchange_class()
                logger.info(f"Initialized async {exchange_name} exchange instance")

            except ccxt.ExchangeNotFound:
                logger.error(f"Exchange '{exchange_name}' not found by ccxt. Skipping.")
            except Exception as e:
                logger.error(f"Failed to initialize {exchange_name} exchange: {str(e)}")
                # Decide if initialization should fail completely
                # raise

        if not self.async_exchanges:
            logger.critical(
                "No async exchanges were successfully initialized. Bot cannot run."
            )
            raise RuntimeError("Failed to initialize any async exchanges.")

        # Initialize database (synchronously)
        try:
            logger.info("Initializing database")
            self.create_tables()
            logger.info("Database initialized successfully")
        except Exception as e:
            logger.critical(f"Failed to initialize database: {str(e)}")
            raise

        logger.info("Triobot initialization completed.")

    # def load_markets(self):
    #     """Load markets for synchronous exchanges (if used)."""
    #     try:
    #         logger.info("Loading markets for sync exchanges")
    #         for exchange_name, exchange in self.exchanges.items():
    #             try:
    #                 exchange.load_markets()
    #                 logger.info(f"Successfully loaded sync markets for {exchange_name}")
    #             except Exception as e:
    #                 logger.error(f"Failed to load sync markets for {exchange_name}: {str(e)}")
    #     except Exception as e:
    #         logger.error(f"Error in load_markets (sync): {str(e)}")

    async def load_async_markets(self):
        """Load markets for all configured asynchronous exchanges."""
        logger.info("Loading markets for async exchanges...")
        tasks = []
        for exchange_name, exchange in self.async_exchanges.items():
            logger.debug(f"Creating task to load markets for {exchange_name}")
            tasks.append(self._load_market_safe(exchange_name, exchange))

        results = await asyncio.gather(*tasks)
        successful_loads = sum(1 for r in results if r)
        logger.info(
            f"Finished loading async markets. Successfully loaded for {successful_loads}/{len(self.async_exchanges)} exchanges."
        )
        if successful_loads == 0 and self.async_exchanges:
            logger.error("Failed to load markets for any async exchange.")
            # Consider raising an error or handling this state

    async def _load_market_safe(self, exchange_name, exchange):
        """Safely load markets for a single async exchange."""
        try:
            await exchange.load_markets()
            logger.info(f"Successfully loaded async markets for {exchange_name}")
            return True
        except ccxt.NetworkError as e:
            logger.error(f"Network error loading markets for {exchange_name}: {str(e)}")
            # Optionally remove exchange from self.async_exchanges if unusable?
            # del self.async_exchanges[exchange_name]
            return False
        except ccxt.ExchangeError as e:
            logger.error(
                f"Exchange error loading markets for {exchange_name}: {str(e)}"
            )
            return False
        except Exception as e:
            logger.error(f"Failed to load async markets for {exchange_name}: {str(e)}")
            return False

    def create_tables(self):
        """Create necessary database tables if they don't exist (Synchronous)."""
        try:
            logger.info("Creating database tables")
            # Use context manager for connection and cursor
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        CREATE TABLE IF NOT EXISTS arbitrage_opportunities (
                            id SERIAL PRIMARY KEY,
                            type VARCHAR(50),
                            exchange1 VARCHAR(50),
                            exchange2 VARCHAR(50),
                            symbol1 VARCHAR(50),
                            symbol2 VARCHAR(50),
                            price1 FLOAT,
                            price2 FLOAT,
                            profit_percentage FLOAT,
                            notes TEXT,
                            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """
                    )
                # No explicit commit needed with context manager for connection
            logger.info("Database tables checked/created successfully")
        except psycopg2.Error as e:
            logger.error(f"Database error creating tables: {str(e)}")
            raise  # Raise DB errors during init
        except Exception as e:
            logger.error(f"Failed to create database tables: {str(e)}")
            raise

    async def get_ticker_async(self, exchange_name, symbol):
        """Asynchronously fetch ticker data for a symbol from an exchange."""
        exchange = self.async_exchanges.get(exchange_name)
        if not exchange:
            logger.error(f"Async exchange instance not found for {exchange_name}")
            return exchange_name, symbol, None
        try:
            logger.debug(f"Fetching ticker async for {symbol} from {exchange_name}")
            ticker = await exchange.fetch_ticker(symbol)
            logger.debug(
                f"Successfully retrieved ticker async for {symbol} from {exchange_name}"
            )
            return exchange_name, symbol, ticker
        except ccxt.NetworkError as e:
            logger.warning(
                f"Network error fetching ticker for {symbol} on {exchange_name}: {str(e)}"
            )
            return exchange_name, symbol, None
        except ccxt.ExchangeError as e:
            logger.warning(
                f"Exchange error fetching ticker for {symbol} on {exchange_name}: {str(e)}"
            )
            return exchange_name, symbol, None
        except Exception as e:
            logger.error(
                f"Generic error fetching ticker async for {symbol} on {exchange_name}: {str(e)}"
            )
            return exchange_name, symbol, None

    async def fetch_all_tickers(self, symbol):
        """Fetch all tickers for a symbol across async exchanges asynchronously."""
        logger.info(f"Fetching all tickers async for {symbol}")
        tasks = [
            self.get_ticker_async(exchange_name, symbol)
            for exchange_name in self.async_exchanges
        ]
        results = await asyncio.gather(*tasks)
        tickers = {
            name: ticker
            for name, sym, ticker in results
            if ticker is not None and sym == symbol
        }
        logger.info(f"Successfully fetched {len(tickers)} tickers async for {symbol}")
        return tickers

    async def record_opportunity(
        self,
        type_,
        exchange1,
        exchange2,
        symbol1,
        symbol2,
        price1,
        price2,
        profit_percentage,
        notes,
    ):
        """Record an arbitrage opportunity in the database asynchronously using executor."""
        try:
            logger.info(
                f"Recording {type_} arbitrage opportunity: {exchange1}/{exchange2} - {symbol1}/{symbol2} - Profit: {profit_percentage:.4f}"
            )
            loop = asyncio.get_running_loop()

            def _sync_db_call():
                try:  # Inner try/except for DB specific errors
                    with psycopg2.connect(**self.db_params) as conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO arbitrage_opportunities
                                (type, exchange1, exchange2, symbol1, symbol2, price1, price2,
                                 profit_percentage, notes)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                                (
                                    type_,
                                    exchange1,
                                    exchange2,
                                    symbol1,
                                    symbol2,
                                    price1,
                                    price2,
                                    profit_percentage,
                                    notes,
                                ),
                            )
                    logger.debug("DB insert executed successfully")
                except psycopg2.Error as db_err:
                    logger.error(f"Database error during record opportunity: {db_err}")
                    # Decide if this should propagate or just be logged

            await loop.run_in_executor(None, _sync_db_call)
            logger.info("Successfully recorded arbitrage opportunity in executor")

        except Exception as e:
            # Catch errors from run_in_executor or other async issues
            logger.error(
                f"Failed to record arbitrage opportunity (async wrapper): {str(e)}"
            )

    async def get_sentiment(self, symbol_base):
        """Asynchronously get market sentiment for a given symbol base (e.g., BTC) using aiohttp."""
        if not self.sentiment_api_url:
            logger.warning(
                "Sentiment API URL not configured. Skipping sentiment check."
            )
            return 0  # Return neutral if not configured

        try:
            logger.debug(f"Fetching sentiment async for {symbol_base}")
            # Consider using a shared aiohttp session for efficiency if called frequently
            async with aiohttp.ClientSession() as session:
                # Ensure URL is formed correctly, handle potential trailing slashes
                url = f"{self.sentiment_api_url.rstrip('/')}/{symbol_base}"
                # Add timeout to prevent hanging indefinitely
                async with session.get(url) as response:  # 10 second timeout
                    response.raise_for_status()
                    sentiment_data = await response.json()

                    if "sentiment" not in sentiment_data:
                        logger.error(
                            f"Invalid sentiment data format for {symbol_base}: 'sentiment' key missing. Data: {sentiment_data}"
                        )
                        return -1  # Indicate error

                    sentiment = sentiment_data["sentiment"]
                    # Basic validation: Check if sentiment is a number
                    if not isinstance(sentiment, (int, float)):
                        logger.error(
                            f"Invalid sentiment value type for {symbol_base}: {type(sentiment)}. Data: {sentiment_data}"
                        )
                        return -1  # Indicate error

                    logger.info(
                        f"Successfully retrieved sentiment async for {symbol_base}: {sentiment}"
                    )
                    return sentiment

        except aiohttp.ClientResponseError as e:
            logger.error(
                f"HTTP error fetching sentiment for {symbol_base}: Status {e.status}, Message: {e.message}, URL: {url}"
            )
            return -1
        except aiohttp.ClientConnectionError as e:
            logger.error(
                f"Connection error fetching sentiment for {symbol_base}: {str(e)}, URL: {url}"
            )
            return -1
        except asyncio.TimeoutError:
            logger.error(f"Timeout fetching sentiment for {symbol_base} from {url}")
            return -1
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to decode JSON sentiment data for {symbol_base}: {str(e)}, URL: {url}"
            )
            return -1
        except (KeyError, ValueError) as e:
            logger.error(
                f"Invalid sentiment data format or value for {symbol_base}: {str(e)}, URL: {url}"
            )
            return -1
        except Exception as e:
            logger.error(
                f"Generic error fetching sentiment async for {symbol_base}: {str(e)}, URL: {url}"
            )
            return -1

    async def triangular_arbitrage(self, symbol1, symbol2, symbol3):
        """
        Identify triangular arbitrage opportunities within a single exchange.
        Uses asynchronous fetching.
        NOTE: Calculation logic assumes a specific path (e.g., USD->Sym1->Sym2->USD via Sym3)
        and needs verification/generalization based on actual pair structures (A/B, B/C, C/A).
        """
        logger.info(
            f"Starting async triangular arbitrage analysis for {symbol1}-{symbol2}-{symbol3}"
        )
        if not all([symbol1, symbol2, symbol3]):
            logger.warning(
                "One or more symbols for triangular arbitrage are invalid/missing. Skipping."
            )
            return

        for exchange_name in list(
            self.async_exchanges.keys()
        ):  # Iterate over keys copy in case of modification
            exchange = self.async_exchanges.get(exchange_name)
            if not exchange:
                continue  # Should not happen if keys() is used

            try:
                logger.debug(
                    f"Checking triangular arbitrage on {exchange_name} for {symbol1}, {symbol2}, {symbol3}"
                )
                tasks = [
                    self.get_ticker_async(exchange_name, symbol1),
                    self.get_ticker_async(exchange_name, symbol2),
                    self.get_ticker_async(exchange_name, symbol3),
                ]
                results = await asyncio.gather(*tasks)
                tickers = {sym: ticker for name, sym, ticker in results if ticker}

                ticker1 = tickers.get(symbol1)
                ticker2 = tickers.get(symbol2)
                ticker3 = tickers.get(symbol3)

                if not all([ticker1, ticker2, ticker3]):
                    logger.debug(
                        f"Missing ticker data for one or more symbols on {exchange_name}. Skipping."
                    )
                    continue

                # --- Calculation Logic (NEEDS VALIDATION/REFINEMENT) ---
                try:
                    # Ensure prices are valid numbers and ask > 0 for division
                    ask1 = float(ticker1.get("ask", 0))
                    bid2 = float(ticker2.get("bid", 0))  # Check if this should be ask2
                    bid3 = float(ticker3.get("bid", 0))

                    if ask1 <= 0:
                        logger.debug(
                            f"Invalid ask price for {symbol1} on {exchange_name}: {ask1}"
                        )
                        continue

                    # Example Path: Start with 100 units of quote currency of symbol1 (e.g., USD in BTC/USD)
                    # 1. Buy base of symbol1 (BTC) with quote (USD): amount_base1 = 100 / ask1 (BTC/USD)
                    # 2. Use base1 (BTC) to acquire base2 (ETH) via symbol2 (ETH/BTC): amount_base2 = amount_base1 * bid2 (ETH/BTC)? Or /ask2? Needs check. Let's assume * bid2 for now.
                    # 3. Sell base2 (ETH) for quote currency via symbol3 (ETH/USD): final_amount = amount_base2 * bid3 (ETH/USD)

                    initial_amount = 100  # Assuming start with quote currency
                    amount1 = (
                        initial_amount / ask1
                    )  # Amount of base currency of symbol1
                    # This step is highly dependent on pair structure. If symbol2 is BASE2/BASE1 (e.g. ETH/BTC)
                    # To get BASE2 from BASE1, you sell BASE1, buy BASE2. You cross the spread.
                    # Price is BASE1 per BASE2. Amount_BASE2 = Amount_BASE1 / ask_price(BASE2/BASE1)
                    # Let's assume ticker2 price is quote/base (BTC per ETH for ETH/BTC)
                    ask2 = float(ticker2.get("ask", 0))
                    if ask2 <= 0:
                        logger.debug(
                            f"Invalid ask price for {symbol2} on {exchange_name}: {ask2}"
                        )
                        continue
                    amount2 = (
                        amount1 / ask2
                    )  # Amount of base currency of symbol2 (e.g., ETH)

                    # Sell base2 (ETH) for quote currency (USD) using symbol3 (ETH/USD)
                    # Price is quote/base (USD per ETH). Amount_Quote = Amount_Base * bid_price(Base/Quote)
                    final_amount = amount2 * bid3  # Final amount in quote currency

                    # Calculate fees
                    fee1 = (
                        initial_amount * 0.001
                    )  # Example fee of 0.1% on initial amount
                    fee2 = amount1 * 0.001  # Example fee of 0.1% on amount1
                    fee3 = amount2 * 0.001  # Example fee of 0.1% on amount2

                    # Adjust final amount for fees
                    final_amount -= fee1 + fee2 + fee3

                    profit_percentage = (final_amount - initial_amount) / initial_amount

                    if profit_percentage > self.min_profit_percentage:
                        notes = f"Path: {symbol1}(buy@{ask1}) -> {symbol2}(buy@{ask2}) -> {symbol3}(sell@{bid3})"
                        logger.info(
                            f"Async Triangular Arbitrage Opportunity on {exchange_name}: "
                            f"Profit: {profit_percentage * 100:.4f}%. {notes}"
                        )
                        await self.record_opportunity(
                            "triangular",
                            exchange_name,
                            exchange_name,
                            symbol1,
                            symbol2,  # Record first two symbols
                            ask1,
                            ask2,  # Record first two prices used
                            profit_percentage,
                            notes,
                        )

                except (KeyError, TypeError, ValueError) as e:
                    logger.warning(
                        f"Data format error during triangular calculation for {exchange_name} ({symbol1},{symbol2},{symbol3}): {str(e)}. Tickers: {ticker1}, {ticker2}, {ticker3}"
                    )
                except ZeroDivisionError:
                    logger.warning(
                        f"Zero division error during triangular calculation for {exchange_name} ({symbol1},{symbol2},{symbol3}). Tickers: {ticker1}, {ticker2}, {ticker3}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error calculating async triangular arbitrage for {exchange_name}: {str(e)}"
                    )

            except ccxt.NetworkError as e:
                logger.warning(
                    f"Network error processing exchange {exchange_name} for triangular: {str(e)}"
                )
                continue
            except ccxt.ExchangeError as e:
                logger.warning(
                    f"Exchange error processing exchange {exchange_name} for triangular: {str(e)}"
                )
                continue
            except Exception as e:
                logger.error(
                    f"Generic error processing exchange {exchange_name} for triangular: {str(e)}"
                )
                continue
        logger.info(
            f"Finished triangular arbitrage analysis for {symbol1}-{symbol2}-{symbol3}"
        )

    async def spatial_arbitrage(self, symbol):
        """
        Identify spatial arbitrage opportunities between exchanges for a symbol.
        Uses asynchronous fetching and sentiment check.
        """
        logger.info(f"Starting async spatial arbitrage analysis for {symbol}")
        if not symbol:
            logger.warning("Invalid symbol provided for spatial arbitrage. Skipping.")
            return

        try:
            exchange_tickers = await self.fetch_all_tickers(symbol)

            if len(exchange_tickers) < 2:
                logger.info(
                    f"Need at least 2 exchanges with tickers for spatial arbitrage on {symbol}, found {len(exchange_tickers)}. Skipping."
                )
                return

            # Get sentiment for the base currency (e.g., 'BTC' from 'BTC/USD')
            symbol_base = symbol.split("/")[0] if "/" in symbol else symbol
            sentiment = await self.get_sentiment(symbol_base)
            logger.debug(f"Sentiment for {symbol_base}: {sentiment}")

            exchange_names = list(exchange_tickers.keys())
            for i in range(len(exchange_names)):
                for j in range(i + 1, len(exchange_names)):
                    exchange1_name = exchange_names[i]
                    exchange2_name = exchange_names[j]
                    ticker1 = exchange_tickers[exchange1_name]
                    ticker2 = exchange_tickers[exchange2_name]

                    try:
                        # Ensure tickers and prices are valid
                        ask1 = float(ticker1.get("ask", 0))
                        bid1 = float(ticker1.get("bid", 0))
                        ask2 = float(ticker2.get("ask", 0))
                        bid2 = float(ticker2.get("bid", 0))

                        if not all([ask1 > 0, bid1 > 0, ask2 > 0, bid2 > 0]):
                            logger.debug(
                                f"Invalid ticker prices for {symbol} between {exchange1_name} and {exchange2_name}. Skipping pair."
                            )
                            continue

                        # Opportunity: Buy on Ex1 (pay ask1), Sell on Ex2 (receive bid2)
                        if ask1 < bid2:
                            profit_percentage = (bid2 - ask1) / ask1
                            if profit_percentage > self.min_profit_percentage:
                                notes = f"Sentiment: {sentiment}"
                                if sentiment > -0.5:
                                    logger.info(
                                        f"Async Spatial Opportunity: Buy {symbol} on {exchange1_name} @ {ask1}, Sell on {exchange2_name} @ {bid2}. Profit: {profit_percentage * 100:.4f}%. {notes}"
                                    )
                                    await self.record_opportunity(
                                        "spatial",
                                        exchange1_name,
                                        exchange2_name,
                                        symbol,
                                        symbol,
                                        ask1,
                                        bid2,
                                        profit_percentage,
                                        notes,
                                    )
                                else:
                                    logger.info(
                                        f"Spatial opportunity found but skipped (sentiment <= -0.5): Buy {exchange1_name}@{ask1}, Sell {exchange2_name}@{bid2}. Profit: {profit_percentage * 100:.4f}%. {notes}"
                                    )

                        # Opportunity: Buy on Ex2 (pay ask2), Sell on Ex1 (receive bid1)
                        if ask2 < bid1:
                            profit_percentage = (bid1 - ask2) / ask2
                            if profit_percentage > self.min_profit_percentage:
                                notes = f"Sentiment: {sentiment}"
                                if sentiment > -0.5:
                                    logger.info(
                                        f"Async Spatial Opportunity: Buy {symbol} on {exchange2_name} @ {ask2}, Sell on {exchange1_name} @ {bid1}. Profit: {profit_percentage * 100:.4f}%. {notes}"
                                    )
                                    await self.record_opportunity(
                                        "spatial",
                                        exchange2_name,
                                        exchange1_name,
                                        symbol,
                                        symbol,
                                        ask2,
                                        bid1,
                                        profit_percentage,
                                        notes,
                                    )
                                else:
                                    logger.info(
                                        f"Spatial opportunity found but skipped (sentiment <= -0.5): Buy {exchange2_name}@{ask2}, Sell {exchange1_name}@{bid1}. Profit: {profit_percentage * 100:.4f}%. {notes}"
                                    )

                    except (KeyError, TypeError, ValueError) as e:
                        logger.warning(
                            f"Data format error during spatial calculation for {symbol} between {exchange1_name} and {exchange2_name}: {str(e)}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error calculating spatial arbitrage for {symbol} between {exchange1_name} and {exchange2_name}: {str(e)}"
                        )

        except Exception as e:
            logger.error(
                f"Error in async spatial arbitrage analysis for {symbol}: {str(e)}"
            )
        logger.info(f"Finished spatial arbitrage analysis for {symbol}")

    async def statistical_arbitrage(self, symbol):
        """
        Fetches historical arbitrage data for analysis (placeholder for actual stat arb logic).
        Runs DB query in executor.
        """
        logger.info(
            f"Starting async statistical arbitrage analysis (data fetch) for {symbol}"
        )
        if not symbol:
            logger.warning(
                "Invalid symbol provided for statistical arbitrage. Skipping."
            )
            return None

        try:
            loop = asyncio.get_running_loop()

            def _sync_db_call():
                try:  # Inner try/except
                    with psycopg2.connect(**self.db_params) as conn:
                        with conn.cursor() as cur:
                            query = """
                                SELECT * FROM arbitrage_opportunities
                                WHERE (symbol1 = %s OR symbol2 = %s)
                                  AND timestamp > NOW() - INTERVAL '1 day'
                                ORDER BY timestamp DESC
                            """
                            cur.execute(query, (symbol, symbol))
                            rows = cur.fetchall()
                            if not rows:
                                return None
                            colnames = [desc[0] for desc in cur.description]
                            return pd.DataFrame(rows, columns=colnames)
                except psycopg2.Error as db_err:
                    logger.error(
                        f"Database error during statistical fetch for {symbol}: {db_err}"
                    )
                    return None  # Return None on DB error

            df = await loop.run_in_executor(None, _sync_db_call)

            if df is None or df.empty:
                logger.info(f"No recent historical arbitrage data found for {symbol}.")
                return None
            else:
                logger.info(
                    f"Retrieved {len(df)} historical arbitrage records for {symbol}"
                )
                # --- Placeholder for actual statistical analysis ---
                # Example: Analyze profit_percentage distribution, time patterns, etc.
                # mean_profit = df['profit_percentage'].mean()
                # logger.info(f"Historical mean profit for {symbol} (last day): {mean_profit:.4f}")
                # ----------------------------------------------------
                return df  # Return the dataframe for potential further use

        except Exception as e:
            logger.error(
                f"Error in async statistical arbitrage analysis for {symbol}: {str(e)}"
            )
            return None

    def plot_performance(self, filename="performance_plot.png"):
        """Generate and save the performance plot (Synchronous DB access)."""
        logger.info(f"Generating performance plot to {filename}")
        try:
            with psycopg2.connect(**self.db_params) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """SELECT timestamp, profit_percentage
                        FROM arbitrage_opportunities
                        WHERE timestamp > NOW() - INTERVAL '7 days' -- Limit data for plot
                        ORDER BY timestamp"""
                    )
                    rows = cur.fetchall()

            if not rows:
                logger.warning("No data available for plotting in the last 7 days.")
                return

            df = pd.DataFrame(rows, columns=["timestamp", "profit_percentage"])
            if df.empty:
                logger.warning("DataFrame is empty after fetching data for plotting.")
                return

            plt.figure(figsize=(12, 6))
            # Ensure profit_percentage is numeric
            df["profit_percentage"] = pd.to_numeric(
                df["profit_percentage"], errors="coerce"
            )
            df.dropna(subset=["profit_percentage"], inplace=True)

            if df.empty:
                logger.warning(
                    "DataFrame is empty after cleaning non-numeric profit percentages."
                )
                return

            plt.plot(df["timestamp"], df["profit_percentage"] * 100)
            plt.title("Recorded Arbitrage Opportunities Over Time (Last 7 Days)")
            plt.xlabel("Time")
            plt.ylabel("Profit Percentage (%)")
            plt.xticks(rotation=45)
            plt.grid(True)
            plt.tight_layout()
            plt.savefig(filename)
            plt.close()  # Close the figure explicitly
            logger.info(f"Performance plot saved to {filename}")

        except psycopg2.Error as db_err:
            logger.error(f"Database error generating performance plot: {db_err}")
        except Exception as e:
            logger.error(f"Error generating performance plot: {str(e)}")

    async def close_exchanges(self):
        """Close all async exchange connections gracefully."""
        logger.info("Closing async exchange connections...")
        tasks = [
            exchange.close()
            for exchange in self.async_exchanges.values()
            if hasattr(exchange, "close")
        ]
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            closed_count = 0
            for i, result in enumerate(results):
                # Find corresponding exchange name (assuming order is preserved)
                exchange_name = list(self.async_exchanges.keys())[i]
                if isinstance(result, Exception):
                    logger.warning(f"Error closing exchange {exchange_name}: {result}")
                else:
                    logger.debug(f"Successfully closed exchange {exchange_name}")
                    closed_count += 1
            logger.info(
                f"Attempted to close {len(tasks)} exchanges, {closed_count} closed successfully."
            )
        else:
            logger.info("No async exchanges needed closing.")

    async def run(self, symbol1, symbol2, symbol3):
        """Run the bot's analysis cycle asynchronously."""
        while True:
            run_start_time = datetime.now()
            logger.info(f"Starting async bot run cycle at {run_start_time}")
            try:
                # Load markets for async exchanges first
                await self.load_async_markets()

                # Check if any markets were loaded successfully
                if not any(
                    ex.markets for ex in self.async_exchanges.values() if ex.markets
                ):
                    logger.error(
                        "No markets loaded for any async exchange. Cannot proceed."
                    )
                    return  # Exit run cycle if no markets loaded

                logger.info("Markets loaded. Starting arbitrage analysis...")

                # Define symbols to check based on config (ensure they are valid)
                symbols_to_check = {s for s in [symbol1, symbol2, symbol3] if s}
                triangular_symbols = (
                    symbol1,
                    symbol2,
                    symbol3,
                )  # Keep as tuple for triangular

                # --- Run Analyses ---
                # Run spatial and statistical analyses concurrently for relevant symbols
                analysis_tasks = []
                for s in symbols_to_check:
                    analysis_tasks.append(self.spatial_arbitrage(s))
                    analysis_tasks.append(self.statistical_arbitrage(s))  # Fetches data

                # Add triangular arbitrage task (if symbols are valid)
                if all(triangular_symbols):
                    analysis_tasks.append(
                        self.triangular_arbitrage(*triangular_symbols)
                    )
                else:
                    logger.warning(
                        "Skipping triangular arbitrage due to invalid/missing symbols."
                    )

                # Execute all analysis tasks concurrently
                await asyncio.gather(*analysis_tasks)

                logger.info("Analysis cycle completed.")

            except Exception as e:
                logger.error(
                    f"Critical error during async bot run cycle: {str(e)}",
                    exc_info=True,
                )
                # Depending on the error, might need specific handling or shutdown
            finally:
                run_end_time = datetime.now()
                logger.info(
                    f"Bot run cycle finished at {run_end_time}. Duration: {run_end_time - run_start_time}"
                )
                # Generate performance plot (sync call, runs after async tasks)
                try:
                    self.plot_performance()
                except Exception as plot_err:
                    logger.error(
                        f"Failed to generate performance plot in finally block: {plot_err}"
                    )

                # Ensure exchanges are closed
                await self.close_exchanges()

                # Add a delay before restarting the analysis cycle
                await asyncio.sleep(60)  # Sleep for 60 seconds before restarting
            logger.info("Exiting run method.")


# Main execution block
if __name__ == "__main__":
    logger.info("Starting Triobot application")
    try:
        # Initialize the bot
        triobot = Triobot(
            exchanges, db_params, sentiment_api_url, min_profit_percentage
        )

        # Run the main async loop
        logger.info("Running the bot...")
        asyncio.run(triobot.run(pair1, pair2, pair3))

    except RuntimeError as e:
        logger.critical(f"Bot initialization failed: {e}")
    except KeyboardInterrupt:
        logger.info("Bot stopped by user (KeyboardInterrupt).")
    except Exception as e:
        logger.critical(
            f"An unexpected error occurred at the top level: {str(e)}", exc_info=True
        )
    finally:
        logger.info("Triobot application finished.")
