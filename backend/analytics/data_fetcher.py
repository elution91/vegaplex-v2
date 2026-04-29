"""
Data Fetcher Module
Handles fetching options data from IBKR and YFinance
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import logging
import asyncio
import threading

try:
    import yfinance as yf
    yfinance = yf
except ImportError:
    yfinance = None
    yf = None

try:
    from ib_insync import IB, Stock, Option, util
except (ImportError, RuntimeError):
    IB = None

logger = logging.getLogger(__name__)


class DataFetcher:
    """Unified data fetcher supporting both IBKR and YFinance"""
    
    def __init__(self, config):
        self.config = config
        self.ib = None
        self.connected = False
        self._ib_lock = threading.Lock()
        self._last_connect_attempt = 0  # timestamp of last failed connection attempt
        
    def connect(self) -> bool:
        """Connect to data source"""
        if self.config.data_source.value == "ibkr":
            return self._connect_ibkr()
        else:
            return self._connect_yfinance()
    
    def _ensure_event_loop(self):
        """Ensure the IBKR event loop is available in the current thread."""
        if hasattr(self, '_ib_loop') and self._ib_loop:
            asyncio.set_event_loop(self._ib_loop)

    def _ensure_ibkr_connected(self) -> bool:
        """Check IBKR connection and reconnect if dropped. Must be called under _ib_lock."""
        import time
        if self.ib and self.ib.isConnected():
            return True

        # Cooldown: don't retry more than once per 10 seconds
        now = time.time()
        if now - self._last_connect_attempt < 10:
            return False

        self._last_connect_attempt = now
        logger.warning("IBKR not connected, attempting to connect...")
        try:
            if self.ib:
                try:
                    self.ib.disconnect()
                except Exception:
                    pass
            util.patchAsyncio()
            self.ib = IB()
            self.ib.connect(
                host=self.config.ibkr_host,
                port=self.config.ibkr_port,
                clientId=self.config.ibkr_client_id
            )
            self._ib_loop = asyncio.get_event_loop()
            self.ib.reqMarketDataType(3)
            self.connected = True
            logger.info("IBKR connected successfully")
            return True
        except Exception as e:
            logger.error(f"IBKR connection failed: {e}")
            return False

    def _connect_ibkr(self) -> bool:
        """Connect to IBKR"""
        if IB is None:
            raise ImportError("ib_insync not installed. Run: pip install ib_insync")

        try:
            util.patchAsyncio()  # Allow nested event loops (needed for threaded Dash callbacks)
            self.ib = IB()
            self.ib.connect(
                host=self.config.ibkr_host,
                port=self.config.ibkr_port,
                clientId=self.config.ibkr_client_id
            )
            self._ib_loop = asyncio.get_event_loop()
            self.ib.reqMarketDataType(3)  # 3 = delayed-frozen fallback
            self.connected = True
            logger.info("Connected to IBKR")
            return True

        except Exception as e:
            logger.error(f"Failed to connect to IBKR: {e}")
            return False
    
    def _connect_yfinance(self) -> bool:
        """Connect to YFinance (no connection needed)"""
        if yfinance is None:
            raise ImportError("yfinance not installed. Run: pip install yfinance")
        
        self.connected = True
        logger.info("Using YFinance data source")
        return True
    
    def get_options_chain(self, symbol: str) -> Optional[Dict]:
        """Get complete options chain for a symbol"""
        if self.config.data_source.value == "ibkr":
            return self._get_options_chain_ibkr(symbol)
        else:
            return self._get_options_chain_yfinance(symbol)
    
    def _get_options_chain_ibkr(self, symbol: str) -> Optional[Dict]:
        """Get options chain from IBKR (auto-reconnects if connection dropped)"""
        with self._ib_lock:
            self._ensure_event_loop()
            if not self._ensure_ibkr_connected():
                return None
            return self._get_options_chain_ibkr_locked(symbol)

    @staticmethod
    def _standard_strikes(strikes, current_price):
        """Filter to standard option strikes that actually exist on exchanges.

        Options trade at standard increments:
        - $1 increments for strikes near ATM (within ~5% of price)
        - $5 increments for mid-range strikes
        - $10+ increments for far OTM/ITM strikes
        We only keep strikes that are multiples of 5 (works for most underlyings)
        and a few $1-increment strikes near ATM for finer granularity.
        """
        atm_range = current_price * 0.05  # 5% near ATM
        result = []
        for s in strikes:
            near_atm = abs(s - current_price) <= atm_range
            if near_atm:
                # Near ATM: keep whole-dollar strikes
                if s == int(s):
                    result.append(s)
            else:
                # Away from ATM: only multiples of 5
                if s % 5 == 0:
                    result.append(s)
        return result

    def _get_options_chain_ibkr_locked(self, symbol: str) -> Optional[Dict]:
        """Get options chain from IBKR (must be called under _ib_lock).

        Uses batch qualifyContracts and reqMktData for speed.
        """
        try:
            # Get stock contract
            stock = Stock(symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(stock)

            if stock.conId == 0:
                logger.warning(f"{symbol}: Stock contract not found (IBKR may still be loading)")
                return None

            # Get option chains
            chains = self.ib.reqSecDefOptParams(stock.symbol, '', stock.secType, stock.conId)
            if not chains:
                logger.warning(f"{symbol}: No option chain data returned")
                return None

            # Get strikes and expirations
            chain = chains[0]
            strikes = sorted(chain.strikes)
            expirations = sorted(chain.expirations)

            # Get current price
            current_price = self._get_current_price_ibkr(symbol)
            if current_price is None:
                return None

            # Filter strikes: 20% ITM/OTM range, standard increments only
            strike_range = 0.20
            min_strike = current_price * (1 - strike_range)
            max_strike = current_price * (1 + strike_range)
            range_strikes = [s for s in strikes if min_strike <= s <= max_strike]
            filtered_strikes = self._standard_strikes(range_strikes, current_price)

            # Limit to ~15 strikes evenly spaced if still too many
            if len(filtered_strikes) > 15:
                step = max(1, len(filtered_strikes) // 15)
                filtered_strikes = filtered_strikes[::step]

            # Filter expirations: 7-90 DTE, limit to 4
            filtered_expirations = []
            for exp in expirations:
                exp_date = datetime.strptime(exp, '%Y%m%d') if isinstance(exp, str) else exp
                days = (exp_date - datetime.now()).days
                if 7 <= days <= 90:
                    filtered_expirations.append(exp)
            filtered_expirations = filtered_expirations[:4]

            if not filtered_strikes or not filtered_expirations:
                logger.warning(f"{symbol}: No valid strikes/expirations after filtering")
                return None

            logger.info(f"{symbol}: Fetching {len(filtered_strikes)} strikes x "
                        f"{len(filtered_expirations)} expirations")

            # Build all contracts at once
            contracts = []
            for exp in filtered_expirations:
                for strike in filtered_strikes:
                    contracts.append(Option(symbol, exp, strike, 'C', 'SMART', currency='USD'))
                    contracts.append(Option(symbol, exp, strike, 'P', 'SMART', currency='USD'))

            # Batch qualify - filters out non-existent contracts
            self.ib.qualifyContracts(*contracts)
            valid_contracts = [c for c in contracts if c.conId > 0]
            logger.info(f"{symbol}: {len(valid_contracts)}/{len(contracts)} contracts qualified")

            if not valid_contracts:
                return None

            # Batch request market data for all valid contracts
            tickers = {}
            for contract in valid_contracts:
                tickers[contract] = self.ib.reqMktData(contract, '', False, False)

            # Single wait for all data to populate (Greeks need time)
            self.ib.sleep(3.0)

            # Extract data from tickers
            calls_data = []
            puts_data = []

            for contract, ticker in tickers.items():
                price = ticker.last if ticker.last == ticker.last else (
                    ticker.close if ticker.close == ticker.close else (
                    ticker.marketPrice() if ticker.marketPrice() == ticker.marketPrice() else None))

                iv = None
                delta = gamma = vega = theta = None
                if ticker.modelGreeks:
                    iv = ticker.modelGreeks.impliedVol
                    delta = ticker.modelGreeks.delta
                    gamma = ticker.modelGreeks.gamma
                    vega = ticker.modelGreeks.vega
                    theta = ticker.modelGreeks.theta

                # Accept contract if we have either price or IV
                if price is None and iv is None:
                    continue

                exp_str = contract.lastTradeDateOrContractMonth
                row = {
                    'strike': contract.strike,
                    'expiry': exp_str,
                    'last': price if price else 0,
                    'bid': ticker.bid if ticker.bid == ticker.bid else 0,
                    'ask': ticker.ask if ticker.ask == ticker.ask else 0,
                    'volume': ticker.volume if ticker.volume == ticker.volume else 0,
                    'open_interest': 0,
                    'delta': delta,
                    'gamma': gamma,
                    'vega': vega,
                    'theta': theta,
                    'implied_vol': iv,
                }

                if contract.right == 'C':
                    calls_data.append(row)
                else:
                    puts_data.append(row)

            # Cancel market data subscriptions to free slots
            for contract in valid_contracts:
                try:
                    self.ib.cancelMktData(contract)
                except Exception:
                    pass

            logger.info(f"{symbol}: Got {len(calls_data)} calls, {len(puts_data)} puts")

            return {
                'symbol': symbol,
                'current_price': current_price,
                'calls': pd.DataFrame(calls_data),
                'puts': pd.DataFrame(puts_data),
                'timestamp': datetime.now(),
            }

        except Exception as e:
            logger.error(f"Error fetching IBKR data for {symbol}: {e}")
            return None
    
    def _get_options_chain_yfinance(self, symbol: str) -> Optional[Dict]:
        """Get options chain from YFinance"""
        if not self.connected:
            return None
        
        try:
            ticker = yf.Ticker(symbol)

            # Get current price — prefer fast_info (no lag), fall back to history
            current_price = None
            try:
                fi = ticker.fast_info
                live = fi.get('lastPrice') or fi.get('regularMarketPrice')
                if live and float(live) > 0:
                    current_price = float(live)
                    logger.info(f"{symbol}: price via fast_info = {current_price}")
            except Exception as e:
                logger.warning(f"{symbol}: fast_info failed: {e}")
            if current_price is None:
                try:
                    hist = ticker.history(period='5d')
                    logger.info(f"{symbol}: history rows = {len(hist)}, empty={hist.empty}")
                    if hist.empty:
                        logger.warning(f"{symbol}: history empty, cannot get price")
                        return None
                    current_price = float(hist['Close'].dropna().iloc[-1])
                    logger.info(f"{symbol}: price via history = {current_price}")
                except Exception as e:
                    logger.warning(f"{symbol}: history failed: {e}")
                    return None
            if not current_price or pd.isna(current_price):
                logger.warning(f"{symbol}: current_price invalid: {current_price}")
                return None

            # Get options data
            expirations = ticker.options
            
            calls_data = []
            puts_data = []
            
            # Column mapping from yfinance names to internal names
            _col_rename = {
                'impliedVolatility': 'implied_vol',
                'lastPrice': 'last_price',
                'openInterest': 'open_interest',
                'inTheMoney': 'in_the_money',
                'percentChange': 'percent_change',
                'contractSymbol': 'contract_symbol',
                'lastTradeDate': 'last_trade_date',
                'contractSize': 'contract_size',
            }

            for exp in expirations[:6]:  # Limit to 6 expirations
                try:
                    chain = ticker.option_chain(exp)
                    from datetime import date as _date
                    import math as _math
                    _dte = max(1, (_date.fromisoformat(exp) - _date.today()).days)
                    _T = _dte / 365.0

                    def _patch_iv(df, right):
                        """Fill near-zero IV from lastPrice time-value approx (pre-market fallback).

                        Restricted to near-ATM strikes (moneyness 0.75–1.25) where lastPrice
                        is most reliable. Deep ITM/OTM lastPrice is often stale and produces
                        absurd IV values that corrupt the smile chart.
                        """
                        df = df.copy()
                        bad = df['impliedVolatility'].fillna(0) < 0.05
                        if not bad.any():
                            return df
                        for idx in df[bad].index:
                            strike = float(df.loc[idx, 'strike'])
                            moneyness = strike / current_price if current_price > 0 else 1.0
                            # Skip deep ITM/OTM — lastPrice too stale to be useful
                            if not (0.75 <= moneyness <= 1.25):
                                continue
                            last = float(df.loc[idx, 'lastPrice'] or 0)
                            if last <= 0:
                                continue
                            intrinsic = max(0.0, current_price - strike) if right == 'C' else max(0.0, strike - current_price)
                            time_val = max(0.0, last - intrinsic)
                            if time_val > 0 and _T > 0:
                                iv_approx = time_val / (current_price * _math.sqrt(_T)) * _math.sqrt(2 * _math.pi)
                                # Sanity gate: 5%–200% annualised IV is the plausible range
                                if 0.05 <= iv_approx <= 2.0:
                                    df.loc[idx, 'impliedVolatility'] = iv_approx
                        return df

                    # Process calls
                    calls = _patch_iv(chain.calls, 'C').copy()
                    calls = calls.rename(columns=_col_rename)
                    calls['expiry'] = exp
                    calls['current_price'] = current_price
                    calls_data.extend(calls.to_dict('records'))

                    # Process puts
                    puts = _patch_iv(chain.puts, 'P').copy()
                    puts = puts.rename(columns=_col_rename)
                    puts['expiry'] = exp
                    puts['current_price'] = current_price
                    puts_data.extend(puts.to_dict('records'))

                except Exception as e:
                    continue
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'calls': pd.DataFrame(calls_data),
                'puts': pd.DataFrame(puts_data),
                'timestamp': datetime.now(),
            }
            
        except Exception as e:
            logger.error(f"Error fetching YFinance data for {symbol}: {e}")
            return None
    
    def _get_current_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol (thread-safe public method)"""
        if self.config.data_source.value == "ibkr":
            # Note: when called from _get_options_chain_ibkr_locked, we're already
            # under _ib_lock; _get_current_price_ibkr is the non-locking version
            # used for that case. This public method is for standalone price lookups.
            with self._ib_lock:
                self._ensure_event_loop()
                if not self._ensure_ibkr_connected():
                    return None
                return self._get_current_price_ibkr(symbol)
        else:
            try:
                ticker = yf.Ticker(symbol)
                fi = ticker.fast_info
                live = fi.get('lastPrice') or fi.get('regularMarketPrice')
                if live and float(live) > 0:
                    return float(live)
                return float(ticker.history(period='5d')['Close'].dropna().iloc[-1])
            except Exception:
                return None

    def _get_current_price_ibkr(self, symbol: str) -> Optional[float]:
        """Get current price from IBKR (must be called under _ib_lock)"""
        try:
            stock = Stock(symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(stock)
            ticker = self.ib.reqMktData(stock, '', False, False)
            self.ib.sleep(1.0)
            # Try last, then close, then marketPrice
            price = ticker.last
            if price != price:  # NaN check
                price = ticker.close
            if price != price:
                price = ticker.marketPrice()
            if price != price:
                return None
            return float(price)
        except Exception as e:
            logger.warning(f"Failed to get price for {symbol}: {e}")
            return None
    
    def get_historical_data(self, symbol: str, period: str = "1y") -> Optional[pd.DataFrame]:
        """Get historical price data"""
        if self.config.data_source.value == "yfinance":
            try:
                ticker = yf.Ticker(symbol)
                data = ticker.history(period=period)
                return data
            except Exception as e:
                logger.error(f"Error fetching historical data for {symbol}: {e}")
                return None
        else:
            # IBKR historical data would require more implementation
            logger.warning("Historical data not implemented for IBKR")
            return None
    
    def calculate_forward_price(self, symbol: str, days_to_expiry: int) -> Optional[float]:
        """Calculate forward price for options pricing"""
        try:
            # Get current price
            current_price = self._get_current_price(symbol)
            if current_price is None:
                return None
            
            # Get historical data for drift calculation
            hist_data = self.get_historical_data(symbol, period="2y")
            if hist_data is None or len(hist_data) < 50:
                return current_price  # Fallback to current price
            
            # Calculate annualized drift
            returns = hist_data['Close'].pct_change().dropna()
            annual_drift = returns.mean() * 252
            
            # Calculate forward price
            forward_price = current_price * np.exp(annual_drift * days_to_expiry / 365.25)
            
            return forward_price
            
        except Exception as e:
            logger.error(f"Error calculating forward price for {symbol}: {e}")
            return None
    
    def get_market_data(self, symbols: List[str]) -> Dict[str, Dict]:
        """Get market data for multiple symbols"""
        results = {}
        
        for symbol in symbols:
            try:
                data = self.get_options_chain(symbol)
                if data:
                    results[symbol] = data
            except Exception as e:
                logger.warning(f"Failed to get data for {symbol}: {e}")
                continue
        
        return results
    
    def get_historical_iv(self, symbol: str, days: int = 365) -> Optional[pd.DataFrame]:
        """Fetch daily ATM implied volatility history for *symbol*.

        Uses IBKR reqHistoricalData with whatToShow='OPTION_IMPLIED_VOLATILITY',
        which returns the market-consensus ATM IV directly.  Falls back to None
        if IBKR is unavailable — caller should then use stored data or VIX proxy.

        Returns a DataFrame with columns ['date', 'atm_iv'] or None.
        """
        if self.config.data_source.value != 'ibkr' or IB is None:
            return None

        with self._ib_lock:
            self._ensure_event_loop()
            if not self._ensure_ibkr_connected():
                return None
            return self._get_historical_iv_locked(symbol, days)

    def _get_historical_iv_locked(self, symbol: str, days: int) -> Optional[pd.DataFrame]:
        """Must be called under _ib_lock."""
        try:
            from ib_insync import Stock
            contract = Stock(symbol, 'SMART', 'USD')
            self.ib.qualifyContracts(contract)
            if contract.conId == 0:
                logger.warning(f"{symbol}: contract not found for IV history")
                return None

            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime='',
                durationStr=f'{min(days, 365)} D',
                barSizeSetting='1 day',
                whatToShow='OPTION_IMPLIED_VOLATILITY',
                useRTH=True,
                formatDate=1,
            )
            if not bars:
                logger.warning(f"{symbol}: no IV history returned from IBKR")
                return None

            records = []
            for bar in bars:
                date_str = str(bar.date)[:10]   # 'YYYY-MM-DD'
                iv_val   = bar.close             # ATM IV, annualised (e.g. 0.22 = 22%)
                if iv_val and iv_val == iv_val:  # not NaN
                    records.append({'date': date_str, 'atm_iv': float(iv_val)})

            if not records:
                return None

            df = pd.DataFrame(records)
            logger.info(f"{symbol}: fetched {len(df)} days of ATM IV history from IBKR")
            return df

        except Exception as e:
            logger.error(f"Error fetching IBKR IV history for {symbol}: {e}")
            return None

    def disconnect(self):
        """Disconnect from data source"""
        if self.ib and self.connected:
            self.ib.disconnect()
            self.connected = False
            logger.info("Disconnected from IBKR")