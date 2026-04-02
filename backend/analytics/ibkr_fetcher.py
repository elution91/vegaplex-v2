"""
IBKR TWS/Gateway fetcher — live options data via Interactive Brokers.

Requires:
  - TWS or IB Gateway running locally with API enabled
  - ib_insync installed: pip install ib_insync

TWS API settings (in TWS: File → Global Configuration → API → Settings):
  - Enable ActiveX and Socket Clients: checked
  - Socket port: 7497 (paper) or 7496 (live)
  - Allow connections from localhost only: checked
"""
import asyncio
import logging
import threading
from concurrent.futures import Future as _Future
from types import SimpleNamespace
from typing import List, Optional
from datetime import datetime, date

import pandas as pd

logger = logging.getLogger(__name__)


def _in_fresh_thread(fn, timeout: int = 20):
    """Run fn() in a brand-new daemon thread with its own asyncio event loop.

    Dash request threads (process_request_thread) carry unknown asyncio state.
    A fresh thread guarantees a clean event loop for ib_insync every time.
    """
    fut: _Future = _Future()

    def _worker():
        asyncio.set_event_loop(asyncio.new_event_loop())
        try:
            fut.set_result(fn())
        except Exception as exc:
            fut.set_exception(exc)

    threading.Thread(target=_worker, daemon=True).start()
    return fut.result(timeout=timeout)


def _ib_connect(host: str, port: int, client_id: int):
    """Return a connected IB instance. Must be called inside _in_fresh_thread."""
    from ib_insync import IB
    ib = IB()
    ib.connect(host, port, clientId=client_id, timeout=8, readonly=True)
    return ib


class IBKRFetcher:
    """Wraps ib_insync with a yfinance-compatible interface."""

    def __init__(self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 10):
        self.host = host
        self.port = port
        self.client_id = client_id

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

    def test_connection(self) -> dict:
        try:
            def _run():
                ib = _ib_connect(self.host, self.port, self.client_id)
                accounts = ib.managedAccounts()
                ib.disconnect()
                return accounts
            accounts = _in_fresh_thread(_run)
            label = accounts[0] if accounts else "Connected"
            return {"ok": True, "name": f"IBKR {label}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Market data helpers
    # ------------------------------------------------------------------

    def get_quote(self, symbol: str) -> float:
        try:
            def _run():
                from ib_insync import Stock
                ib = _ib_connect(self.host, self.port, self.client_id)
                contract = Stock(symbol, "SMART", "USD")
                ib.qualifyContracts(contract)
                ticker = ib.reqMktData(contract, "", False, False)
                ib.sleep(1.5)
                price = ticker.last or ticker.close or 0.0
                ib.cancelMktData(contract)
                ib.disconnect()
                return float(price or 0)
            return _in_fresh_thread(_run)
        except Exception as e:
            logger.debug(f"IBKR quote error {symbol}: {e}")
            return 0.0

    def get_expirations(self, symbol: str) -> List[str]:
        try:
            def _run():
                from ib_insync import Stock
                ib = _ib_connect(self.host, self.port, self.client_id)
                stock = Stock(symbol, "SMART", "USD")
                con_id = ib.qualifyContracts(stock)[0].conId
                chains = ib.reqSecDefOptParams(symbol, "", "STK", con_id)
                ib.disconnect()
                exps = set()
                for chain in chains:
                    exps.update(chain.expirations)
                return sorted(
                    f"{e[:4]}-{e[4:6]}-{e[6:]}"
                    for e in exps if len(e) == 8
                )
            return _in_fresh_thread(_run)
        except Exception as e:
            logger.debug(f"IBKR expirations error {symbol}: {e}")
            return []

    def get_chain(self, symbol: str, expiration: str) -> tuple:
        """Returns (calls_df, puts_df) matching yfinance column schema."""
        try:
            def _run():
                from ib_insync import Stock, Option
                ib = _ib_connect(self.host, self.port, self.client_id)

                stock = Stock(symbol, "SMART", "USD")
                qualified = ib.qualifyContracts(stock)
                if not qualified:
                    ib.disconnect()
                    return pd.DataFrame(), pd.DataFrame()

                exp_ibkr = expiration.replace("-", "")
                chains = ib.reqSecDefOptParams(symbol, "", "STK", qualified[0].conId)
                strikes = set()
                for ch in chains:
                    if exp_ibkr in ch.expirations:
                        strikes.update(ch.strikes)
                strikes = sorted(strikes)

                if not strikes:
                    ib.disconnect()
                    return pd.DataFrame(), pd.DataFrame()

                ticker_obj = ib.reqMktData(qualified[0], "", False, False)
                ib.sleep(1.0)
                spot = ticker_obj.last or ticker_obj.close or 0
                ib.cancelMktData(qualified[0])
                if spot > 0:
                    strikes = [k for k in strikes if 0.80 * spot <= k <= 1.20 * spot]

                rows = {"call": [], "put": []}
                for right, label in [("C", "call"), ("P", "put")]:
                    contracts = [Option(symbol, exp_ibkr, k, right, "SMART") for k in strikes]
                    qualified_opts = ib.qualifyContracts(*contracts)
                    tickers = ib.reqTickers(*qualified_opts)
                    ib.sleep(2.0)
                    for tk in tickers:
                        c = tk.contract
                        rows[label].append({
                            "strike": c.strike,
                            "bid": tk.bid if tk.bid and tk.bid > 0 else 0.0,
                            "ask": tk.ask if tk.ask and tk.ask > 0 else 0.0,
                            "lastPrice": tk.last or 0.0,
                            "volume": tk.volume or 0,
                            "openInterest": tk.callOpenInterest or tk.putOpenInterest or 0,
                            "impliedVolatility": (tk.modelGreeks.impliedVol
                                                  if tk.modelGreeks else 0.0) or 0.0,
                            "delta": (tk.modelGreeks.delta if tk.modelGreeks else None),
                        })

                ib.disconnect()
                return pd.DataFrame(rows["call"]), pd.DataFrame(rows["put"])

            return _in_fresh_thread(_run, timeout=30)
        except Exception as e:
            logger.debug(f"IBKR chain error {symbol}/{expiration}: {e}")
            return pd.DataFrame(), pd.DataFrame()

    # ------------------------------------------------------------------
    # VIX futures strip
    # ------------------------------------------------------------------

    def get_vx_strip(self, n_contracts: int = 8) -> list:
        """Fetch live VX futures strip from IBKR."""
        try:
            def _run():
                from ib_insync import Future
                ib = _ib_connect(self.host, self.port, self.client_id)

                today = date.today()
                strip = []
                month, year = today.month, today.year
                if today.day > 18:
                    month += 1
                    if month > 12:
                        month, year = 1, year + 1

                for _ in range(n_contracts):
                    expiry_str = f"{year}{month:02d}"
                    contract = Future('VX', expiry_str, 'CFE', currency='USD')
                    try:
                        ib.qualifyContracts(contract)
                        ticker = ib.reqMktData(contract, '', False, False)
                        ib.sleep(1.0)
                        price = ticker.last or ticker.close or ticker.bid or 0.0
                        ib.cancelMktData(contract)
                        if price and price > 0:
                            exp_date = datetime(year, month, 15).date()
                            strip.append({
                                'label': datetime(year, month, 1).strftime('%b %Y'),
                                'level': round(float(price), 3),
                                'dte': (exp_date - today).days,
                            })
                    except Exception as inner:
                        logger.debug(f"VX {expiry_str} fetch error: {inner}")

                    month += 1
                    if month > 12:
                        month, year = 1, year + 1

                ib.disconnect()
                logger.info(f"IBKR VX strip: {len(strip)} contracts fetched")
                return strip

            return _in_fresh_thread(_run, timeout=60)
        except Exception as e:
            logger.debug(f"IBKR VX strip unavailable: {e}")
            return []

    # ------------------------------------------------------------------
    # Single-connection batch fetch for Bennett move calculation
    # ------------------------------------------------------------------

    def get_options_for_bennett(self, symbol: str, earnings_date_str: str,
                                current_price: float) -> Optional[dict]:
        """Fetch expirations + two option chains in ONE IBKR connection.

        Returns dict with keys: t1_exp, t2_exp, t1_data, t2_data
        where each *_data has: iv, straddle_mid, straddle_bid, straddle_ask,
        spread_pct, has_quotes.  Returns None on failure.
        """
        import math as _math
        from datetime import datetime as _dt, date as _date

        def _run():
            from ib_insync import Stock, Option
            ib = _ib_connect(self.host, self.port, self.client_id)
            try:
                today = _date.today()
                earnings_dt = _dt.strptime(earnings_date_str, '%Y-%m-%d').date()

                # ── Expirations ────────────────────────────────────────────
                stock = Stock(symbol, "SMART", "USD")
                con_id = ib.qualifyContracts(stock)[0].conId
                chains = ib.reqSecDefOptParams(symbol, "", "STK", con_id)
                exps_raw = set()
                for ch in chains:
                    exps_raw.update(ch.expirations)
                expirations = sorted(
                    f"{e[:4]}-{e[4:6]}-{e[6:]}"
                    for e in exps_raw if len(e) == 8
                )
                if len(expirations) < 2:
                    return None

                # ── Find T1/T2 ─────────────────────────────────────────────
                t1_exp = t2_exp = None
                for exp_str in expirations:
                    exp_dt = _dt.strptime(exp_str, '%Y-%m-%d').date()
                    if exp_dt >= earnings_dt:
                        if t1_exp is None:
                            t1_exp = exp_str
                        elif t2_exp is None:
                            t2_exp = exp_str
                            break
                if not t1_exp or not t2_exp:
                    return None

                # ── Helper: ATM data for one expiry (reuses open connection) ─
                def _atm(exp_str):
                    exp_ibkr = exp_str.replace("-", "")
                    strikes_all = set()
                    for ch in chains:
                        if exp_ibkr in ch.expirations:
                            strikes_all.update(ch.strikes)
                    strikes = sorted(
                        k for k in strikes_all
                        if 0.85 * current_price <= k <= 1.15 * current_price
                    ) or sorted(strikes_all)
                    if not strikes:
                        return None

                    call_contracts = [Option(symbol, exp_ibkr, k, "C", "SMART") for k in strikes]
                    put_contracts  = [Option(symbol, exp_ibkr, k, "P", "SMART") for k in strikes]
                    qc = ib.qualifyContracts(*call_contracts)
                    qp = ib.qualifyContracts(*put_contracts)
                    c_tks = ib.reqTickers(*qc)
                    p_tks = ib.reqTickers(*qp)
                    ib.sleep(1.5)

                    # Find ATM strike
                    def _pick_atm(tks):
                        best, best_dist = None, float('inf')
                        for tk in tks:
                            dist = abs(tk.contract.strike - current_price)
                            if dist < best_dist:
                                best_dist, best = dist, tk
                        return best

                    c_tk = _pick_atm(c_tks)
                    p_tk = _pick_atm(p_tks)
                    if not c_tk or not p_tk:
                        return None

                    exp_dte = (_dt.strptime(exp_str, '%Y-%m-%d').date() - today).days
                    c_iv = (c_tk.modelGreeks.impliedVol if c_tk.modelGreeks else 0) or 0
                    p_iv = (p_tk.modelGreeks.impliedVol if p_tk.modelGreeks else 0) or 0
                    atm_iv = (c_iv + p_iv) / 2

                    # Fallback IV from last price
                    if atm_iv < 0.05 and exp_dte > 0:
                        c_last = c_tk.last or 0
                        p_last = p_tk.last or 0
                        straddle_time = max(0, c_last - max(0, current_price - c_tk.contract.strike)) + \
                                        max(0, p_last - max(0, p_tk.contract.strike - current_price))
                        T = exp_dte / 365.0
                        if straddle_time > 0 and T > 0:
                            atm_iv = straddle_time / (current_price * _math.sqrt(T)) * _math.sqrt(2 * _math.pi)
                    if atm_iv < 0.05:
                        return None

                    c_bid = c_tk.bid or 0; c_ask = c_tk.ask or 0
                    p_bid = p_tk.bid or 0; p_ask = p_tk.ask or 0
                    has_quotes = c_bid > 0 and p_bid > 0
                    if has_quotes:
                        c_mid = (c_bid + c_ask) / 2
                        p_mid = (p_bid + p_ask) / 2
                    else:
                        c_mid = c_tk.last or 0
                        p_mid = p_tk.last or 0
                    straddle_mid = c_mid + p_mid
                    spread_pct = ((c_ask - c_bid) + (p_ask - p_bid)) / straddle_mid \
                                 if has_quotes and straddle_mid > 0 else None
                    return {
                        'iv': atm_iv,
                        'straddle_mid': straddle_mid,
                        'straddle_bid': c_bid + p_bid,
                        'straddle_ask': c_ask + p_ask,
                        'spread_pct': spread_pct,
                        'has_quotes': has_quotes,
                    }

                d1 = _atm(t1_exp)
                d2 = _atm(t2_exp)
                return {'t1_exp': t1_exp, 't2_exp': t2_exp, 't1_data': d1, 't2_data': d2}
            finally:
                ib.disconnect()

        try:
            return _in_fresh_thread(_run, timeout=30)
        except Exception as e:
            logger.debug(f"IBKR bennett fetch error {symbol}: {e}")
            return None

    # ------------------------------------------------------------------
    # yfinance-compatible Ticker shim
    # ------------------------------------------------------------------

    def ticker(self, symbol: str) -> "IBKRTicker":
        return IBKRTicker(symbol, self)


class IBKRTicker:
    """Drop-in replacement for yfinance.Ticker using IBKR data."""

    def __init__(self, symbol: str, fetcher: IBKRFetcher):
        self.symbol = symbol
        self._fetcher = fetcher
        self._exps: Optional[tuple] = None
        self._chains: dict = {}
        self._price: Optional[float] = None

    @property
    def options(self) -> tuple:
        if self._exps is None:
            self._exps = tuple(self._fetcher.get_expirations(self.symbol))
        return self._exps

    def option_chain(self, expiry: str) -> SimpleNamespace:
        if expiry not in self._chains:
            calls, puts = self._fetcher.get_chain(self.symbol, expiry)
            self._chains[expiry] = SimpleNamespace(calls=calls, puts=puts)
        return self._chains[expiry]

    def history(self, period: str = "1d", **kwargs) -> pd.DataFrame:
        if self._price is None:
            self._price = self._fetcher.get_quote(self.symbol)
        return pd.DataFrame(
            {"Close": [self._price]},
            index=[pd.Timestamp.now()],
        )
