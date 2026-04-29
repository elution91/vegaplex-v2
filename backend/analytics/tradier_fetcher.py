"""
Tradier API fetcher — live options data source.

Provides a yfinance-compatible interface so existing scanner code
works unchanged whether using Tradier or yfinance.

Usage:
    fetcher = TradierFetcher(api_key="your_key")
    ticker = fetcher.ticker("AAPL")
    # ticker.options, ticker.option_chain(exp), ticker.history() all work
    # identically to a yfinance Ticker object.
"""
import logging
from types import SimpleNamespace
from typing import List, Optional
from datetime import datetime, date

import pandas as pd
import requests

logger = logging.getLogger(__name__)

TRADIER_BASE = "https://api.tradier.com/v1"


class TradierFetcher:
    """Wraps the Tradier REST API with a yfinance-compatible interface."""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {api_key}",
            "Accept": "application/json",
        })

    # ------------------------------------------------------------------
    # Connection test
    # ------------------------------------------------------------------

    def test_connection(self) -> dict:
        """Return {'ok': True, 'name': 'John Doe'} or {'ok': False, 'error': '...'}."""
        try:
            r = self.session.get(f"{TRADIER_BASE}/user/profile", timeout=8)
            if r.status_code == 200:
                profile = r.json().get("profile", {})
                name = profile.get("name", "Connected")
                return {"ok": True, "name": name}
            return {"ok": False, "error": f"HTTP {r.status_code}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    # ------------------------------------------------------------------
    # Market data helpers
    # ------------------------------------------------------------------

    def get_quote(self, symbol: str) -> float:
        """Current last/close price for a stock."""
        try:
            r = self.session.get(
                f"{TRADIER_BASE}/markets/quotes",
                params={"symbols": symbol, "greeks": "false"},
                timeout=8,
            )
            q = r.json().get("quotes", {}).get("quote", {})
            return float(q.get("last") or q.get("prevclose") or 0)
        except Exception as e:
            logger.debug(f"Tradier quote error {symbol}: {e}")
            return 0.0

    def get_expirations(self, symbol: str) -> List[str]:
        """Sorted list of expiration date strings 'YYYY-MM-DD'."""
        try:
            r = self.session.get(
                f"{TRADIER_BASE}/markets/options/expirations",
                params={"symbol": symbol, "includeAllRoots": "true", "strikes": "false"},
                timeout=8,
            )
            exps = r.json().get("expirations", {}).get("date", [])
            if isinstance(exps, str):
                exps = [exps]
            return sorted(exps or [])
        except Exception as e:
            logger.debug(f"Tradier expirations error {symbol}: {e}")
            return []

    def get_chain(self, symbol: str, expiration: str) -> tuple:
        """
        Returns (calls_df, puts_df) with columns matching yfinance:
        strike, bid, ask, lastPrice, volume, openInterest,
        impliedVolatility, delta (optional)
        """
        try:
            r = self.session.get(
                f"{TRADIER_BASE}/markets/options/chains",
                params={"symbol": symbol, "expiration": expiration, "greeks": "true"},
                timeout=10,
            )
            options = r.json().get("options", {}).get("option", []) or []
            if not options:
                return pd.DataFrame(), pd.DataFrame()

            df = pd.DataFrame(options)

            # Extract IV and delta from greeks sub-dict
            def _greek(row, key):
                g = row.get("greeks")
                if isinstance(g, dict):
                    return g.get(key)
                return None

            df["impliedVolatility"] = df.apply(lambda r: _greek(r, "smv_vol") or _greek(r, "mid_iv") or 0, axis=1)
            df["delta"] = df.apply(lambda r: _greek(r, "delta"), axis=1)

            # Normalise column names
            df = df.rename(columns={
                "last": "lastPrice",
                "open_interest": "openInterest",
            })

            # Ensure required columns exist
            for col in ["bid", "ask", "lastPrice", "volume", "openInterest", "impliedVolatility"]:
                if col not in df.columns:
                    df[col] = 0
            df = df.fillna({"bid": 0, "ask": 0, "lastPrice": 0, "volume": 0,
                            "openInterest": 0, "impliedVolatility": 0})

            calls = df[df["option_type"] == "call"].copy().reset_index(drop=True)
            puts  = df[df["option_type"] == "put"].copy().reset_index(drop=True)
            return calls, puts

        except Exception as e:
            logger.debug(f"Tradier chain error {symbol}/{expiration}: {e}")
            return pd.DataFrame(), pd.DataFrame()

    # ------------------------------------------------------------------
    # yfinance-compatible Ticker shim
    # ------------------------------------------------------------------

    def ticker(self, symbol: str) -> "TradierTicker":
        return TradierTicker(symbol, self)


class TradierTicker:
    """
    Drop-in replacement for yfinance.Ticker.
    Exposes .options, .option_chain(exp), .history() with the same
    interface so scanner/analyzer code works unchanged.
    """

    def __init__(self, symbol: str, fetcher: TradierFetcher):
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
        """Return a single-row DataFrame with Close column, matching yfinance."""
        if self._price is None:
            self._price = self._fetcher.get_quote(self.symbol)
        return pd.DataFrame(
            {"Close": [self._price]},
            index=[pd.Timestamp.now()],
        )
