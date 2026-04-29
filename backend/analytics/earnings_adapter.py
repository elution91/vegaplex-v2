"""
Bridge module for integrating EarningsEdgeDetection into the volatility skew scanner.
Imports EarningsScanner from the cloned repo and optionally swaps yfinance
for IBKR via DataFetcher.
"""

import sys
import os
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf
from scipy.interpolate import interp1d
from scipy.stats import norm as _norm_dist
from tradier_fetcher import TradierFetcher

logger = logging.getLogger(__name__)


def _recommend_structure(vol_edge_pp: float, richness_ratio: float, win_rate: float) -> str:
    """Recommend an earnings short-vol structure based on idiosyncratic earnings signals.

    The scanner only surfaces names that have already passed the IV/RV gate (≥ 1.25),
    confirming a short-vol edge exists. This function selects the *structure* — it does
    not second-guess the directional bias.

    Primary axis — term structure inversion (vol_edge_pp = IV_front − forward_IV):
      This is idiosyncratic: it measures how much near-term IV is elevated relative to
      the post-event forward vol for *this specific stock and event*, independent of
      the general market vol level.

    Secondary axes:
      win_rate   — historical IV overstatement rate for this ticker's earnings events
      richness_ratio (Bennett) — used as confirmatory signal only; NOT as a veto.
        The straddle/Bennett ratio can be depressed by macro vol inflating the Bennett
        denominator, so it is not used to recommend Long Vol here. Users can assess
        the macro overlay themselves from the RICH column.

    Structure map:
      Strong inversion (≥ 40pp)  → Calendar  (harvest term premium directly)
      Strong inversion + high win → Either   (calendar and iron fly both valid)
      Moderate inversion (≥ 20pp) → Sell     (short straddle, collect flat premium)
      No inversion + high win     → Iron Fly (pin risk, collect theta across wings)
      Mild inversion              → Sell      (default short vol given IV/RV gate passed)
    """
    HIGH_EDGE = 40
    MOD_EDGE  = 20
    HIGH_WIN  = 70

    high_edge = vol_edge_pp >= HIGH_EDGE
    mod_edge  = vol_edge_pp >= MOD_EDGE
    high_win  = win_rate >= HIGH_WIN

    if high_edge and high_win:
        return 'Either'
    if high_edge:
        return 'Calendar'
    if high_win and not mod_edge:
        return 'Iron Fly'
    if mod_edge:
        return 'Sell'
    return 'Sell'

# ---------------------------------------------------------------------------
# Wire in the EarningsEdgeDetection repo
# ---------------------------------------------------------------------------
_EARNINGS_DIR = str(
    Path(__file__).resolve().parent.parent / 'EarningsEdgeDetection' / 'cli_scanner'
)
if _EARNINGS_DIR not in sys.path:
    sys.path.insert(0, _EARNINGS_DIR)

# Apply cookie patch before any yfinance usage inside their code
import core.yfinance_cookie_patch as _cookie_patch
_cookie_patch.patch_yfdata_cookie_basic()

from core.scanner import EarningsScanner
from core.analyzer import OptionsAnalyzer


# ---------------------------------------------------------------------------
# IBKR-backed OptionsAnalyzer (drop-in replacement)
# ---------------------------------------------------------------------------
class IBKROptionsAnalyzer(OptionsAnalyzer):
    """OptionsAnalyzer that uses DataFetcher (IBKR) for options chain data
    while keeping yfinance for historical OHLC (Yang-Zhang vol)."""

    def __init__(self, data_fetcher):
        super().__init__()
        self.data_fetcher = data_fetcher

    def compute_recommendation(self, ticker: str) -> Dict:
        try:
            ticker = ticker.strip().upper()
            if not ticker:
                return {"error": "No symbol provided."}

            # Fetch options chain via DataFetcher (IBKR or yfinance)
            options_data = self.data_fetcher.get_options_chain(ticker)
            if not options_data:
                return {"error": f"No options data for {ticker}."}

            current_price = options_data['current_price']
            calls = options_data['calls']
            puts = options_data['puts']

            if calls.empty or puts.empty:
                return {"error": f"Empty options chain for {ticker}."}

            # Group by expiration to build term structure
            expiry_col = 'expiry'
            if expiry_col not in calls.columns:
                return {"error": "No expiry column in options data."}

            expirations = sorted(calls[expiry_col].unique())
            if not expirations:
                return {"error": "No expirations found."}

            today = datetime.today().date()
            atm_ivs = {}
            straddle = None
            atm_call_delta = None
            atm_put_delta = None
            first_chain = True

            for exp in expirations:
                exp_calls = calls[calls[expiry_col] == exp]
                exp_puts = puts[puts[expiry_col] == exp]

                if exp_calls.empty or exp_puts.empty:
                    continue

                # Find ATM options
                call_idx = (exp_calls['strike'] - current_price).abs().idxmin()
                put_idx = (exp_puts['strike'] - current_price).abs().idxmin()

                call_iv = exp_calls.loc[call_idx, 'implied_vol']
                put_iv = exp_puts.loc[put_idx, 'implied_vol']

                if pd.isna(call_iv) or pd.isna(put_iv) or call_iv <= 0 or put_iv <= 0:
                    continue

                atm_iv = (call_iv + put_iv) / 2.0

                # Parse expiration date
                try:
                    if len(str(exp)) == 8:  # IBKR format: '20260213'
                        exp_date = datetime.strptime(str(exp), '%Y%m%d').date()
                    else:
                        exp_date = datetime.strptime(str(exp), '%Y-%m-%d').date()
                except ValueError:
                    continue

                exp_str = exp_date.strftime('%Y-%m-%d')
                atm_ivs[exp_str] = atm_iv

                if first_chain:
                    call_bid = exp_calls.loc[call_idx, 'bid']
                    call_ask = exp_calls.loc[call_idx, 'ask']
                    put_bid = exp_puts.loc[put_idx, 'bid']
                    put_ask = exp_puts.loc[put_idx, 'ask']

                    call_mid = (call_bid + call_ask) / 2 if call_bid > 0 and call_ask > 0 else 0
                    put_mid = (put_bid + put_ask) / 2 if put_bid > 0 and put_ask > 0 else 0
                    straddle = call_mid + put_mid if call_mid > 0 and put_mid > 0 else None

                    if 'delta' in exp_calls.columns:
                        atm_call_delta = exp_calls.loc[call_idx, 'delta']
                        atm_put_delta = exp_puts.loc[put_idx, 'delta']

                    first_chain = False

            if not atm_ivs:
                return {"error": "Could not calculate ATM IVs from IBKR data."}

            # Build term structure (same logic as original)
            dtes = [(datetime.strptime(exp, '%Y-%m-%d').date() - today).days
                    for exp in atm_ivs.keys()]
            ivs = list(atm_ivs.values())

            term_spline = self.build_term_structure(dtes, ivs)
            iv30 = term_spline(30)
            slope = (term_spline(45) - term_spline(min(dtes))) / (45 - min(dtes))

            # Historical vol via yfinance (DataFetcher doesn't provide OHLC)
            stock = yf.Ticker(ticker)
            hist_data = stock.history(period='3mo')
            hist_vol = self.yang_zhang_volatility(hist_data)

            avg_volume = hist_data['Volume'].rolling(30).mean().dropna().iloc[-1]

            result = {
                'avg_volume': avg_volume >= 1_500_000,
                'iv30_rv30': iv30 / hist_vol if hist_vol > 0 else 9999,
                'term_slope': slope,
                'term_structure_valid': slope <= -0.004,
                'term_structure_tier2': -0.006 < slope <= -0.004,
                'expected_move': f"{(straddle / current_price * 100):.2f}%" if straddle else "N/A",
                'current_price': current_price,
                'ticker': ticker,
                'recommendation': (
                    'BUY' if iv30 < hist_vol and avg_volume >= 1_500_000
                    else 'SELL' if iv30 > hist_vol * 1.2
                    else 'HOLD'
                ),
            }

            if atm_call_delta is not None and atm_put_delta is not None:
                result['atm_call_delta'] = float(atm_call_delta)
                result['atm_put_delta'] = float(atm_put_delta)

            return result

        except Exception as e:
            logger.error(f"IBKR OptionsAnalyzer error for {ticker}: {e}")
            return {"error": str(e), "ticker": ticker, "status": "ERROR"}


# ---------------------------------------------------------------------------
# Main adapter class
# ---------------------------------------------------------------------------
class EarningsAdapter:
    """Wraps EarningsScanner for use in the dashboard.
    Optionally injects IBKR-backed options analysis."""

    def __init__(self, config=None, data_fetcher=None):
        """
        Args:
            config: ScannerConfig from vol_skew_scanner (optional)
            data_fetcher: DataFetcher instance (optional, for IBKR mode)
        """
        self.config = config
        self.data_fetcher = data_fetcher
        self.scanner = EarningsScanner()

        # Swap in IBKR analyzer if configured
        use_ibkr = False
        if config:
            earnings_cfg = getattr(config, 'earnings_scanner', {})
            if isinstance(earnings_cfg, dict):
                use_ibkr = earnings_cfg.get('use_ibkr_data', False)
            if use_ibkr and data_fetcher:
                logger.info("Earnings scanner: using IBKR for options data")
                self.scanner.analyzer = IBKROptionsAnalyzer(data_fetcher)
            else:
                logger.info("Earnings scanner: using yfinance for options data")

    def _compute_bennett_move(self, ticker: str, earnings_date_str: str,
                              current_price: float, tradier=None) -> Optional[Dict]:
        """Extract implied jump volatility and compute Bennett expected move.

        Uses Colin Bennett's variance extraction (Trading Volatility):
          forward_var = (iv2²·T2 − iv1²·T1) / (T2 − T1)
          jump_var    = iv1²·T1 − forward_var·(T1 − 1/365)
          E[|move|]   = jump_vol·(2·φ(0) + E_ret·(2·Φ(σ)−1))

        T1 = first expiry on/after earnings date (contains the jump)
        T2 = next expiry after T1 (post-earnings, clean forward vol)
        """
        try:
            today = datetime.today().date()

            # ── IBKR fast path: single connection fetches all data ──────────
            from ibkr_fetcher import IBKRFetcher as _IBKRFetcher
            if isinstance(tradier, _IBKRFetcher):
                batch = tradier.get_options_for_bennett(
                    ticker, earnings_date_str, current_price
                )
                if not batch:
                    return None
                t1_exp, t2_exp = batch['t1_exp'], batch['t2_exp']
                d1, d2 = batch['t1_data'], batch['t2_data']
                if not d1 or not d2:
                    return None
                t1_days = (datetime.strptime(t1_exp, '%Y-%m-%d').date() - today).days
                t2_days = (datetime.strptime(t2_exp, '%Y-%m-%d').date() - today).days
                # Jump to variance extraction — skip yfinance block below
                return self._bennett_from_chain_data(
                    d1, d2, t1_days, t2_days, current_price
                )

            # ── yfinance / Tradier path ─────────────────────────────────────
            t = tradier.ticker(ticker) if tradier else yf.Ticker(ticker)
            expirations = t.options
            if not expirations or len(expirations) < 2:
                return None

            earnings_dt = datetime.strptime(earnings_date_str, '%Y-%m-%d').date()

            # Find T1 = first expiry on/after earnings date, T2 = next after T1
            t1_exp = t2_exp = None
            for exp_str in sorted(expirations):
                exp_dt = datetime.strptime(exp_str, '%Y-%m-%d').date()
                if exp_dt >= earnings_dt:
                    if t1_exp is None:
                        t1_exp = exp_str
                    elif t2_exp is None:
                        t2_exp = exp_str
                        break

            if not t1_exp or not t2_exp:
                return None

            t1_days = (datetime.strptime(t1_exp, '%Y-%m-%d').date() - today).days
            t2_days = (datetime.strptime(t2_exp, '%Y-%m-%d').date() - today).days
            if t1_days <= 0 or t2_days <= t1_days:
                return None

            def _atm_data(exp_str: str) -> Optional[Dict]:
                """Return ATM IV + bid/ask spread data for one expiry."""
                import math as _math
                chain = t.option_chain(exp_str)
                calls, puts = chain.calls, chain.puts
                if calls.empty or puts.empty:
                    return None
                ci = (calls['strike'] - current_price).abs().idxmin()
                pi = (puts['strike'] - current_price).abs().idxmin()
                c_iv = float(calls.loc[ci, 'impliedVolatility'] or 0)
                p_iv = float(puts.loc[pi, 'impliedVolatility'] or 0)
                atm_iv = (c_iv + p_iv) / 2.0

                # Fallback: compute IV from lastPrice straddle when yfinance returns near-zero
                if atm_iv < 0.05:
                    exp_dte = (datetime.strptime(exp_str, '%Y-%m-%d').date() - today).days
                    c_last = float(calls.loc[ci, 'lastPrice'] or 0)
                    p_last = float(puts.loc[pi, 'lastPrice'] or 0)
                    c_strike = float(calls.loc[ci, 'strike'])
                    p_strike = float(puts.loc[pi, 'strike'])
                    c_time = max(0.0, c_last - max(0.0, current_price - c_strike))
                    p_time = max(0.0, p_last - max(0.0, p_strike - current_price))
                    straddle_time = c_time + p_time
                    T = exp_dte / 365.0
                    if straddle_time > 0 and T > 0:
                        atm_iv = straddle_time / (current_price * _math.sqrt(T)) * _math.sqrt(2 * _math.pi)

                if atm_iv < 0.05:
                    return None

                c_bid = float(calls.loc[ci, 'bid'] or 0)
                c_ask = float(calls.loc[ci, 'ask'] or 0)
                p_bid = float(puts.loc[pi, 'bid'] or 0)
                p_ask = float(puts.loc[pi, 'ask'] or 0)
                has_quotes = c_bid > 0 and p_bid > 0

                if has_quotes:
                    c_mid = (c_bid + c_ask) / 2
                    p_mid = (p_bid + p_ask) / 2
                else:
                    c_mid = float(calls.loc[ci, 'lastPrice'] or 0)
                    p_mid = float(puts.loc[pi, 'lastPrice'] or 0)

                straddle_mid = c_mid + p_mid
                straddle_bid = c_bid + p_bid
                straddle_ask = c_ask + p_ask
                spread_pct = ((c_ask - c_bid) + (p_ask - p_bid)) / straddle_mid if (has_quotes and straddle_mid > 0) else None
                return {
                    'iv': atm_iv,
                    'straddle_mid': straddle_mid,
                    'straddle_bid': straddle_bid,
                    'straddle_ask': straddle_ask,
                    'spread_pct': spread_pct,
                    'has_quotes': has_quotes,
                }

            d1 = _atm_data(t1_exp)
            d2 = _atm_data(t2_exp)
            if not d1 or not d2:
                return None
            iv1, iv2 = d1['iv'], d2['iv']

            # Bennett variance extraction
            T1, T2 = t1_days / 365.0, t2_days / 365.0
            fwd_var = (iv2**2 * T2 - iv1**2 * T1) / (T2 - T1)
            normal_t = T1 - (1.0 / 365.0)   # strip one jump day
            jump_var = iv1**2 * T1 - fwd_var * normal_t

            if jump_var <= 0 or fwd_var <= 0:
                return None

            jump_vol = float(np.sqrt(jump_var))
            forward_iv = float(np.sqrt(fwd_var))

            # Bennett expected absolute move
            bennett_ret = np.exp(-jump_vol**2 / 2) * (2 * _norm_dist.cdf(jump_vol) - 1)
            bennett_abs = jump_vol * (2 * _norm_dist.pdf(0) + bennett_ret * (2 * _norm_dist.cdf(jump_vol) - 1))

            # --- Bid-ask execution cost for the calendar spread ---
            cal_mid_debit = d2['straddle_mid'] - d1['straddle_mid']
            # Worst-case: pay ask on back month, receive bid on front month
            cal_worst_debit = d2['straddle_ask'] - d1['straddle_bid']
            # Slippage = friction to fill both sides at worst vs mid
            cal_slippage = cal_worst_debit - cal_mid_debit if cal_mid_debit > 0 else None
            cal_slippage_pct = (cal_slippage / cal_mid_debit * 100) if (
                cal_slippage is not None and cal_mid_debit > 0
            ) else None

            # Spread signal thresholds (slippage as % of theoretical debit)
            if cal_slippage_pct is None or not d1['has_quotes'] or not d2['has_quotes']:
                spread_signal = 'Unknown'
            elif cal_slippage_pct < 10:
                spread_signal = 'Tight'
            elif cal_slippage_pct < 25:
                spread_signal = 'Moderate'
            elif cal_slippage_pct < 50:
                spread_signal = 'Wide'
            else:
                spread_signal = 'Very Wide'

            return {
                'jump_vol': round(jump_vol, 4),
                'forward_iv': round(forward_iv, 4),
                'bennett_move_pct': round(float(bennett_abs) * 100, 2),
                'earnings_risk_premium_pct': round((jump_vol - forward_iv) * 100, 2),
                't1_days': t1_days,
                't2_days': t2_days,
                'iv1': round(iv1, 4),
                'iv2': round(iv2, 4),
                # Spread metrics
                't1_spread_pct': round(d1['spread_pct'] * 100, 1) if d1['spread_pct'] is not None else None,
                't2_spread_pct': round(d2['spread_pct'] * 100, 1) if d2['spread_pct'] is not None else None,
                'cal_slippage_pct': round(cal_slippage_pct, 1) if cal_slippage_pct is not None else None,
                'spread_signal': spread_signal,
            }

        except Exception as e:
            logger.warning(f"Bennett move calc failed for {ticker}: {e}")
            return None

    def _bennett_from_chain_data(self, d1: dict, d2: dict,
                                  t1_days: int, t2_days: int,
                                  current_price: float) -> Optional[Dict]:
        """Compute Bennett variance extraction from pre-fetched chain data dicts."""
        try:
            if t1_days <= 0 or t2_days <= t1_days:
                return None
            iv1, iv2 = d1['iv'], d2['iv']
            T1, T2 = t1_days / 365.0, t2_days / 365.0
            fwd_var = (iv2**2 * T2 - iv1**2 * T1) / (T2 - T1)
            jump_var = iv1**2 * T1 - fwd_var * (T1 - 1.0 / 365.0)
            if jump_var <= 0 or fwd_var <= 0:
                return None
            jump_vol = float(np.sqrt(jump_var))
            forward_iv = float(np.sqrt(fwd_var))
            bennett_ret = np.exp(-jump_vol**2 / 2) * (2 * _norm_dist.cdf(jump_vol) - 1)
            bennett_abs = jump_vol * (2 * _norm_dist.pdf(0) + bennett_ret * (2 * _norm_dist.cdf(jump_vol) - 1))

            cal_mid = d2['straddle_mid'] - d1['straddle_mid']
            cal_worst = d2['straddle_ask'] - d1['straddle_bid']
            cal_slip = (cal_worst - cal_mid) if cal_mid > 0 else None
            cal_slip_pct = (cal_slip / cal_mid * 100) if cal_slip is not None and cal_mid > 0 else None
            if cal_slip_pct is None or not d1['has_quotes'] or not d2['has_quotes']:
                spread_signal = 'Unknown'
            elif cal_slip_pct < 10:   spread_signal = 'Tight'
            elif cal_slip_pct < 25:   spread_signal = 'Moderate'
            elif cal_slip_pct < 50:   spread_signal = 'Wide'
            else:                     spread_signal = 'Very Wide'

            return {
                'jump_vol': round(jump_vol, 4),
                'forward_iv': round(forward_iv, 4),
                'bennett_move_pct': round(float(bennett_abs) * 100, 2),
                'earnings_risk_premium_pct': round((jump_vol - forward_iv) * 100, 2),
                't1_days': t1_days, 't2_days': t2_days,
                'iv1': round(iv1, 4), 'iv2': round(iv2, 4),
                't1_spread_pct': round(d1['spread_pct'] * 100, 1) if d1['spread_pct'] else None,
                't2_spread_pct': round(d2['spread_pct'] * 100, 1) if d2['spread_pct'] else None,
                'cal_slippage_pct': round(cal_slip_pct, 1) if cal_slip_pct else None,
                'spread_signal': spread_signal,
            }
        except Exception as e:
            logger.warning(f"Bennett variance extraction failed: {e}")
            return None

    def scan(
        self,
        input_date: Optional[str] = None,
        workers: int = 4,
        term_slope_threshold: float = -0.004,
        tradier_key: Optional[str] = None,
        ibkr_host: Optional[str] = None,
        ibkr_port: Optional[int] = None,
        days_ahead: int = 1,
    ) -> Dict:
        """Run earnings scan and return structured results for the dashboard store.

        Returns dict with keys: recommended, near_misses, stock_metrics,
        iron_flies, scan_dates, thresholds, timing, candidates_count, data_source.

        term_slope_threshold: hard gate for term structure slope.
            -0.004 = backwardation required (iron fly strategy, default)
             0.010 = flat/any allowed (short straddle strategy)
        days_ahead: how many calendar days forward to look for earnings.
            Raises the DTE gate from 9 to (days_ahead + 9) so forward scans
            can find candidates whose nearest option expiry is further out.
        tradier_key: optional Tradier API key; when set, all options data
            comes from Tradier (real-time bid/ask, IV, greeks).
        """
        # Apply term structure threshold before scan
        self.scanner.term_slope_threshold = term_slope_threshold
        # Raise the DTE gate proportionally so forward scans are not blocked
        self.scanner.max_dte = 9 + max(0, days_ahead - 1)

        # If scanning forward and no explicit date provided, target the end of the window
        if input_date is None and days_ahead > 1:
            from datetime import timedelta as _td
            target = datetime.today().date() + _td(days=days_ahead - 1)
            input_date = target.strftime('%m/%d/%Y')

        # Set up live data fetcher if broker credentials provided
        tradier = None
        if tradier_key:
            tradier = TradierFetcher(tradier_key)
            data_source = 'tradier'
        elif ibkr_host and ibkr_port:
            try:
                from ibkr_fetcher import IBKRFetcher
                tradier = IBKRFetcher(ibkr_host, ibkr_port)  # same interface
                data_source = 'ibkr'
            except Exception as _e:
                logger.warning(f"IBKR fetcher init failed: {_e}")
                data_source = 'yfinance'
        else:
            data_source = 'yfinance'
        # Get scan dates first so we can count candidates
        scan_dates = {}
        candidates_count = 0
        try:
            post_date, pre_date = self.scanner.get_scan_dates(input_date)
            scan_dates = {
                'post_date': post_date.strftime('%Y-%m-%d'),
                'pre_date': pre_date.strftime('%Y-%m-%d'),
            }
        except Exception:
            post_date = pre_date = None

        # Build timing map BEFORE scan so we can count candidates
        timing_map = {}
        try:
            if post_date:
                for stock in self.scanner.fetch_earnings_data(post_date):
                    t = stock.get('ticker', '')
                    tmg = stock.get('timing', '')
                    # Include 'Unknown' timing — most sources can't resolve AMC/BMO so
                    # excluding them silently drops ~90% of valid candidates.
                    if t and tmg in ('Post Market', 'Unknown'):
                        timing_map[t] = {
                            'timing': tmg if tmg != 'Unknown' else 'Post Market',
                            'earnings_date': post_date.strftime('%Y-%m-%d'),
                        }
            if pre_date:
                for stock in self.scanner.fetch_earnings_data(pre_date):
                    t = stock.get('ticker', '')
                    tmg = stock.get('timing', '')
                    if t and tmg == 'Pre Market':
                        timing_map[t] = {
                            'timing': 'Pre Market',
                            'earnings_date': pre_date.strftime('%Y-%m-%d'),
                        }
                    elif t and tmg == 'Unknown' and t not in timing_map:
                        # Don't overwrite a post_date entry with Unknown pre_date
                        timing_map[t] = {
                            'timing': 'Post Market',
                            'earnings_date': pre_date.strftime('%Y-%m-%d'),
                        }
            candidates_count = len(timing_map)
        except Exception as e:
            logger.warning(f"Could not build timing map: {e}")

        # Run the actual scan
        recommended, near_misses, stock_metrics = self.scanner.scan_earnings(
            input_date=input_date,
            workers=workers,
        )

        # Enrich all qualifying tickers (iron fly + Bennett) in parallel
        from concurrent.futures import ThreadPoolExecutor, as_completed as _as_completed

        all_tickers = list(recommended) + [t for t, _ in near_misses]
        iron_flies: Dict = {}

        def _enrich(ticker: str) -> Dict:
            out: Dict = {'ticker': ticker, 'iron_fly': None, 'bennett': None}
            try:
                out['iron_fly'] = self.scanner.calculate_iron_fly_strikes(
                    ticker, tradier=tradier
                )
            except Exception as e:
                logger.warning(f"Iron fly calc failed for {ticker}: {e}")
                out['iron_fly'] = {'error': str(e)}

            m = stock_metrics.get(ticker)
            if m:
                timing_info = timing_map.get(ticker, {})
                earnings_date = timing_info.get('earnings_date')
                price = m.get('price', 0)
                if earnings_date and price > 0:
                    out['bennett'] = self._compute_bennett_move(
                        ticker, earnings_date, price, tradier=tradier
                    )
            return out

        n_workers = min(len(all_tickers), workers) if all_tickers else 1
        with ThreadPoolExecutor(max_workers=n_workers) as pool:
            for enriched in _as_completed(pool.submit(_enrich, t) for t in all_tickers):
                r = enriched.result()
                ticker = r['ticker']
                iron_flies[ticker] = r['iron_fly']

                bennett = r['bennett']
                m = stock_metrics.get(ticker)
                if not m or not bennett:
                    continue

                m['bennett_move_pct'] = bennett['bennett_move_pct']
                m['bennett_jump_vol'] = bennett['jump_vol']
                m['bennett_forward_iv'] = bennett['forward_iv']
                m['bennett_risk_premium_pct'] = bennett['earnings_risk_premium_pct']

                straddle_dollars = m.get('expected_move_dollars', 0)
                price = m.get('price', 0)
                if straddle_dollars and price > 0:
                    straddle_pct = (straddle_dollars / price) * 100
                    m['straddle_vs_bennett'] = round(straddle_pct - bennett['bennett_move_pct'], 2)
                    ratio = straddle_pct / bennett['bennett_move_pct'] if bennett['bennett_move_pct'] > 0 else 1.0
                    if ratio > 1.10:
                        m['premium_signal'] = 'Rich'
                    elif ratio > 1.03:
                        m['premium_signal'] = 'Slight rich'
                    elif ratio > 0.97:
                        m['premium_signal'] = 'Fair'
                    elif ratio > 0.90:
                        m['premium_signal'] = 'Slight cheap'
                    else:
                        m['premium_signal'] = 'Cheap'

                    vol_edge_pp = round((bennett['iv1'] - bennett['forward_iv']) * 100, 1)
                    m['vol_edge_pp'] = vol_edge_pp
                    m['richness_ratio'] = round(ratio, 2)
                    m['structure_rec'] = _recommend_structure(
                        vol_edge_pp, ratio, m.get('win_rate', 50)
                    )

                m['t1_spread_pct'] = bennett.get('t1_spread_pct')
                m['t2_spread_pct'] = bennett.get('t2_spread_pct')
                m['cal_slippage_pct'] = bennett.get('cal_slippage_pct')
                m['spread_signal'] = bennett.get('spread_signal', 'Unknown')

        return {
            'recommended': recommended,
            'near_misses': near_misses,
            'stock_metrics': stock_metrics,
            'iron_flies': iron_flies,
            'scan_dates': scan_dates,
            'timing': timing_map,
            'candidates_count': candidates_count,
            'data_source': data_source,
            'thresholds': {
                'iv_rv_pass': self.scanner.iv_rv_pass_threshold,
                'iv_rv_near_miss': self.scanner.iv_rv_near_miss_threshold,
                'term_slope': self.scanner.term_slope_threshold,
            },
        }

    def analyze_ticker(self, ticker: str) -> Dict:
        return self.scanner.analyze_ticker(ticker)

    def get_iron_fly(self, ticker: str) -> Dict:
        return self.scanner.calculate_iron_fly_strikes(ticker)
