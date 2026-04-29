"""
Black-Scholes Greeks Calculator

Computes option prices and Greeks from first principles.
Needed because YFinance does not provide Greeks — only IBKR does.
Uses standard BS formulas via scipy.stats.norm for CDF/PDF.
"""

import math
import numpy as np
from dataclasses import dataclass
from typing import Optional
from scipy.stats import norm
from scipy.optimize import brentq
import logging

logger = logging.getLogger(__name__)


@dataclass
class GreeksResult:
    """Option price and Greeks from Black-Scholes."""
    price: float       # BS theoretical price
    delta: float       # dV/dS
    gamma: float       # d²V/dS²
    vega: float        # dV/dσ (per 1 vol point, i.e. per 0.01)
    theta: float       # dV/dt (per calendar day, negative for long options)
    rho: float = 0.0   # dV/dr (per 1% rate change)


class BSGreeks:
    """Standard Black-Scholes option pricing and Greeks calculator."""

    def __init__(self, risk_free_rate: float = 0.05):
        self.r = risk_free_rate

    def calculate(self, S: float, K: float, T: float, sigma: float,
                  option_type: str = 'call') -> GreeksResult:
        """
        Calculate BS price and all Greeks.

        Parameters
        ----------
        S : float - Spot price
        K : float - Strike price
        T : float - Time to expiry in years (must be > 0)
        sigma : float - Implied volatility (annualized, e.g. 0.25 = 25%)
        option_type : str - 'call' or 'put'

        Returns
        -------
        GreeksResult with price, delta, gamma, vega, theta, rho
        """
        if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
            return GreeksResult(price=0.0, delta=0.0, gamma=0.0,
                                vega=0.0, theta=0.0, rho=0.0)

        sqrt_T = math.sqrt(T)
        d1 = (math.log(S / K) + (self.r + 0.5 * sigma ** 2) * T) / (sigma * sqrt_T)
        d2 = d1 - sigma * sqrt_T

        # Standard normal CDF and PDF
        Nd1 = norm.cdf(d1)
        Nd2 = norm.cdf(d2)
        nd1 = norm.pdf(d1)  # PDF at d1

        discount = math.exp(-self.r * T)

        is_call = option_type.lower() == 'call'

        # Price
        if is_call:
            price = S * Nd1 - K * discount * Nd2
        else:
            price = K * discount * norm.cdf(-d2) - S * norm.cdf(-d1)

        # Delta
        if is_call:
            delta = Nd1
        else:
            delta = Nd1 - 1.0

        # Gamma (same for calls and puts)
        gamma = nd1 / (S * sigma * sqrt_T)

        # Vega — per 0.01 change in vol (1 vol point)
        vega_raw = S * nd1 * sqrt_T  # dV/dσ for 1.0 change
        vega = vega_raw * 0.01  # per 1 vol point (0.01)

        # Theta — per calendar day
        theta_part1 = -(S * nd1 * sigma) / (2 * sqrt_T)
        if is_call:
            theta_annual = theta_part1 - self.r * K * discount * Nd2
        else:
            theta_annual = theta_part1 + self.r * K * discount * norm.cdf(-d2)
        theta = theta_annual / 365.0  # per calendar day

        # Rho — per 1% change in rate
        if is_call:
            rho = K * T * discount * Nd2 * 0.01
        else:
            rho = -K * T * discount * norm.cdf(-d2) * 0.01

        return GreeksResult(
            price=price,
            delta=delta,
            gamma=gamma,
            vega=vega,
            theta=theta,
            rho=rho,
        )

    def iv_from_price(self, S: float, K: float, T: float,
                      market_price: float, option_type: str = 'call') -> Optional[float]:
        """
        Solve for implied volatility given a market price using Brent's method.

        Returns None if no valid IV can be found.
        """
        if market_price <= 0 or T <= 0 or S <= 0 or K <= 0:
            return None

        # Intrinsic value check
        is_call = option_type.lower() == 'call'
        intrinsic = max(0, S - K) if is_call else max(0, K - S)
        if market_price < intrinsic * 0.99:  # allow tiny tolerance
            return None

        def objective(sigma):
            result = self.calculate(S, K, T, sigma, option_type)
            return result.price - market_price

        try:
            iv = brentq(objective, 0.001, 10.0, xtol=1e-6, maxiter=100)
            return iv
        except (ValueError, RuntimeError):
            return None

    def price_option(self, S: float, K: float, T: float, sigma: float,
                     option_type: str = 'call') -> float:
        """Convenience: return just the BS price."""
        return self.calculate(S, K, T, sigma, option_type).price
