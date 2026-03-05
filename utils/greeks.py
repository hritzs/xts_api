"""
Black-Scholes Greeks Calculator Module
Calculates IV, Delta, Gamma, Vega, Theta for options
"""

import numpy as np
from numba import njit
import math
from utils.logger import logger
import warnings
from typing import Dict, Optional

warnings.simplefilter('ignore')


@njit(cache=True, fastmath=True)
def _norm_pdf(x: float) -> float:
    return np.exp(-x**2 / 2.0) / (np.sqrt(2 * np.pi))


@njit(cache=True, fastmath=True)
def _norm_cdf(x: float) -> float:
    return (1.0 + math.erf(x / np.sqrt(2.0))) / 2.0


@njit(cache=True, fastmath=True)
def blackScholes(calculation_type: str, option_type: str, K: float, S: float,
                 T: float, sigma: float, r: float = 0.0) -> float:
    """
    Black-Scholes option pricing and Greeks calculator

    Args:
        calculation_type: 'p' (price), 'd' (delta), 'g' (gamma), 'v' (vega), 't' (theta), 'r' (rho)
        option_type: 'c' (call) or 'p' (put)
        K: Strike price
        S: Spot price
        T: Days to expiry
        sigma: Implied volatility (annualized)
        r: Risk-free rate (default 0)

    Returns:
        Calculated value based on calculation_type
    """
    K = float(K)
    S = float(S)
    T = float(T)
    T = T / 365.0
    calculation_type = calculation_type.lower()
    option_type = option_type.lower()

    if math.isnan(sigma) or sigma <= 0:
        return np.nan
    if math.isnan(K) or K <= 0:
        return np.nan
    if math.isnan(S) or S <= 0:
        return np.nan
    if math.isnan(T) or T <= 0:
        return np.nan

    denominator = sigma * np.sqrt(T)
    if denominator == 0:
        return np.nan
    d1 = (np.log(S / K) + (r + sigma**2 / 2) * T) / denominator
    d2 = d1 - denominator

    if calculation_type == "p":
        if option_type.startswith("c"):
            return S * _norm_cdf(d1) - K * np.exp(-r * T) * _norm_cdf(d2)
        elif option_type.startswith("p"):
            return K * np.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)

    elif calculation_type == "d":
        if option_type.startswith("c"):
            return _norm_cdf(d1)
        elif option_type.startswith("p"):
            return -_norm_cdf(-d1)

    elif calculation_type == "g":
        return _norm_pdf(d1) / (S * sigma * np.sqrt(T))

    elif calculation_type == "v":
        return S * _norm_pdf(d1) * np.sqrt(T) * 0.01

    elif calculation_type == "t":
        if option_type.startswith("c"):
            theta = -S * _norm_pdf(d1) * sigma / (2 * np.sqrt(T)) - r * K * np.exp(-r * T) * _norm_cdf(d2)
        elif option_type.startswith("p"):
            theta = -S * _norm_pdf(d1) * sigma / (2 * np.sqrt(T)) + r * K * np.exp(-r * T) * _norm_cdf(-d2)
        return theta / 365.0

    elif calculation_type == "r":
        if option_type.startswith("c"):
            return K * T * np.exp(-r * T) * _norm_cdf(d2) * 0.01
        elif option_type.startswith("p"):
            return -K * T * np.exp(-r * T) * _norm_cdf(-d2) * 0.01

    return np.nan


@njit(cache=True)
def implied_volatility(option_type: str, K: float, S: float, T: float,
                       option_price: float, r: float = 0.0,
                       tol: float = 0.0001, max_iterations: int = 100) -> float:
    """
    Calculate implied volatility using Newton-Raphson method.

    Args:
        option_type: 'c' (call) or 'p' (put)
        K: Strike price
        S: Spot price
        T: Days to expiry
        option_price: Market price of option
        r: Risk-free rate
        tol: Convergence tolerance
        max_iterations: Maximum iterations

    Returns:
        Implied volatility (annualized)
    """
    K = float(K)
    S = float(S)
    T = float(T)

    if math.isnan(option_price) or option_price <= 0:
        return np.nan
    if math.isnan(K) or K <= 0:
        return np.nan
    if math.isnan(S) or S <= 0:
        return np.nan
    if math.isnan(T) or T <= 0:
        return np.nan

    option_type = option_type.lower()

    if option_type.startswith('c'):
        intrinsic = max(0.0, S - K)
    else:
        intrinsic = max(0.0, K - S)

    if option_price <= intrinsic:
        return 0.0

    # --- FIX: Replaced try/except (ValueError, ZeroDivisionError) with guard checks ---
    # Numba @njit does NOT support catching multiple exception types as a tuple.
    # All error conditions are handled with explicit numerical guards instead.
    T_in_years = T / 365.0
    if T_in_years <= 0.0:
        return np.nan

    sqrt_T = math.sqrt(T_in_years)
    if sqrt_T <= 0.0 or S <= 0.0:
        return np.nan

    sigma_guess = (math.sqrt(2 * math.pi) / sqrt_T) * (option_price / S)

    if sigma_guess <= 0.03:
        sigma = 0.2
    elif sigma_guess >= 4.0:
        sigma = 2.5
    else:
        sigma = sigma_guess
    # ---------------------------------------------------------------------------------

    for i in range(max_iterations):
        price = blackScholes("p", option_type, K, S, T, sigma, r)
        vega_per_1_percent = blackScholes("v", option_type, K, S, T, sigma, r)

        if math.isnan(price) or math.isnan(vega_per_1_percent):
            return np.nan

        if price < 0.05:
            nudge_count = 0
            nudge_increment = 0.1 if option_type.startswith('c') else 0.01
            while blackScholes("p", option_type, K, S, T, sigma, r) < 0.05:
                sigma += nudge_increment
                nudge_count += 1
                if nudge_count > 50:
                    return np.nan
            price = blackScholes("p", option_type, K, S, T, sigma, r)
            vega_per_1_percent = blackScholes("v", option_type, K, S, T, sigma, r)

        if vega_per_1_percent == 0:
            return np.nan

        diff = price - option_price

        if abs(diff) < tol:
            return sigma

        sigma = sigma - (diff / vega_per_1_percent) / 100

        if sigma > 4.0:
            sigma = 4.0
        if sigma <= 0.001:
            sigma = 0.001

    return np.nan


def calculate_greeks_from_iv(option_type: str, K: float, S: float, T: float,
                             iv: float, r: float = 0.0) -> Dict[str, float]:
    """
    Calculate all Greeks for an option from a given Implied Volatility.
    """
    if math.isnan(iv) or iv <= 0:
        return {'iv': 0.0, 'delta': 0.0, 'gamma': 0.0, 'vega': 0.0, 'theta': 0.0}

    delta = blackScholes("d", option_type, K, S, T, iv, r)
    gamma = blackScholes("g", option_type, K, S, T, iv, r)
    vega  = blackScholes("v", option_type, K, S, T, iv, r)
    theta = blackScholes("t", option_type, K, S, T, iv, r)

    return {
        'iv':    round(iv, 4),
        'delta': round(delta, 4) if not math.isnan(delta) else 0.0,
        'gamma': round(gamma, 6) if not math.isnan(gamma) else 0.0,
        'vega':  round(vega,  4) if not math.isnan(vega)  else 0.0,
        'theta': round(theta, 4) if not math.isnan(theta) else 0.0,
    }


def calculate_all_greeks(option_type: str, K: float, S: float, T: float,
                         option_price: float, r: float = 0.0) -> Dict[str, float]:
    """
    Calculate all Greeks for an option.
    """
    _zero = {'iv': 0.0, 'delta': 0.0, 'gamma': 0.0, 'vega': 0.0, 'theta': 0.0}

    try:
        iv = implied_volatility(option_type, K, S, T, option_price, r)

        if math.isnan(iv) or iv == 0:
            logger.warning(
                f"Greeks: IV is NaN or 0 for {option_type} K={K} S={S} T={T:.4f} P={option_price}. Returning zero greeks."
            )
            return _zero

        delta = blackScholes("d", option_type, K, S, T, iv, r)
        gamma = blackScholes("g", option_type, K, S, T, iv, r)
        vega  = blackScholes("v", option_type, K, S, T, iv, r)
        theta = blackScholes("t", option_type, K, S, T, iv, r)

        return {
            'iv':    round(iv,    4) if not math.isnan(iv)    else 0.0,
            'delta': round(delta, 4) if not math.isnan(delta) else 0.0,
            'gamma': round(gamma, 6) if not math.isnan(gamma) else 0.0,
            'vega':  round(vega,  4) if not math.isnan(vega)  else 0.0,
            'theta': round(theta, 4) if not math.isnan(theta) else 0.0,
        }

    except Exception as e:
        logger.error(
            f"Greeks calculation error for {option_type} K={K} S={S} T={T} P={option_price}",
            exc_info=True
        )
        return _zero


def calculate_straddle_greeks(ce_greeks: Dict, pe_greeks: Dict,
                               ce_quantity: int, pe_quantity: int) -> Dict[str, float]:
    """
    Calculate combined Greeks for a SHORT straddle position.
    """
    ce_pos_delta = ce_greeks.get('delta', 0) * ce_quantity * -1
    pe_pos_delta = pe_greeks.get('delta', 0) * pe_quantity * -1

    ce_pos_gamma = ce_greeks.get('gamma', 0) * ce_quantity * -1
    pe_pos_gamma = pe_greeks.get('gamma', 0) * pe_quantity * -1

    ce_pos_vega  = ce_greeks.get('vega',  0) * ce_quantity * -1
    pe_pos_vega  = pe_greeks.get('vega',  0) * pe_quantity * -1

    ce_pos_theta = ce_greeks.get('theta', 0) * ce_quantity * -1
    pe_pos_theta = pe_greeks.get('theta', 0) * pe_quantity * -1

    return {
        'delta': ce_pos_delta + pe_pos_delta,
        'gamma': ce_pos_gamma + pe_pos_gamma,
        'vega':  ce_pos_vega  + pe_pos_vega,
        'theta': ce_pos_theta + pe_pos_theta,
    }


if __name__ == "__main__":
    print("=" * 60)
    print("Black-Scholes Greeks Calculator Test")
    print("=" * 60)

    option_type  = "c"
    strike       = 25600
    spot         = 25650
    dte          = 4
    market_price = 150

    print(f"\nOption Type: {option_type.upper()}")
    print(f"Strike:      {strike}")
    print(f"Spot:        {spot}")
    print(f"DTE:         {dte}")
    print(f"Market Price: ₹{market_price}")

    greeks = calculate_all_greeks(option_type, strike, spot, dte, market_price)

    print("\nCalculated Greeks:")
    print(f"IV:    {greeks['iv']:.2%}")
    print(f"Delta: {greeks['delta']:.4f}")
    print(f"Gamma: {greeks['gamma']:.6f}")
    print(f"Vega:  {greeks['vega']:.4f}")
    print(f"Theta: {greeks['theta']:.4f}")
    print("=" * 60)
