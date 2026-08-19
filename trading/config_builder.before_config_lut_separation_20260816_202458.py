"""
Configuration-Based Builder

Builds positions using a Look-Up Table (LUT) based on multiple factors:
- DTE score
- IV/IDV ratio score
- Straddle ratio score
- Build IV score

Final rule:
    - A combination of DTE, IV/IDV Ratio, Straddle Ratio, Build IV, Norm OG Gap, and Adj IV Chg
    - build/sell only if the corresponding entry in the LUT is "YES"
"""

import asyncio
import os
import platform
from pathlib import Path


# Supported symbols across API/builder
ALLOWED_CHAIN_SYMBOLS = {
    "NIFTY",
    "SENSEX",
}

import datetime as dt
from datetime import datetime, date, time, timedelta
# removed local datetime import, time, timedelta, date
from typing import Optional, Dict, Any, Tuple
from utils.helpers import get_synthetic_reference_spot, _safe_float
import pandas as pd
import math
import json
from utils.logger import logger
from utils.helpers import get_ist_now
from models.state import state
from trading.data_client import get_option_chain_from_service

# =============================================================================
# FILE PATHS (MATCH routes.py)
# =============================================================================

WINDOWS_GAMMA_DATA_FILE = r"\\172.16.1.85\Shared\Hardik\Project_Codes\GammaShortDailyProcess\GammaShortDailydata.csv"
WINDOWS_LUT_0916 = r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0916_Custom.csv"
WINDOWS_LUT_0917 = r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0917_Custom.csv"
WINDOWS_LUT_0918 = r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0918_Custom.csv"
WINDOWS_LUT_0919 = r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0919_Custom.csv"
WINDOWS_LUT_0920 = r"\\172.16.1.85\Shared\Hardik\Custom_5_Stage_Run104-436\LUT_0920_Onwards_Custom.csv"

GAMMA_DATA_FILE = os.getenv("GAMMA_DATA_FILE", WINDOWS_GAMMA_DATA_FILE)

LUT_0916 = os.getenv("LUT_0916", WINDOWS_LUT_0916)
LUT_0917 = os.getenv("LUT_0917", WINDOWS_LUT_0917)
LUT_0918 = os.getenv("LUT_0918", WINDOWS_LUT_0918)
LUT_0919 = os.getenv("LUT_0919", WINDOWS_LUT_0919)
LUT_0920 = os.getenv("LUT_0920", WINDOWS_LUT_0920)
NETWORK_SHARE_MOUNT = os.getenv("NETWORK_SHARE_MOUNT", "").strip()

# =============================================================================
# HELPERS (copied/aligned with routes.py)
# =============================================================================

score_check_cutoff_time = time(18, 30, 0)

def _is_lut_stage_mode(config) -> bool:
    """
    Dedicated five-stage LUT selling mode.

    True:
        - Ignore configured entry_time as the build decision time.
        - Evaluate at minute boundaries starting from 09:16.
        - Use LUT_0916 / 0917 / 0918 / 0919 / 0920+.
        - Stop at configured exit_time.
        - Build immediately on first YES.

    False:
        - Preserve existing configuration-based builder behavior.
    """
    try:
        return bool(config.get("lut_based_selling", False))
    except Exception:
        return False


def _normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper()

def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None:
            return default
        if isinstance(value, str):
            value = value.replace(",", "").strip()
            if not value:
                return default
        return int(float(value))
    except Exception:
        return default


def _find_first_matching_column(df: pd.DataFrame, candidates) -> Optional[str]:
    normalized = {str(col).strip().lower(): col for col in df.columns}
    for candidate in candidates:
        key = candidate.strip().lower()
        if key in normalized:
            return normalized[key]
    return None


def _pick_symbol_row(df: pd.DataFrame, symbol: str) -> Optional[pd.Series]:
    symbol = _normalize_symbol(symbol)

    # First, try a symbol/index/underlying column
    for col in df.columns:
        col_l = str(col).strip().lower()
        if "symbol" in col_l or "index" in col_l or "underlying" in col_l:
            tmp = df[df[col].astype(str).str.upper().str.strip() == symbol]
            if not tmp.empty:
                return tmp.iloc[-1]

    # Then try the last row by date
    date_col = _find_first_matching_column(df, ["Date", "date"])
    if date_col:
        temp = df.copy()
        temp[date_col] = pd.to_datetime(temp[date_col], errors="coerce")
        temp = temp.dropna(subset=[date_col]).sort_values(date_col)
        if not temp.empty:
            return temp.iloc[-1]

    # Fallback: last non-empty row
    temp = df.dropna(how="all")
    if not temp.empty:
        return temp.iloc[-1]

    return None


def _resolve_data_file_path(raw_path: str) -> str:
    if not raw_path:
        return raw_path

    if os.path.exists(raw_path):
        return raw_path

    is_linux = platform.system().lower() == "linux"

    if is_linux and raw_path.startswith("\\\\") and NETWORK_SHARE_MOUNT:
        unc = raw_path.lstrip("\\")
        parts = [p for p in unc.split("\\") if p]
        if len(parts) >= 3:
            mapped = Path(NETWORK_SHARE_MOUNT, *parts[2:])
            mapped_str = str(mapped)
            if os.path.exists(mapped_str):
                return mapped_str

    return raw_path


def _try_existing_file(raw_path: str) -> Optional[str]:
    resolved = _resolve_data_file_path(raw_path)
    return resolved if os.path.exists(resolved) else None


def _load_gamma_data(symbol: str) -> Dict[str, float]:
    """
    Loads all required historical data points for the LUT calculation
    from the consolidated GammaShortDailydata.csv file.
    """
    csv_path = _try_existing_file(GAMMA_DATA_FILE)
    if not csv_path:
        raise FileNotFoundError(f"Gamma data file not found: {GAMMA_DATA_FILE}")

    df = pd.read_csv(csv_path)
    row = _pick_symbol_row(df, symbol)
    if row is None:
        raise ValueError(f"No data row found for {symbol} in Gamma data file.")

    # Define a mapping from desired keys to possible column names in the CSV
    column_map = {
        "idv_with_prev": ["IDV_With_Prev", "idv_with_prev"],
        "idv_pure": ["IDV_Pure", "idv_pure"],
        "adj_iv_from_file": ["Adj_IV", "adj_iv"],
        "future_price_ref": ["Future", "future"],
        "prev_day_straddle": ["Straddle", "straddle"],
        "adj_iv_chg_raw": ["Final_IV_Chg", "final_iv_chg"],
        "prev2_adj_iv": ["Prev2_Adj_IV", "prev2_adj_iv"],
    }

    loaded_data = {}
    for key, candidates in column_map.items():
        col_name = _find_first_matching_column(df, candidates)
        if not col_name:
            raise ValueError(f"Could not find required column for '{key}' in {GAMMA_DATA_FILE}")
        loaded_data[key] = _safe_float(row[col_name], 0.0)

    return loaded_data


def _parse_expiry_date(expiry_value) -> Optional[date]:
    if expiry_value is None:
        return None

    text = str(expiry_value).strip()
    if not text or text.upper() == "N/A":
        return None

    formats = [
        "%d%b%Y",
        "%d%b%y",
        "%d-%b-%Y",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except Exception:
            continue
    return None


def _compute_rounded_dte(today: date, expiry_date: date) -> int:
    raw = (expiry_date - today).days
    if raw < 0:
        return raw
    if raw == 0:
        return 0
    return raw + 1


def _get_expiry_and_dte(chain_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[int]]:
    expiry = (
        chain_data.get("expiry")
        or chain_data.get("current_expiry")
        or chain_data.get("selected_expiry")
    )
    expiry_date = _parse_expiry_date(expiry)

    if expiry_date is None:
        first_row = None
        for row in chain_data.get("chain", []):
            first_row = row
            break
        if first_row:
            expiry = first_row.get("expiry") or first_row.get("Expiry") or expiry
            expiry_date = _parse_expiry_date(expiry)

    if expiry_date is None:
        return expiry, None

    today = get_ist_now().date()
    dte = _compute_rounded_dte(today, expiry_date)
    return expiry, dte


def _get_lut_day_bucket() -> int:
    """Maps current weekday to a specific DTE bucket for the LUT."""
    weekday = get_ist_now().weekday()  # Monday is 0, Sunday is 6
    # Monday: 2, Tuesday: 1, Wednesday: 6, Thursday: 5, Friday: 4, Saturday: 3
    mapping = {0: 2, 1: 1, 2: 6, 3: 5, 4: 4, 5: 3}
    return mapping.get(weekday, 7) # Default to 7 for Sunday or errors


def _get_lut_iv_ratio_bucket(value: float) -> str:
    if value <= 0.65: return "<=0.65"
    if value <= 0.80: return "0.66-0.80"
    if value <= 0.90: return "0.81-0.90"
    if value <= 0.95: return "0.91-0.95"
    if value <= 1.00: return "0.96-1.00"
    if value <= 1.05: return "1.01-1.05"
    if value <= 1.10: return "1.06-1.10"
    if value <= 1.20: return "1.11-1.20"
    if value <= 1.30: return "1.21-1.30"
    return ">1.30"


def _get_lut_straddle_ratio_bucket(value: float) -> str:
    if value <= 0.95: return "<=0.95"
    if value <= 1.00: return "0.96-1.00"
    if value <= 1.05: return "1.01-1.05"
    if value <= 1.10: return "1.06-1.10"
    if value <= 1.15: return "1.11-1.15"
    if value <= 1.20: return "1.16-1.20"
    if value <= 1.30: return "1.21-1.30"
    if value <= 1.50: return "1.31-1.50"
    if value <= 1.75: return "1.51-1.75"
    if value <= 2.00: return "1.76-2.00"
    return ">2.00"


def _get_lut_build_iv_bucket(value: float) -> str:
    if value < 0.08: return "<0.08"
    if value <= 0.12: return "0.08-0.12"
    if value <= 0.16: return "0.12-0.16"
    if value <= 0.20: return "0.16-0.20"
    return ">=0.20"


def _get_lut_norm_og_gap_bucket(value: float) -> str:
    if value < -0.0075: return "<-0.75%"
    if value > 0.0075: return ">0.75%"
    return "Between -0.75% and 0.75%"


def _get_lut1_adj_iv_chg_bucket(value: float) -> str:
    """Bucketing for LUT_Tax0_AdjL-0.0075_AdjH0.01.csv"""
    if value < -0.0075: return "<-0.75%"
    if value <= 0.01: return "-0.75% to 1.00%"
    return ">1.00%"


def _get_lut2_adj_iv_chg_bucket(value: float) -> str:
    """Bucketing for new Custom LUT"""
    if value < -0.0075:
        return "<-0.0075"
    if value <= 0.0050:
        return "-0.0075 to 0.0050"
    return ">0.0050"



def _get_trade_decision_from_lut(
    dte_bucket,
    iv_ratio_bucket,
    straddle_ratio_bucket,
    build_iv_bucket,
    norm_og_gap_bucket,
    adj_iv_chg_bucket,
    lut_path, silent_logs: bool = True,
):
    """
    Look up the exact LUT row and return the trade decision.
    """

    if not lut_path:
        raise FileNotFoundError(f"LUT file not found: {lut_path}")

    df = pd.read_csv(lut_path)

    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)
    (logger.debug if not silent_logs else lambda *a,**kw: None)("[LUT LOOKUP]")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(
        f"DTE={dte_bucket}, "
        f"IV_Ratio={iv_ratio_bucket}, "
        f"Straddle_Ratio={straddle_ratio_bucket}, "
        f"Build_IV={build_iv_bucket}, "
        f"Norm_OG_Gap={norm_og_gap_bucket}, "
        f"Adj_IV_Chg={adj_iv_chg_bucket}"
    )
    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)

    try:
        result = df[
            (df["DTE"] == dte_bucket) &
            (df["IV_Ratio"] == iv_ratio_bucket) &
            (df["Straddle_Ratio"] == straddle_ratio_bucket) &
            (df["Build_IV"] == build_iv_bucket) &
            (df["Norm_OG_Gap"] == norm_og_gap_bucket) &
            (df["Adj_IV_Chg"] == adj_iv_chg_bucket)
        ]

        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Matched Rows : {len(result)}")

    except Exception:
        logger.exception("LUT lookup failed")
        return {
            "decision": "NO",
            "matched_row": None,
        }

    if result.empty:
        (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)
        (logger.debug if not silent_logs else lambda *a,**kw: None)("[NO LUT MATCH]")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(df.head(25).to_string())
        (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)

        return {
            "decision": "NO",
            "matched_row": None,
        }

    matched_row = result.iloc[0].to_dict()

    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)
    (logger.debug if not silent_logs else lambda *a,**kw: None)("[MATCHED LUT ROW]")

    for k, v in matched_row.items():
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"{k:<30} = {v}")

    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)

    decision = str(matched_row["Trade"]).strip().upper()

    return {
        "decision": decision,
        "matched_row": matched_row,
    }


def _get_current_lut_file(now=None):
    now = now or get_ist_now()
    t = now.time()

    if time(9,16) <= t < time(9,17):
        return _try_existing_file(LUT_0916)

    elif time(9,17) <= t < time(9,18):
        return _try_existing_file(LUT_0917)

    elif time(9,18) <= t < time(9,19):
        return _try_existing_file(LUT_0918)

    elif time(9,19) <= t < time(9,20):
        return _try_existing_file(LUT_0919)

    else:
        return _try_existing_file(LUT_0920)


def _calculate_adj_iv(live_iv: float, dte: int) -> float:
    """Calculates adjusted IV based on the day of the week."""
    weekday = get_ist_now().weekday()  # Monday is 0
    # Wednesday, Thursday, Friday, Saturday
    if weekday in [2, 3, 4, 5] and dte > 1:
        return live_iv * math.sqrt(dte / (dte - 1))
    # Monday, Tuesday
    return live_iv


# =============================================================================
# SCORE PAYLOAD (builder version, aligned with _compute_config_score)
# =============================================================================


def _compute_score_payload(
    symbol: str,
    live_iv: float,
    live_straddle: float,
    chain_data: Dict[str, Any],
    manual_historical_idv: Optional[float] = None,
    manual_prev_day_straddle: Optional[float] = None,
    tp_points: Optional[float] = None,
    tp_bps: Optional[float] = None, # New
    manual_spot_price: Optional[float] = None,
    straddle_price_drop_trigger: Optional[float] = None, # New parameter
    exit_at_straddle: Optional[float] = None,
    straddle_price_drop_pct_sqf: Optional[float] = None, # New parameter
    use_live_spot_for_og: bool = False, silent_logs: bool = True,
) -> Dict[str, Any]:
    """
    Computes score components for builder using the same logic
    as _compute_config_score in routes.py.
    """
    symbol = _normalize_symbol(symbol)
    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)
    (logger.debug if not silent_logs else lambda *a,**kw: None)("[ENTER _compute_score_payload]")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"live_iv={live_iv}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"live_straddle={live_straddle}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 120)
    warnings = []    
    
    atm_row = next((row for row in chain_data.get("chain", []) if row.get("is_atm")), None)
    if not atm_row:
        # Fallback to finding ATM by value if 'is_atm' flag is missing
        atm_value = chain_data.get("atm")
        if atm_value:
            atm_row = next((row for row in chain_data.get("chain", []) if row.get("strike") == atm_value), None)

    reference_spot = get_synthetic_reference_spot(chain_data)
    expiry_text, current_dte = _get_expiry_and_dte(chain_data)

    historical_idv = 0.0
    prev_day_straddle = 0.0
    idv_reference = 0.0

    historical_idv_source = "auto"
    prev_day_straddle_source = "auto"
    # --- NEW: LUT based logic ---
    adj_idv = 0.0
    adj_iv = 0.0
    sell_allowed = False
    decision = "NO"
    selected_lut = None
    lut_payload = {} # Initialize to prevent reference before assignment
    matched_row = None

    try:
        gamma_data = _load_gamma_data(symbol)
        prev_idv = gamma_data.get("idv_with_prev", 0.0)
        intraday_idv = gamma_data.get("idv_pure", 0.0)
        # Manual overrides
        if manual_historical_idv is not None and manual_historical_idv > 0:
            prev_idv = _safe_float(manual_historical_idv, 0.0)
            historical_idv_source = "manual"

        if prev_day_straddle <= 0 and manual_prev_day_straddle is not None and manual_prev_day_straddle > 0:
            prev_day_straddle = _safe_float(manual_prev_day_straddle, 0.0)
            prev_day_straddle_source = "manual"

        adj_iv_from_file = gamma_data.get("adj_iv_from_file", 0.0)
        future_price_ref = gamma_data.get("future_price_ref", 0.0)
        prev_day_straddle = gamma_data.get("prev_day_straddle", 0.0)
        prev2_adj_iv = gamma_data.get("prev2_adj_iv", 0.0)
        prev_day_adj_iv = prev2_adj_iv # Use a clearer name for the response

        # Corrected calculation for Adj_IV_Chg normalization
        adj_iv_chg_raw = gamma_data.get("adj_iv_chg_raw", 0.0)
        prev2_adj_iv = gamma_data.get("prev2_adj_iv", 0.0)
        adj_iv_chg = (adj_iv_chg_raw * 10) / prev2_adj_iv if prev2_adj_iv > 0 else 0.0 # Use Final_IV_Chg * 10 / Prev2_Adj_IV

        # Calculate adj_idv
        # adj_idv = prev_idv * 0.5 + intraday_idv * 0.2 + adj_iv_from_file * 0.3
        adj_idv = (prev_idv * 0.5) + (intraday_idv * 0.2) + (adj_iv_from_file * 0.3) if prev_idv > 0 and intraday_idv > 0 else 0.0

        # Calculate adj_iv using the live_iv passed into the function
        adj_iv = _calculate_adj_iv(live_iv / 100.0, current_dte) # live_iv is in %, convert to decimal

        (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 90)
        (logger.debug if not silent_logs else lambda *a,**kw: None)("[BUILD-IV DEBUG]")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Live IV (%)              : {live_iv}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Live IV (decimal)        : {live_iv / 100.0}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Current DTE              : {current_dte}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Calculated Build IV      : {adj_iv}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 90)

        # Calculate other LUT factors
        iv_ratio = adj_iv / adj_idv if adj_idv > 0 else 0
        straddle_ratio = prev_day_straddle / live_straddle if live_straddle > 0 else 0

        # Norm_OG_Gap: Use stored 9:16 price if available, otherwise fallback to ATM strike proxy.
        if use_live_spot_for_og:
            synthetic_price_ref = reference_spot
            price_ref_source = "Live Synthetic Spot"
        else:
            synthetic_price_ref = getattr(state, 'synthetic_prices_918', {}).get(symbol)
            price_ref_source = "9:18 Capture"
            if manual_spot_price and manual_spot_price > 0:
                synthetic_price_ref = manual_spot_price
                price_ref_source = "Manual Override"

        if not synthetic_price_ref or synthetic_price_ref <= 0:
            synthetic_price_ref = reference_spot
            price_ref_source = "Live Synthetic Proxy"

        og_gap_pct = (synthetic_price_ref - future_price_ref) / future_price_ref if future_price_ref > 0 else 0.0
        # Use adj_iv (decimal) for normalization, converting it to percentage form
        norm_og_gap = (og_gap_pct * 19) / (adj_iv * 100) if adj_iv > 0 else 0.0

        # Get buckets
        dte_bucket_val = _get_lut_day_bucket()
        iv_ratio_bucket_val = _get_lut_iv_ratio_bucket(iv_ratio)
        straddle_ratio_bucket_val = _get_lut_straddle_ratio_bucket(straddle_ratio)
        build_iv_bucket_val = _get_lut_build_iv_bucket(adj_iv)
        norm_og_gap_bucket_val = _get_lut_norm_og_gap_bucket(norm_og_gap)
        
        lut_params = {
            "dte_bucket": dte_bucket_val,

            "iv_ratio": iv_ratio,
            "iv_ratio_bucket": iv_ratio_bucket_val,

            "straddle_ratio": straddle_ratio,
            "straddle_ratio_bucket": straddle_ratio_bucket_val,

            "build_iv": adj_iv,
            "build_iv_bucket": build_iv_bucket_val,

            "norm_og_gap": norm_og_gap,
            "norm_og_gap_bucket": norm_og_gap_bucket_val,

            "adj_iv_chg": adj_iv_chg,
        }

        # For display purposes
        lut_payload = {k: v for k, v in lut_params.items() if k != 'adj_iv_chg'}

        lut_file = _get_current_lut_file()
        selected_lut = os.path.basename(lut_file) if lut_file else None

        (logger.debug if not silent_logs else lambda *a,**kw: None)("="*80)
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"[LUT] Current Time : {get_ist_now().strftime('%H:%M:%S')}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"[LUT] Selected LUT : {lut_file}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("="*80)

        (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 100)
        (logger.debug if not silent_logs else lambda *a,**kw: None)("[LUT INPUTS]")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"DTE                  : {current_dte}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"DTE Bucket           : {dte_bucket_val}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Live IV              : {live_iv}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Build IV             : {adj_iv}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Build IV Bucket      : {build_iv_bucket_val}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Adj IDV              : {adj_idv}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"IV Ratio             : {iv_ratio:.6f}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"IV Ratio Bucket      : {iv_ratio_bucket_val}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Live Straddle        : {live_straddle}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Prev Day Straddle    : {prev_day_straddle}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Straddle Ratio       : {straddle_ratio:.6f}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Straddle Bucket      : {straddle_ratio_bucket_val}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Future Ref           : {future_price_ref}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Reference Spot       : {synthetic_price_ref}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Reference Source     : {price_ref_source}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"OG Gap %%             : {og_gap_pct * 100:.6f}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Norm OG Gap          : {norm_og_gap:.6f}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Norm OG Bucket       : {norm_og_gap_bucket_val}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Adj IV Change        : {adj_iv_chg:.6f}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"Adj IV Bucket        : {_get_lut2_adj_iv_chg_bucket(adj_iv_chg)}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 100)

        lut_result = _get_trade_decision_from_lut(
            lut_path=lut_file,
            dte_bucket=lut_params["dte_bucket"],
            iv_ratio_bucket=lut_params["iv_ratio_bucket"],
            straddle_ratio_bucket=lut_params["straddle_ratio_bucket"],
            build_iv_bucket=lut_params["build_iv_bucket"],
            norm_og_gap_bucket=lut_params["norm_og_gap_bucket"],
            adj_iv_chg_bucket=_get_lut2_adj_iv_chg_bucket(adj_iv_chg), silent_logs=silent_logs
        )

        decision = lut_result["decision"]
        matched_row = lut_result["matched_row"]

        (logger.debug if not silent_logs else lambda *a,**kw: None)("[LUT MATCHED ROW]")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(json.dumps(matched_row, indent=4, default=str))

        sell_allowed = decision == "YES"

        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"[LUT] Decision      : {decision}")
        (logger.debug if not silent_logs else lambda *a,**kw: None)(f"[LUT] Sell Allowed : {sell_allowed}")

    except Exception as e:
        warnings.append(f"LUT decision error: {str(e)}")
        sell_allowed = False
        selected_lut = None
        decision = "ERROR"

    manual_load_required = bool(idv_reference <= 0 or prev_day_straddle <= 0)
    manual_load_message = None
    if manual_load_required:
        manual_load_message = (
            "Auto-fetch failed for one or more required inputs. "
            "Please enter manual values in UI for historical IDV or previous-day straddle."
        )



    calculations = {
        "live_iv": live_iv,
        "reference_iv": adj_idv,

        "iv_ratio": {
            "formula": "live_iv / reference_iv",
            "value": lut_params.get("iv_ratio"),
            "bucket": lut_params.get("iv_ratio_bucket"),
        },

        "straddle_ratio": {
            "formula": "prev_day_straddle / live_straddle",
            "value": lut_params.get("straddle_ratio"),
            "bucket": lut_params.get("straddle_ratio_bucket"),
        },

        "build_iv": {
            "formula": "adjusted_iv",
            "value": adj_iv,
            "bucket": lut_params.get("build_iv_bucket"),
        },

        "norm_og_gap": {
            "formula": "(spot-reference_spot)/reference_spot",
            "value": lut_params.get("norm_og_gap"),
            "bucket": lut_params.get("norm_og_gap_bucket"),
        },

        "adj_iv_chg": {
            "formula": "adj_iv-prev_adj_iv",
            "value": adj_iv_chg,
            "bucket": _get_lut2_adj_iv_chg_bucket(adj_iv_chg),
        },
    }

    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 90)
    (logger.debug if not silent_logs else lambda *a,**kw: None)("[FINAL SCORE PAYLOAD]")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"adj_iv               = {adj_iv}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"adj_idv              = {adj_idv}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"iv_ratio             = {iv_ratio}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"straddle_ratio       = {straddle_ratio}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"norm_og_gap          = {norm_og_gap}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)(f"matched_row          = {matched_row}")
    (logger.debug if not silent_logs else lambda *a,**kw: None)("=" * 90)




    result = {
        "symbol": symbol,
        "expiry_date": expiry_text,
        "current_dte": current_dte,
        "live_iv": round(live_iv, 6),
        "adj_iv": round(adj_iv, 6),
        "live_straddle": round(live_straddle, 2),
        "reference_spot": round(reference_spot, 2) if reference_spot > 0 else None,
        "synthetic_spot": round(_safe_float(chain_data.get("synthetic_spot"), 0.0), 2),
        "fut_ltp": round(_safe_float(chain_data.get("fut_ltp"), 0.0), 2),
        "spot_source": "synthetic_spot" if reference_spot > 0 else None,
        "adj_idv": round(adj_idv, 6) if adj_idv > 0 else None,
        "prev_day_straddle": round(prev_day_straddle, 6) if prev_day_straddle > 0 else None,
        "prev_day_straddle_source": prev_day_straddle_source if prev_day_straddle > 0 else None,
        "prev_day_adj_iv": round(prev_day_adj_iv, 6) if 'prev_day_adj_iv' in locals() and prev_day_adj_iv > 0 else None,
        "future_price_ref": round(future_price_ref, 2) if 'future_price_ref' in locals() and future_price_ref > 0 else None,
        "synthetic_price_ref": round(synthetic_price_ref, 2) if 'synthetic_price_ref' in locals() and synthetic_price_ref > 0 else None,
        "lut_payload": {
            "DTE": lut_params.get("dte_bucket"),
            "IV_Ratio": lut_params.get("iv_ratio_bucket"),
            "Straddle_Ratio": lut_params.get("straddle_ratio_bucket"),
            "Build_IV": lut_params.get("build_iv_bucket"),
            "Norm_OG_Gap": lut_params.get("norm_og_gap_bucket"),
            "Adj_IV_Chg": _get_lut2_adj_iv_chg_bucket(adj_iv_chg),
        },
        "selected_lut": selected_lut,
        "matched_row": matched_row,
        "calculations": calculations,
        "decision": decision,
        "trade_decision": decision,
        "sell_allowed": sell_allowed,
        "score_available": decision != "ERROR",
        "manual_load_required": manual_load_required,
        "manual_load_message": manual_load_message,
        "rule": "Sell only if LUT decision is YES",
        "resolved_paths": {
            "gamma_data_file": _resolve_data_file_path(GAMMA_DATA_FILE),
            "selected_lut": selected_lut,
        },
        "atm_strike": atm_row.get("strike") if atm_row else None,
        "warnings": warnings,
        "straddle_price_drop_trigger": _safe_float(straddle_price_drop_trigger, 0.0), # Include in payload
        "exit_at_straddle": _safe_float(exit_at_straddle, 0.0),
        "straddle_price_drop_pct_sqf": _safe_float(straddle_price_drop_pct_sqf, 0.0), # Include in payload
    }

    # --- ADD MISSING RATIOS FOR UI ---
    if 'iv_ratio' in locals(): result['iv_idv_ratio'] = iv_ratio
    if 'straddle_ratio' in locals(): result['straddle_ratio'] = straddle_ratio
    if 'og_gap_pct' in locals(): result['og_gap_pct'] = og_gap_pct
    if 'norm_og_gap' in locals(): result['norm_og_gap'] = norm_og_gap
    if 'adj_iv_chg' in locals(): result['adj_iv_chg'] = adj_iv_chg
    return result
 
# =============================================================================
# BUILD WITH CONFIG (uses the score payload above)
# =============================================================================


async def build_with_config(config: Dict, trade_uid: str = None) -> Optional[str]:
    """
    Build position with score-based configuration filters.

    Final rule:
        build only if LUT decision is YES

    Manual fallback supported from UI config:
        manual_historical_idv
        manual_prev_day_straddle

    Single-source-of-truth rule:
    - Read published option chain first
    - Service fallback allowed only to fetch the published chain
    - Never rely on mutable option_chains as primary source
    """
    from trading.builder import build_straddle

    try:
        entry_parts = list(map(int, config["entry_time"].split(":")))
        exit_parts = list(map(int, config["exit_time"].split(":")))
        entry_time = time(entry_parts[0], entry_parts[1], entry_parts[2] if len(entry_parts) > 2 else 0)
        exit_time = time(exit_parts[0], exit_parts[1], exit_parts[2] if len(exit_parts) > 2 else 0)

        sl_start_time_str = config.get("sl_start_time")
        hedge_start_time_str = config.get("hedge_start_time")
        roll_start_time_str = config.get("roll_start_time")

        manual_latest_idv = _safe_float(config.get("manual_latest_idv"), 0.0)
        manual_historical_idv = _safe_float(config.get("manual_historical_idv"), 0.0)
        manual_prev_day_straddle = _safe_float(config.get("manual_prev_day_straddle"), 0.0)
        tp_points = _safe_float(config.get("tp_points"), 0.0)
        manual_spot_price = _safe_float(config.get("manual_spot_price"), 0.0)
        use_live_spot_for_og = bool(config.get("use_live_spot_for_og", False))

        symbol_upper = str(config["symbol"]).upper().strip()
        if symbol_upper not in ALLOWED_CHAIN_SYMBOLS:
            logger.warning(f"Unsupported config-build symbol: {symbol_upper}")
            return None

        logger.debug("=" * 100)
        logger.debug(f"CONFIG BUILD: {config['symbol']}")
        logger.debug(f"   Entry: {config['entry_time']}, Exit: {config['exit_time']}")
        logger.debug(f"   Size: {config['size']} lots")
        logger.debug("   Rule: LUT Decision is YES")
        logger.debug(f"   Manual Overrides: historical_idv={config.get('manual_historical_idv')}, "
                    f"prev_day_straddle={config.get('manual_prev_day_straddle')}, "
                    f"spot_price={config.get('manual_spot_price')}")
        if sl_start_time_str:
            logger.debug(f"   SL Start: {sl_start_time_str}")
        if hedge_start_time_str:
            logger.debug(f"   Hedge Start: {hedge_start_time_str}")
        if roll_start_time_str:
            logger.debug(f"   Roll Start: {roll_start_time_str}")
        logger.debug("=" * 100)

        # PRE_ENTRY_CHECK_SECONDS = 2
        now_dt_init = get_ist_now()
        today = now_dt_init.date()
        entry_datetime = dt.datetime.combine(today, entry_time, tzinfo=now_dt_init.tzinfo)
        # pre_entry_check_time = (entry_datetime - timedelta(seconds=PRE_ENTRY_CHECK_SECONDS)).time()

        # filters_passed_at_least_once = False
        build_triggered = False
        last_checked_minute = None

        async def _sleep_with_cancel(total_seconds: float) -> bool:
            elapsed = 0.0
            while elapsed < total_seconds:
                if trade_uid and state.cancellation_flags.get(trade_uid):
                    logger.debug(f"🛑 Build task for {trade_uid} detected cancellation flag during sleep. Aborting.")
                    return False
                step = min(1.0, total_seconds - elapsed)
                await asyncio.sleep(step)
                elapsed += step
            return True

        # This function is part of the old minute-by-minute check logic
        async def _sleep_until_next_minute_boundary(now_dt: datetime, today_date: date) -> bool:
            next_minute_dt = now_dt.replace(second=0, microsecond=0) + timedelta(minutes=1)
            cutoff_dt = dt.datetime.combine(today_date, score_check_cutoff_time, tzinfo=now_dt.tzinfo)
            exit_dt = dt.datetime.combine(today_date, exit_time, tzinfo=now_dt.tzinfo)

            target_dt = min(next_minute_dt, cutoff_dt, exit_dt)

            if now_dt.time() < entry_time:
                entry_dt = dt.datetime.combine(today_date, entry_time, tzinfo=now_dt.tzinfo)
                target_dt = min(target_dt, entry_dt)

            sleep_interval = max(0.0, (target_dt - now_dt).total_seconds())
            logger.debug(f"Waiting for minute-end score check at {target_dt.time()}...")
            return await _sleep_with_cancel(sleep_interval)

        while True:
            if trade_uid and state.cancellation_flags.get(trade_uid):
                logger.debug(f"🛑 Build task for {trade_uid} detected cancellation flag. Aborting.")
                return None

            now_dt = get_ist_now()
            today = now_dt.date()
            now_time = now_dt.time()

            if now_time >= exit_time:
                logger.warning("Exit time reached, aborting build.")
                return None
            
            # ========================================================
            # OPTIONAL FIVE-STAGE LUT SELLING MODE
            # ========================================================
            if _is_lut_stage_mode(config):

                LUT_START_TIME = time(9, 16, 0)

                # Before 09:16 -> wait for exact 09:16 boundary.
                if now_time < LUT_START_TIME:
                    start_dt = datetime.combine(
                        today,
                        LUT_START_TIME,
                        tzinfo=now_dt.tzinfo,
                    )

                    wait_seconds = max(
                        0.1,
                        (start_dt - now_dt).total_seconds(),
                    )

                    logger.info(
                        f"[LUT-STAGE] Waiting for 09:16:00 | "
                        f"{wait_seconds:.1f}s"
                    )

                    ok = await _sleep_with_cancel(wait_seconds)
                    if not ok:
                        return None

                    continue

                # Only evaluate at exact minute start.
                current_minute = now_dt.replace(
                    second=0,
                    microsecond=0,
                )

                if now_dt.second != 0:
                    next_minute = current_minute + timedelta(minutes=1)

                    next_dt = datetime.combine(
                        today,
                        next_minute.time(),
                        tzinfo=now_dt.tzinfo,
                    )

                    exit_dt_for_wait = datetime.combine(
                        today,
                        exit_time,
                        tzinfo=now_dt.tzinfo,
                    )

                    target_dt = min(
                        next_dt,
                        exit_dt_for_wait,
                    )

                    wait_seconds = max(
                        0.1,
                        (target_dt - now_dt).total_seconds(),
                    )

                    logger.info(
                        f"[LUT-STAGE] Waiting for exact minute boundary | "
                        f"next={target_dt.time()} | "
                        f"{wait_seconds:.1f}s"
                    )

                    ok = await _sleep_with_cancel(wait_seconds)
                    if not ok:
                        return None

                    continue

                selected_stage_lut = _get_current_lut_file(now_dt)

                logger.info(
                    f"[LUT-STAGE] EXACT MINUTE CHECK | "
                    f"TIME={now_dt.strftime('%H:%M:%S')} | "
                    f"LUT={selected_stage_lut}"
                )

            else:
                # ========================================================
                # NORMAL CONFIGURATION BUILD
                # ========================================================
                if now_dt < entry_datetime:
                    sleep_until = entry_datetime
                    sleep_duration = max(
                        0.0,
                        (sleep_until - now_dt).total_seconds(),
                    )

                    if sleep_duration > 0:
                        logger.info(
                            f"Waiting until entry time {entry_time} "
                            f"({sleep_duration:.1f}s)..."
                        )

                        ok = await _sleep_with_cancel(
                            sleep_duration
                        )

                        if not ok:
                            return None

                    continue

            # --- OLD: Minute-by-minute check loop (commented out) ---
            # if now_time > score_check_cutoff_time:
            #     logger.warning(
            #         f"Score-check cutoff time {score_check_cutoff_time.strftime('%H:%M:%S')} crossed. "
            #         "No more config score checks will be performed. Aborting build."
            #     )
            #     return None

            # if now_time < pre_entry_check_time:
            #     sleep_until = dt.datetime.combine(today, pre_entry_check_time, tzinfo=now_dt.tzinfo)
            #     sleep_duration = max(0.0, (sleep_until - now_dt).total_seconds())
            #     if sleep_duration > 0:
            #         logger.debug(f"Waiting until pre-entry check time {pre_entry_check_time}...")
            #         ok = await _sleep_with_cancel(sleep_duration)
            #         if not ok:
            #             return None
            #     continue

            # if filters_passed_at_least_once and now_time >= entry_time:
            #     logger.debug("=" * 100)
            #     logger.debug("✅ SCORE RULE PASSED & ENTRY TIME REACHED — BUILDING POSITION")
            #     logger.debug("=" * 100)
            #     build_triggered = True
            #     break

            # current_minute = now_dt.replace(second=0, microsecond=0)

            # if now_dt.second != 0:
            #     ok = await _sleep_until_next_minute_boundary(now_dt, today)
            #     if not ok:
            #         return None
            #     continue

            # if last_checked_minute == current_minute:
            #     ok = await _sleep_until_next_minute_boundary(now_dt, today)
            #     if not ok:
            #         return None
            #     continue
            # --- END OLD LOGIC ---

            logger.debug(f"🔍 CHECKING TRADE CONDITIONS @ {now_dt.time()}")
            # --- FIX: Force a fresh chain fetch to get the latest IV and prices for the check ---
            logger.debug(f"Config build: Forcing a fresh option chain fetch for {symbol_upper} to ensure latest data for LUT check.")
            now_time = now_dt.time()
            if now_time < entry_time:
                # removed local datetime import
                sleep_duration = max(1.0, (dt.datetime.combine(now_dt.date(), entry_time) - dt.datetime.now()).total_seconds())
                logger.info(f"⏳ Waiting {sleep_duration:.1f}s until config entry time {entry_time}...")
                ok = await _sleep_with_cancel(min(sleep_duration, 10.0))
                if not ok: return None
                continue
            
            chain_data = await get_option_chain_from_service(symbol_upper)

            logger.debug("=" * 120)
            logger.debug("[RAW CHAIN DATA RECEIVED BY BUILDER]")

            try:
                import json
                logger.debug(json.dumps(chain_data, indent=2, default=str)[:15000])
            except Exception as e:
                logger.debug(f"Unable to dump chain data : {e}")

            logger.debug("=" * 120)


            if chain_data:
                try:
                    # Publish the fresh chain so it becomes the new source of truth
                    state.publish_option_chain(symbol_upper, chain_data)
                except Exception as pub_e:
                    logger.warning(f"Could not publish freshly fetched chain for {symbol_upper}: {pub_e}")
            # --- END FIX ---

            if not chain_data:
                logger.error("Could not get option chain from cache or service at entry time. Aborting build.")
                # Abort if chain is not available at the critical moment of entry.
                return None

            chain_rows = chain_data.get("chain") or []

            logger.debug("=" * 120)
            logger.debug("[CHAIN DEBUG]")
            logger.debug(f"Chain keys      : {list(chain_data.keys())}")
            logger.debug(f"Chain rows      : {len(chain_rows)}")

            if chain_rows:
                logger.debug(f"First row keys  : {list(chain_rows[0].keys())}")

                logger.debug("FIRST ROW VALUES")
                for k, v in chain_rows[0].items():
                    logger.debug(f"{k:<30} = {v}")

            logger.debug("=" * 120)

            atm_value = chain_data.get("atm")

            logger.debug("=" * 120)
            logger.debug("[CHAIN ATM SEARCH]")

            logger.debug(f"Total Chain Rows : {len(chain_rows)}")
            logger.debug(f"ATM Value        : {atm_value}")

            for i, row in enumerate(chain_rows):
                if row.get("is_atm") or row.get("strike") == atm_value:
                    logger.debug(
                        f"Candidate #{i} | "
                        f"Strike={row.get('strike')} | "
                        f"is_atm={row.get('is_atm')} | "
                        f"CE_IV={row.get('ce_iv')} | "
                        f"PE_IV={row.get('pe_iv')} | "
                        f"CE_LTP={row.get('ce_ltp')} | "
                        f"PE_LTP={row.get('pe_ltp')}"
                    )

            logger.debug("=" * 120)

            atm_row = next(
                (row for row in chain_rows if bool(row.get("is_atm"))),
                None
            )

            
            logger.debug("=" * 120)
            logger.debug("[FULL ATM ROW]")
            logger.debug(f"ATM ROW TYPE : {type(atm_row)}")

            if atm_row is None:
                logger.debug("ATM ROW IS NONE")
            else:
                for k,v in atm_row.items():
                    logger.debug(f"{k:<35} = {v}")

            logger.debug("=" * 120)

            if not atm_row and atm_value is not None:

                atm_row = next(
                    (row for row in chain_rows if row.get("strike") == atm_value),
                    None
                )
            logger.debug("=" * 120)
            logger.debug("[FULL ATM ROW]")

            if atm_row is None:
                logger.debug("ATM ROW IS NONE")
            else:
                for k in sorted(atm_row.keys()):
                    logger.debug(f"{k:<35} = {atm_row[k]}")

            logger.debug("=" * 120)
            logger.debug("=" * 120)
            logger.debug("[ATM SELECTION]")
            logger.debug(f"ATM VALUE : {atm_value}")
            logger.debug(f"ATM FOUND : {atm_row is not None}")

            if atm_row:
                logger.debug(f"ATM STRIKE : {atm_row.get('strike')}")
                logger.debug(f"IS ATM     : {atm_row.get('is_atm')}")

            logger.debug("=" * 120)
            if not atm_row:
                logger.error("Could not find ATM row in option chain at entry time. Aborting build.")
                # ok = await _sleep_until_next_minute_boundary(now_dt, today)
                # if not ok:
                #     return None
                return None # Abort if ATM row not found

            logger.debug("=" * 80)
            logger.debug("[ATM ROW DEBUG]")

            logger.debug(f"ATM Strike : {atm_row.get('strike') if atm_row else None}")
            logger.debug(f"is_atm     : {atm_row.get('is_atm') if atm_row else None}")

            logger.debug(f"CE IV      : {atm_row.get('ce_iv') if atm_row else None}")
            logger.debug(f"PE IV      : {atm_row.get('pe_iv') if atm_row else None}")

            logger.debug(f"CE LTP     : {atm_row.get('ce_ltp') if atm_row else None}")
            logger.debug(f"PE LTP     : {atm_row.get('pe_ltp') if atm_row else None}")

            logger.debug(f"ATM VALUE  : {atm_value}")

            logger.debug("=" * 80)



            logger.debug("=" * 120)
            logger.debug("[ATM ROW BEFORE COMPUTATION]")

            if atm_row is None:
                logger.debug("ATM ROW IS NONE")
            else:
                for k, v in atm_row.items():
                    logger.debug(f"{k:<35} = {v}")

            logger.debug("=" * 120)

            logger.debug(f"CHAIN ATM VALUE : {atm_value}")
            logger.debug(f"SELECTED STRIKE : {atm_row.get('strike') if atm_row else None}")
            logger.debug(f"IS ATM FLAG     : {atm_row.get('is_atm') if atm_row else None}")

            raw_ce_iv = atm_row.get("ce_iv", 0)

            logger.debug(f"RAW CE TYPE : {type(raw_ce_iv)}")
            logger.debug(f"RAW CE VALUE: {repr(raw_ce_iv)}")

            ce_iv = _safe_float(raw_ce_iv, 0.0)

            logger.debug(f"SAFE CE IV  : {ce_iv}")

            raw_pe_iv = atm_row.get("pe_iv", 0)

            logger.debug(f"RAW PE TYPE : {type(raw_pe_iv)}")
            logger.debug(f"RAW PE VALUE: {repr(raw_pe_iv)}")

            pe_iv = _safe_float(raw_pe_iv, 0.0)

            logger.debug(f"SAFE PE IV  : {pe_iv}")

            logger.debug("=" * 120)
            logger.debug("[IV EXTRACTION]")
            logger.debug(f"RAW CE IV : {atm_row.get('ce_iv')}")
            logger.debug(f"RAW PE IV : {atm_row.get('pe_iv')}")
            logger.debug(f"SAFE CE IV: {ce_iv}")
            logger.debug(f"SAFE PE IV: {pe_iv}")
            logger.debug(f"ATM KEYS  : {list(atm_row.keys())}")
            logger.debug("=" * 120)

            logger.debug("=" * 80)
            logger.debug("[IV FIELD DEBUG]")

            possible_keys = [
                "ce_iv","pe_iv",
                "call_iv","put_iv",
                "CE_IV","PE_IV",
                "iv","IV",
                "callIV","putIV",
                "ceIV","peIV"
            ]

            for k in possible_keys:
                logger.debug(f"{k:<20} -> {atm_row.get(k)}")

            logger.debug("=" * 80)

            ce_ltp = _safe_float(atm_row.get("ce_ltp", 0), 0.0)
            pe_ltp = _safe_float(atm_row.get("pe_ltp", 0), 0.0)
            logger.debug("=" * 80)
            logger.debug("[ATM IV EXTRACTION]")

            logger.debug(f"ATM VALUE            : {atm_value}")
            logger.debug(f"ATM ROW FOUND        : {atm_row is not None}")
            logger.debug(f"CHAIN ATM            : {chain_data.get('atm')}")
            logger.debug(f"ROW STRIKE           : {atm_row.get('strike') if atm_row else None}")
            logger.debug(f"ROW IS_ATM           : {atm_row.get('is_atm') if atm_row else None}")

            if atm_row:
                logger.debug(f"ATM STRIKE           : {atm_row.get('strike')}")
                logger.debug(f"ATM CE IV RAW        : {atm_row.get('ce_iv')}")
                logger.debug(f"ATM PE IV RAW        : {atm_row.get('pe_iv')}")

            logger.debug("=" * 80)
            live_iv = (ce_iv + pe_iv) / 2.0 if (ce_iv > 0 or pe_iv > 0) else 0.0

            logger.debug("=" * 120)
            logger.debug("[POST LIVE-IV]")
            logger.debug(f"CE_IV={ce_iv}")
            logger.debug(f"PE_IV={pe_iv}")
            logger.debug(f"LIVE_IV={live_iv}")
            logger.debug("=" * 120)
            live_straddle = ce_ltp + pe_ltp
            live_iv = (ce_iv + pe_iv) / 2.0 if (ce_iv > 0 or pe_iv > 0) else 0.0

            logger.debug("=" * 80)
            logger.debug("[ATM IV DEBUG]")
            logger.debug(f"ATM STRIKE       : {atm_row.get('strike') if atm_row else None}")
            logger.debug(f"ATM CE IV RAW    : {atm_row.get('ce_iv') if atm_row else None}")
            logger.debug(f"ATM PE IV RAW    : {atm_row.get('pe_iv') if atm_row else None}")
            logger.debug(f"Computed CE IV   : {ce_iv}")
            logger.debug(f"Computed PE IV   : {pe_iv}")
            logger.debug(f"Computed LIVE IV : {live_iv}")
            logger.debug("=" * 80)

            logger.debug("=" * 120)
            logger.debug("[PAYLOAD TO _compute_score_payload]")
            logger.debug(f"live_iv        = {live_iv}")
            logger.debug(f"live_straddle  = {live_straddle}")
            logger.debug(f"ce_iv          = {ce_iv}")
            logger.debug(f"pe_iv          = {pe_iv}")
            logger.debug(f"ce_ltp         = {ce_ltp}")
            logger.debug(f"pe_ltp         = {pe_ltp}")
            logger.debug("=" * 120)

            score_data = _compute_score_payload(
                symbol=symbol_upper,
                live_iv=live_iv,
                live_straddle=live_straddle,
                chain_data=chain_data,
                manual_historical_idv=manual_historical_idv,
                manual_prev_day_straddle=manual_prev_day_straddle,
                tp_points=tp_points,
                manual_spot_price=manual_spot_price,
                use_live_spot_for_og=use_live_spot_for_og,
                straddle_price_drop_trigger=config.get("straddle_price_drop_trigger"),
                exit_at_straddle=config.get("exit_at_straddle"),
                straddle_price_drop_pct_sqf=config.get("straddle_price_drop_pct_sqf"), silent_logs=False,
            )

            sell_allowed = bool(score_data["sell_allowed"])
            current_minute = now_dt.replace(second=0, microsecond=0)
            last_checked_minute = current_minute

            decision = score_data.get("decision","NO")
            selected_lut = score_data.get("selected_lut")

            logger.debug("-" * 100)
            logger.debug(f"📊 LUT CHECK: {symbol_upper}")
            logger.debug(f"  Expiry               : {score_data['expiry_date']}")
            logger.debug(f"  Current DTE          : {score_data['current_dte']}")
            logger.debug(f"  Live IV (%)          : {score_data['live_iv']}")
            logger.debug(f"  Adjusted IV          : {score_data['adj_iv']}")
            logger.debug(f"  Live Straddle        : {score_data['live_straddle']}")
            logger.debug(f"  Adjusted IDV         : {score_data['adj_idv']}")
            logger.debug(f"  Prev Day Straddle    : {score_data['prev_day_straddle']} ({score_data.get('prev_day_straddle_source')})")
            logger.debug(f"  --- LUT Payload ---")
            for k, v in score_data.get("lut_payload", {}).items():
                logger.debug(f"    {k:<15}: {v}")
            logger.debug(f"  --- Decision ---")
            logger.debug(f"  Decision      : {decision}")
            logger.debug(f"  Selected LUT  : {score_data.get('selected_lut')}")
            logger.debug(f"  Requested Trade Size : {config.get('size')}")
            logger.debug(f"  Sell Allowed         : {sell_allowed}")
            logger.debug(f"  Manual Load Needed   : {score_data['manual_load_required']}")
            if score_data["manual_load_message"]:
                logger.debug(f"  Manual Load Message  : {score_data['manual_load_message']}")
            if score_data["warnings"]:
                logger.warning(f"  Warnings             : {' | '.join(score_data['warnings'])}")
            logger.debug("=" * 100)
            
            if sell_allowed:
                logger.info(
                    "✅ LUT check passed. Triggering build."
                )
                build_triggered = True
                break

            # ========================================================
            # FIVE-STAGE MODE: NO IS NOT TERMINAL.
            # Wait for the next exact minute and evaluate again.
            # ========================================================
            if _is_lut_stage_mode(config):

                current_minute = now_dt.replace(
                    second=0,
                    microsecond=0,
                )

                last_checked_minute = current_minute

                logger.warning(
                    f"❌ [LUT-STAGE] NO | "
                    f"TIME={now_dt.strftime('%H:%M:%S')} | "
                    f"LUT={score_data.get('selected_lut')} | "
                    "WAITING FOR NEXT STAGE"
                )

                next_minute = current_minute + timedelta(minutes=1)

                next_dt = datetime.combine(
                    today,
                    next_minute.time(),
                    tzinfo=now_dt.tzinfo,
                )

                exit_dt_for_wait = datetime.combine(
                    today,
                    exit_time,
                    tzinfo=now_dt.tzinfo,
                )

                target_dt = min(
                    next_dt,
                    exit_dt_for_wait,
                )

                wait_seconds = max(
                    0.1,
                    (target_dt - now_dt).total_seconds(),
                )

                logger.info(
                    f"[LUT-STAGE] Next check at "
                    f"{target_dt.time()} | "
                    f"sleep={wait_seconds:.1f}s"
                )

                ok = await _sleep_with_cancel(wait_seconds)
                if not ok:
                    return None

                continue

            # NORMAL CONFIGURATION MODE
            logger.warning(
                "❌ LUT check failed at configured entry time. "
                "Aborting normal config build."
            )
            return None

            # --- OLD: Minute-by-minute scheduling block (commented out) ---
            # logger.debug("### SCORE-BUILDER: ENTERING MINUTE-END SCHEDULING BLOCK ###")
            # next_minute_dt = current_minute + timedelta(minutes=1)
            # target_dt = next_minute_dt
            # if sell_allowed and now_time < entry_time:
            #     entry_dt = dt.datetime.combine(today, entry_time, tzinfo=now_dt.tzinfo)
            #     target_dt = min(target_dt, entry_dt)
            # cutoff_dt = dt.datetime.combine(today, score_check_cutoff_time, tzinfo=now_dt.tzinfo)
            # target_dt = min(target_dt, cutoff_dt)
            # exit_dt = dt.datetime.combine(today, exit_time, tzinfo=now_dt.tzinfo)
            # target_dt = min(target_dt, exit_dt)
            # sleep_interval = max(0.0, (target_dt - now_dt).total_seconds())
            # logger.debug(f"  Next score check scheduled in {sleep_interval:.1f}s (at {target_dt.time()}).")
            # ok = await _sleep_with_cancel(sleep_interval)
            # if not ok:
            #     return None
            # --- END OLD LOGIC ---

        if not build_triggered:
            logger.debug(
                f"Config build loop ended without triggering build for trade_uid={trade_uid}. "
                "Returning None without modifying persisted trade state."
            )
            return None

        if not trade_uid:
            logger.debug("No trade_uid provided to build_with_config, generating a new one.")
            timestamp = get_ist_now().strftime("%d%m%y%H%M%S")

            SYMBOL_PREFIXES = {
                "NIFTY": "ny",
                "SENSEX": "sx",
            }

            prefix = SYMBOL_PREFIXES.get(symbol_upper, symbol_upper[:2].lower())
            base_trade_uid = f"{prefix}{timestamp}"
            trade_uid = base_trade_uid
            suffix_counter = 0
            loop = asyncio.get_event_loop()

            while await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid):
                suffix_counter += 1
                suffix = chr(ord("a") + suffix_counter - 1)
                trade_uid = f"{base_trade_uid}{suffix}"

        config_for_builder = config.copy()
        config_for_builder.pop("order_lots_per_call", None)
        logger.debug("Using legacy chunking for config-based build (order_lots_per_call removed).")

        # Adjust trade size based on the LUT decision multiplier
        if decision != "YES":
            raise ValueError("LUT rejected trade")

        TEST_HALF_SIZE=True

        if TEST_HALF_SIZE:
            final_trade_size=max(1,config["size"]//2)
        else:
            final_trade_size=config["size"]
        logger.debug(f"Trade decision={decision} | Selected LUT={selected_lut} | Final trade size={final_trade_size}")

        result = await build_straddle(
            symbol=config["symbol"],
            lots=final_trade_size, # Use the adjusted size
            trade_uid=trade_uid,
            delta_neutral=True,
            trade_config=config_for_builder,
            ce_strike_price=config.get("ce_strike_price"),
            pe_strike_price=config.get("pe_strike_price")
        )

        if result and result.get("success"):
            logger.debug(f"✅ Position built: {trade_uid}")
            straddle_data = result.get("straddle_data", {}) or {}
            entry_spot = straddle_data.get("entry_spot", 0)

            if not straddle_data and result.get("pending_entry"):
                logger.debug(f"[{trade_uid}] Build deferred to pending entry monitor; no immediate straddle payload available.")
                return trade_uid

            sl_points = config["sl_bps"] * entry_spot / 10000 if entry_spot > 0 else 0
            logger.debug(f"  Entry Spot : {entry_spot:.2f}")
            logger.debug(f"  SL Points  : {sl_points:.2f} per straddle")
            state.db.update_straddle_config(trade_uid, config, sl_points)
            return trade_uid

        loop = asyncio.get_event_loop()
        current_trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
        current_status = current_trade.get("status", "UNKNOWN") if current_trade else "UNKNOWN"

        TERMINAL_STATUSES = {
            "CLOSED_SL_BUILD",
            "CLOSED_SQF",
            "CLOSED_SL",
            "CLOSED_ROLL",
            "PARTIAL",
            "CANCELLED",
        }

        if current_status in TERMINAL_STATUSES:
            if current_status == "PARTIAL":
                logger.error(
                    f"⚠️  [{trade_uid}] Build ended PARTIAL — orders were placed but "
                    f"order-book verification failed (network timeout). "
                    f"Preserving PARTIAL status. MANUAL REVIEW REQUIRED."
                )
                state.db.update_straddle_status(trade_uid, "PARTIAL")
                if not hasattr(state, "partial_build_uids"):
                    state.partial_build_uids = set()
                state.partial_build_uids.add(trade_uid)

            elif current_status == "CLOSED_SL_BUILD":
                logger.warning(
                    f"⚠️  [{trade_uid}] Build returned None but trade was already "
                    f"closed by in-build SL hit (status=CLOSED_SL_BUILD). "
                    "Preserving closed status — NOT marking as FAILED_FILTER."
                )
            else:
                logger.warning(
                    f"⚠️  [{trade_uid}] Build returned None but trade already has "
                    f"terminal status={current_status}. Preserving — NOT overwriting."
                )

            return None

        logger.error(
            f"❌ [{trade_uid}] Build failed completely. "
            f"No orders placed. Current status: {current_status}"
        )
        state.db.update_straddle_status(trade_uid, "FAILED_FILTER")
        return None

    except Exception as e:
        logger.error(f"Config build error: {e}", exc_info=True)
        return None
    
