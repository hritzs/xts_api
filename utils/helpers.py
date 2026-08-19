"""
Helper Utility Functions
"""
from datetime import datetime, time
from typing import List, Dict,Any
import config
from dateutil import parser as date_parser
from models.state import state
from utils.logger import logger



# Trading hours constants
MARKET_OPEN = time(0, 0)
MARKET_CLOSE = time(23, 59)
EXPIRY_TIME = time(23, 59)
TRADING_MINUTES_PER_DAY = 385  # 6h 15m


def get_ist_now() -> datetime:
    """Get current time in IST"""
    return datetime.now(config.IST)


def get_ist_date_str() -> str:
    """Get current date string (YYYY-MM-DD)"""
    return get_ist_now().strftime('%Y-%m-%d')

def _safe_float(value, default: float = 0.0) -> float:
    """Safely convert a value to a float, returning a default on failure."""
    try:
        if value is None:
            return default
        if isinstance(value, str):
            # Handle potential commas, percentage signs, or whitespace
            value = value.replace(",", "").replace("%", "").strip()
            if not value:
                return default
        return float(value)
    except (ValueError, TypeError):
        return default

def get_synthetic_reference_spot(chain_data: Dict[str, Any]) -> float:
    if not isinstance(chain_data, dict):
        return 0.0

    try:
        synthetic_spot = float(chain_data.get("synthetic_spot") or 0.0)
        if synthetic_spot > 0:
            return synthetic_spot
    except Exception:
        pass

    return 0.0

def get_strike_gap(symbol: str) -> int:
    """Get strike gap for symbol"""
    for key, gap in config.STRIKE_GAPS.items():
        if key in symbol:
            return gap
    return 100  # Default


def get_weekly_expiry(expiry_dates: List[str]) -> str:
    """Get nearest weekly expiry from list"""
    parsed = []
    for d_str in expiry_dates:
        try:
            if "T" in d_str:
                dt = datetime.strptime(d_str, "%Y-%m-%dT%H:%M:%S").replace(tzinfo=config.IST)
            else:
                dt = datetime.strptime(d_str, "%d%b%Y").replace(tzinfo=config.IST)
            parsed.append(dt)
        except ValueError:
            continue
    
    parsed.sort()
    now = get_ist_now()
    
    for dt in parsed:
        if dt.date() >= now.date():
            return dt.strftime("%d%b%Y")
    
    return parsed[-1].strftime("%d%b%Y") if parsed else ""


def calculate_dte(expiry_date_str: str) -> float:
    """
    Calculate DTE: calendar days + normalized trading hours for today.

    - Treats 9:15 AM to 8:30 PM as 1 complete day (675 trading minutes).
    - Example: Jan 28 9:42 AM → Feb 3 8:30 PM
      - Calendar days: 6 days
      - Today's progress: (9:42 - 9:15) / 675 = 27 / 675 = 0.04
      - Remaining today: 1 - 0.04 = 0.96
      - Total DTE: 6 + 0.96 = 6.96 days
    
    Args:
        expiry_date_str: Expiry date like "03Feb2026"
        
    Returns:
        DTE in normalized days
    """
    try:
        # Parse expiry date
        parsed_dt = date_parser.parse(expiry_date_str).replace(tzinfo=None)
        expiry_date = parsed_dt.date()
        
        # Get current time
        now = get_ist_now().replace(tzinfo=None)
        current_date = now.date()
        current_time = now.time()
        
        # Calendar days difference
        days_diff = (expiry_date - current_date).days
        
        # Already expired
        if days_diff < 0:
            return 0.0001
        
        # Expiry is today
        if days_diff == 0:
            if current_time >= MARKET_CLOSE:
                return 0.0001
            elif current_time < MARKET_OPEN:
                return 1.0
            else:
                current_min = current_time.hour * 60 + current_time.minute
                close_min = MARKET_CLOSE.hour * 60 + MARKET_CLOSE.minute
                minutes_remaining = close_min - current_min
                return max(minutes_remaining / TRADING_MINUTES_PER_DAY, 0.0001)
        
        # Future expiry - calculate today's remaining fraction
        if current_time >= MARKET_CLOSE:
            # Market closed - no time remaining today
            today_fraction = 0.0
        elif current_time < MARKET_OPEN:
            # Before market - full day remaining
            today_fraction = 1.0
        else:
            # During market hours
            current_min = current_time.hour * 60 + current_time.minute
            open_min = MARKET_OPEN.hour * 60 + MARKET_OPEN.minute
            close_min = MARKET_CLOSE.hour * 60 + MARKET_CLOSE.minute
            
            minutes_elapsed = current_min - open_min
            today_fraction = 1.0 - (minutes_elapsed / TRADING_MINUTES_PER_DAY)
        
        # Total DTE = calendar days + today's remaining fraction
        total_dte = days_diff + today_fraction
        
        return max(total_dte, 0.0001)
        
    except Exception as e:
        print(f"❌ DTE calculation error: {e} for {expiry_date_str}")
        return 1.0


async def get_correct_lot_size(straddle_data: Dict) -> int:
    """Get correct lot_size from option chain"""
    try:
        symbol = straddle_data.get('symbol', 'NIFTY')
        
        # Get option chain from state (cached)
        option_chain = state.option_chains.get(symbol.upper())
        
        if option_chain:
            lot_size = option_chain.get('lot_size', 65)
            logger.debug(f"✅ Lot size from cached option chain: {lot_size}")
            return lot_size
        
        # Fallback: Get from straddle data
        lot_size = straddle_data.get('lot_size', 65)
        logger.warning(f"⚠️  Using lot_size from database: {lot_size}")
        
        return lot_size
        
    except Exception as e:
        logger.error(f"❌ Error getting lot_size: {e}")
        return 65  # Default for NIFTY
