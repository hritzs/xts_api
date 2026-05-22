"""
Option Chain Provider - STATE-DRIVEN

This module now reads directly from the central `state` object, which is
populated by the `background.bridge` task connected to the C++ engine.
"""
from typing import Dict, Optional, List
from utils.logger import logger
from models.state import state  # Keep state for local caching
import config


# Central symbol configurations
SYMBOL_CONFIG: Dict[str, Dict] = {
    'NIFTY': {
        'segment': 2,            # NSE F&O
        'cash_index_segment': 1, # NSECM
        'cash_index_token': 26000,
        'gap': 50,
        'lot_size': 25,
        'series_fut': 'FUTIDX',
        'series_opt': 'OPTIDX',
        'min_roll_threshold': 30,
        'max_order_qty': 1755,
        'min_future_price': 10000
    },
    'BANKNIFTY': {
        'segment': 2,
        'cash_index_segment': 1,
        'cash_index_token': 26001,
        'gap': 100,
        'lot_size': 15,
        'series_fut': 'FUTIDX',
        'series_opt': 'OPTIDX',
        'min_roll_threshold': 60,
        'max_order_qty': 900,
        'min_future_price': 20000
    },
    'FINNIFTY': {
        'segment': 2,
        'cash_index_segment': 1,
        'cash_index_token': 26034,
        'gap': 50,
        'lot_size': 25,
        'series_fut': 'FUTIDX',
        'series_opt': 'OPTIDX',
        'min_roll_threshold': 30,
        'max_order_qty': 1800,
        'min_future_price': 10000
    },
    'MIDCPNIFTY': {
        'segment': 2,
        'cash_index_segment': 1,
        'cash_index_token': 26121,
        'gap': 25,
        'lot_size': 75,
        'series_fut': 'FUTIDX',
        'series_opt': 'OPTIDX',
        'min_roll_threshold': 20,
        'max_order_qty': 4200,
        'min_future_price': 5000
    },
    'SENSEX': {
        'segment': 12,            # BSE F&O
        'cash_index_segment': 11, # BSECM
        'cash_index_token': 26065,
        'gap': 100,
        'lot_size': 10,
        'series_fut': 'IF',
        'series_opt': 'IO',
        'min_roll_threshold': 65,
        'max_order_qty': 5000,
        'min_future_price': 50000
    },
    'BANKEX': {
        'segment': 12,
        'cash_index_segment': 11,
        'cash_index_token': 26118,
        'gap': 100,
        'lot_size': 15,
        'series_fut': 'IF',
        'series_opt': 'IO',
        'min_roll_threshold': 60,
        'max_order_qty': 4000,
        'min_future_price': 20000
    },
}


async def get_ltp(token: int, exchange_segment: int = config.EXCHANGE_NSEFO, ignore_cache: bool = False) -> float:
    """Get LTP from the main application's synchronized price cache."""
    try:
        token = int(token)
        if not ignore_cache:
            cached = state.get_price(token)
            if cached is not None and cached > 0:
                return cached

        logger.debug(f"💰 LTP for {token} not found in cache, returning 0.")
        return 0.0

    except Exception as e:
        logger.error(f"Get LTP error for token {token}: {e}")
        return 0.0


async def get_market_depth(token: int, exchange_segment: int = config.EXCHANGE_NSEFO) -> Optional[Dict]:
    """DEPRECATED: Market depth is now handled by the C++ engine."""
    logger.warning("get_market_depth is deprecated and will return None.")
    return None


async def get_bulk_market_depth(instruments: List[Dict]) -> Dict[int, Dict]:
    """DEPRECATED: Market depth is now handled by the C++ engine."""
    logger.warning("get_bulk_market_depth is deprecated and will return an empty dict.")
    return {}


async def get_bulk_ltp(tokens: List[int], exchange_segment: int) -> Dict[int, float]:
    """Fetches LTPs for multiple tokens directly from the state cache."""
    return {token: state.get_price(token) for token in tokens}


def _spot_details_from_chain(cached_chain: Dict) -> Dict:
    """
    Build a spot_details dict from a cached chain.
    Centralised here so both the fresh-cache path and the stale-fallback
    path return exactly the same keys — including cash_segment.
    """
    sym = cached_chain.get("symbol", "")
    sym_cfg = SYMBOL_CONFIG.get(sym, {})
    return {
        "fut_ltp":          cached_chain.get("fut_ltp"),
        "atm":              cached_chain.get("atm"),
        "lot_size":         cached_chain.get("lot_size"),
        "expiry_date":      cached_chain.get("expiry"),
        "dte":              cached_chain.get("dte"),
        "exchange_segment": cached_chain.get("exchange_segment"),
        "base_symbol":      sym,
        "fut_token":        cached_chain.get("fut_token"),
        # ✅ pass cash_segment through — needed by _build_new_chain on server
        "cash_segment":     cached_chain.get("cash_segment") or sym_cfg.get("cash_index_segment"),
        "gap":              sym_cfg.get("gap"),
    }


async def get_spot_details(symbol: str, target_expiry: str = None, use_cache_only: bool = False) -> Optional[Dict]:
    """
    Fetches spot details directly from the local option chain cache,
    which is populated by the C++ bridge.
    """
    symbol_upper = symbol.upper()
    try:
        cached_chain = state.get_option_chain(symbol_upper)
        if cached_chain:
            spot_details = _spot_details_from_chain(cached_chain)
            if all(v is not None for v in spot_details.values()):
                logger.debug(f"Serving spot details for {symbol_upper} from local cache.")
                return spot_details

        return None

    except Exception as e:
        logger.error(f"❌ Error in async get_spot_details client: {e}", exc_info=True)
        return None


async def get_option_chain(symbol: str, strike_range: int = 15, target_expiry: str = None) -> Optional[Dict]:
    """
    Fetches the option chain directly from the local cache, which is kept
    up-to-date by the C++ bridge.
    """
    symbol_upper = symbol.upper()
    try:
        cached_chain = state.get_option_chain(symbol_upper)
        if cached_chain:
            logger.debug(f"Serving option chain for {symbol_upper} from local cache.")
            return cached_chain
        
        logger.warning(f"Option chain for {symbol_upper} not yet available in state cache.")
        return None

    except Exception as e:
        logger.error(f"❌ Error in get_option_chain client: {e}", exc_info=True)
        return None
