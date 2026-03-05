"""
Option Chain Provider - CLIENT
This module now acts as a client to the dedicated Market Data Microservice.
It fetches data via HTTP requests.
"""
from typing import Dict, Optional, List
from utils.logger import logger
from models.state import state # Keep state for local caching
from . import data_client # Import the module to avoid circular dependency issues
import config

# Symbol configurations (only gap is hardcoded, rest is dynamic)
SYMBOL_CONFIG: Dict[str, Dict] = {
    'NIFTY': {
        'segment': 2,  # Add segment for NSEFO
        'gap': 50,
        'min_roll_threshold': 30,
        'max_order_qty': 1800,   # Max order size for NSE F&O
        'min_future_price': 10000
    },
    'SENSEX': {
        'segment': 12, # Add segment for BSEFO
        'gap': 100,
        'min_roll_threshold': 65,
        'max_order_qty': 5000,   # Max order size for BSE F&O (typically higher)
        'min_future_price': 50000
    }
}

async def get_ltp(token: int, exchange_segment: int = config.EXCHANGE_NSEFO, ignore_cache: bool = False) -> float:
    """Get LTP from the main application's synchronized price cache."""
    try:
        token = int(token)
        if not ignore_cache:
            cached = state.get_price(token)
            if cached is not None and cached > 0:
                return cached
        
        logger.debug(f"💰 LTP for {token} not found in local cache, fetching from microservice...")
        # Fetch from microservice if not in local cache
        return await data_client.get_ltp_from_service(token, exchange_segment)

    except Exception as e:
        logger.error(f"Get LTP error for token {token}: {e}")
        return 0.0

async def get_market_depth(token: int, exchange_segment: int = config.EXCHANGE_NSEFO) -> Optional[Dict]:
    """Fetches market depth for a given token from the Market Data Microservice."""
    return await data_client.get_market_depth_from_service(token)

async def get_bulk_market_depth(instruments: List[Dict]) -> Dict[int, Dict]:
    """Fetches market depth for multiple tokens from the Market Data Microservice."""
    return await data_client.get_bulk_market_depth_from_service(instruments)

async def get_bulk_ltp(tokens: List[int], exchange_segment: int) -> Dict[int, float]:
    """Fetches LTPs for multiple tokens from the Market Data Microservice."""
    # The exchange_segment is passed for future use but the service currently doesn't need it.
    return await data_client.get_bulk_ltp_from_service(tokens)

async def get_spot_details(symbol: str, target_expiry: str = None, use_cache_only: bool = False) -> Optional[Dict]:
    """
    Fetches spot details. It first tries to derive them from the local option chain cache.
    If that fails, it calls the Market Data Service as a fallback.
    """
    symbol_upper = symbol.upper()
    try:
        # 1. Try to get details from the local option chain cache first.
        cached_chain = state.get_option_chain(symbol_upper)
        if cached_chain and not state.is_option_chain_stale(symbol_upper, max_age=15):
            spot_details = {
                "fut_ltp": cached_chain.get("fut_ltp"), "atm": cached_chain.get("atm"),
                "lot_size": cached_chain.get("lot_size"), "expiry_date": cached_chain.get("expiry"),
                "dte": cached_chain.get("dte"), "exchange_segment": cached_chain.get("exchange_segment"),
                "base_symbol": cached_chain.get("symbol"), "fut_token": cached_chain.get("fut_token"),
                "gap": SYMBOL_CONFIG.get(cached_chain.get("symbol", ""), {}).get("gap"),
            }
            if all(spot_details.values()):
                logger.debug(f"Serving spot details for {symbol_upper} from local cache.")
                return spot_details

        # 2. If local cache is insufficient, fetch from the microservice.
        logger.debug(f"Local cache insufficient for spot details for {symbol_upper}. Fetching from service...")
        spot_data = await data_client.get_spot_details_from_service(symbol)
        if spot_data:
            # --- FIX: Validate spot data from service ---
            fut_ltp = spot_data.get('fut_ltp', 0)
            min_price = SYMBOL_CONFIG.get(symbol_upper, {}).get('min_future_price', 0)
            if min_price > 0 and fut_ltp < min_price:
                logger.warning(f"⚠️ Discarding invalid spot details for {symbol_upper}: LTP {fut_ltp} < {min_price}")
                return None
            # --- END FIX ---
            return spot_data

        # 3. As a last resort, if the service call fails but we have a stale chain, use it.
        if cached_chain:
            logger.warning(f"Falling back to stale cached spot details for {symbol_upper}.")
            return {
                "fut_ltp": cached_chain.get("fut_ltp"), "atm": cached_chain.get("atm"),
                "lot_size": cached_chain.get("lot_size"), "expiry_date": cached_chain.get("expiry"),
                "dte": cached_chain.get("dte"), "exchange_segment": cached_chain.get("exchange_segment"),
                "base_symbol": cached_chain.get("symbol"), "fut_token": cached_chain.get("fut_token"),
                "gap": SYMBOL_CONFIG.get(cached_chain.get("symbol", ""), {}).get("gap"),
            }
        return None
    except Exception as e:
        logger.error(f"❌ Error in async get_spot_details client: {e}", exc_info=True)
        return None

async def get_option_chain(symbol: str, strike_range: int = 5, target_expiry: str = None) -> Optional[Dict]:
    """
    Fetches the option chain. It prioritizes the local cache, which is kept warm
    by a background sync task. It only fetches directly from the microservice as a fallback.
    """
    symbol_upper = symbol.upper()
    try:
        # For rolls, we must force a fresh build from the service.
        if target_expiry:
            logger.info(f"Forcing fresh chain fetch for {symbol_upper} due to target_expiry: {target_expiry}")
            chain_data = await data_client.get_option_chain_from_service(symbol)
            if chain_data:
                state.update_option_chain(symbol_upper, chain_data)
            return chain_data

        # 1. Check local cache first.
        cached_chain = state.get_option_chain(symbol_upper)
        if cached_chain and not state.is_option_chain_stale(symbol_upper, max_age=15):
            logger.debug(f"Serving option chain for {symbol_upper} from local cache.")
            return cached_chain

        # 2. If cache is stale or empty, fetch from microservice.
        logger.debug(f"Local cache for {symbol_upper} is stale or empty. Fetching from Market Data Microservice...")
        chain_data = await data_client.get_option_chain_from_service(symbol)
        if chain_data:
            # --- FIX: Validate chain data from service ---
            fut_ltp = chain_data.get('fut_ltp', 0)
            min_price = SYMBOL_CONFIG.get(symbol_upper, {}).get('min_future_price', 0)
            if min_price > 0 and fut_ltp < min_price:
                logger.warning(f"⚠️ Discarding invalid option chain for {symbol_upper}: Spot {fut_ltp} < {min_price}")
                return None # Do not update cache
            # --- END FIX ---
            # Update the local state cache
            state.update_option_chain(symbol_upper, chain_data)
            logger.debug(f"Successfully updated local option chain cache for {symbol}.")

        # If fetch fails, return the stale cache data if it exists, otherwise None.
        return chain_data or cached_chain
    except Exception as e:
        logger.error(f"❌ Error in get_option_chain client: {e}", exc_info=True)
        return None