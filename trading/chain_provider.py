"""
Option Chain Provider Service
Supports: NIFTY (NSE) and SENSEX (BSE)
This module acts as the core of the market data service.
"""
import time
import json
import threading
import math
from typing import Dict, Optional, List
from datetime import datetime
from utils.logger import logger
from utils.greeks import calculate_all_greeks, calculate_greeks_from_iv
import config
from models.state import state


# Global XTS instances (set by data_processor)
xt_m = None
md_socket = None

# Cached expiries (stable until next day or restart)
# cached_weekly_expiry = None
# cached_nearest_future_expiry = None
# cached_date = None

expiry_cache: Dict[str, Dict] = {}

# --- NEW: Add a short-term cache for spot details to prevent rapid, redundant API calls ---
_spot_details_cache: Dict[str, Dict] = {}
_spot_details_cooldown_seconds = 1.0 # Cooldown period in seconds

# --- FIX: Add a lock to prevent race conditions during chain builds ---
_chain_build_locks: Dict[str, threading.Lock] = {}

def set_xts_instances(market_api, socket_client):
    """Set global XTS instances"""
    global xt_m, md_socket
    xt_m = market_api
    md_socket = socket_client
    logger.info("✅ XTS instances set in marketdata.chain_provider module")


def get_xts_market_api():
    """Returns the global market data API instance."""
    return xt_m


def _call_with_retry(func, *args, **kwargs):
    """
    Wrapper to retry a synchronous XTS call that might fail due to network issues.
    This specifically targets the UnboundLocalError that occurs in Connect.py
    when a network request fails, making the code more resilient.
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (UnboundLocalError, Exception) as e:
            # Catch generic exceptions too, as network calls can fail in various ways
            logger.warning(f"⚠️ Network/API error in {func.__name__}: {e}. Retrying... (Attempt {attempt + 1}/{max_retries})")
            if attempt + 1 == max_retries:
                logger.error(f"❌ All {max_retries} retries failed for {func.__name__}.")
                # Return a consistent error structure instead of raising
                return {'type': 'error', 'description': f'All retries failed for {func.__name__}'}
            time.sleep(0.5 * (attempt + 1))  # backoff: 0.5s, 1s
    # This path should ideally not be reached
    return {'type': 'error', 'description': 'Retry logic failed unexpectedly'}


# Symbol configurations (only gap is hardcoded, rest is dynamic)
SYMBOL_CONFIG: Dict[str, Dict] = {
    'NIFTY': {
        'segment': 2,  # NSE F&O
        'gap': 50,
        'series_fut': 'FUTIDX',
        'series_opt': 'OPTIDX',
        'min_roll_threshold': 30,
        'max_order_qty': 1800,   # Max order size for NSE F&O
        'min_future_price': 10000
    },
    'SENSEX': {
        'segment': 12,  # BSE F&O
        'gap': 100,
        'series_fut': 'IF',
        'series_opt': 'IO',
        'min_roll_threshold': 65,
        'max_order_qty': 5000,   # Max order size for BSE F&O (typically higher)
        'min_future_price': 50000
    }
}


def get_ltp(token: int, exchange_segment: int = config.EXCHANGE_NSEFO, ignore_cache: bool = False) -> float:
    """Get LTP from cache or fetch via REST if not available."""
    try:
        if not xt_m or not token:
            return 0.0

        token = int(token)

        # 1. Check cache first
        if not ignore_cache:
            cached = state.get_price(token)
            if cached is not None and cached > 0:
                return cached

        # 2. If not in cache, fetch via REST API
        logger.info(f"💰 LTP for {token} not in cache, fetching via REST...")
        instruments = [{
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": token
        }]
        
        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=config.MESSAGE_CODE_LTP,
            publishFormat='JSON'
        )

        if response and response.get('type') == 'success':
            result = response.get('result', {})
            list_quotes = result.get('listQuotes', [])
            if list_quotes:
                # The response is a list of JSON strings
                quote_str = list_quotes[0]
                quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                ltp = float(quote.get('LastTradedPrice', 0.0))
                
                if ltp > 0:
                    logger.info(f"💰 Fetched LTP for {token}: ₹{ltp:.2f}")
                    return ltp

        logger.warning(f"⚠️  Failed to fetch LTP for {token} via REST. Response: {response}")
        
        # 3. If REST fails, try to subscribe and hope for a socket update (less reliable for immediate use)
        try:
            xt_m.send_subscription(instruments, config.MESSAGE_CODE_LTP)
        except Exception as sub_e:
            logger.warning(f"⚠️  LTP subscription warning for {token}: {sub_e}")

        return 0.0

    except Exception as e:
        logger.error(f"Get LTP error for token {token}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0.0

def get_market_depth(token: int, exchange_segment: int = config.EXCHANGE_NSEFO) -> Optional[Dict]:
    """Get L1 market depth (best bid/ask) via REST."""
    try:
        if not xt_m or not token:
            return None

        token = int(token)

        instruments = [{
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": token
        }]
        
        # Use message code 1501 for L1 depth
        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=1501, # 1501 for Bid/Ask
            publishFormat='JSON'
        )

        if response and response.get('type') == 'success':
            result = response.get('result', {})
            list_quotes = result.get('listQuotes', [])
            if list_quotes:
                quote_str = list_quotes[0]
                quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                
                bid_info = quote.get('BidInfo', {})
                ask_info = quote.get('AskInfo', {})

                bid_price = float(bid_info.get('Price', 0.0))
                ask_price = float(ask_info.get('Price', 0.0))
                
                if bid_price > 0 and ask_price > 0:
                    logger.info(f"💰 Fetched Depth for {token}: Bid={bid_price:.2f}, Ask={ask_price:.2f}")
                    return {'bid_price': bid_price, 'ask_price': ask_price}

        logger.warning(f"⚠️  Failed to fetch market depth for {token} via REST. Response: {response}")
        return None

    except Exception as e:
        logger.error(f"Get Market Depth error for token {token}: {e}", exc_info=True)
        return None

def get_bulk_market_depth(instruments: List[Dict]) -> Dict[int, Dict]:
    """Get L1 market depth (best bid/ask) for multiple instruments via a single REST call."""
    depth_map = {}
    try:
        if not xt_m or not instruments:
            return {}

        # Use message code 1501 for L1 depth
        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=1501, # 1501 for Bid/Ask
            publishFormat='JSON'
        )

        if response and response.get('type') == 'success':
            result = response.get('result', {})
            list_quotes = result.get('listQuotes', [])
            if list_quotes:
                for quote_str in list_quotes:
                    quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                    token = quote.get('ExchangeInstrumentID')
                    bid_info = quote.get('BidInfo', {})
                    ask_info = quote.get('AskInfo', {})
                    bid_price = float(bid_info.get('Price', 0.0))
                    ask_price = float(ask_info.get('Price', 0.0))
                    
                    if token and bid_price > 0 and ask_price > 0:
                        depth_map[token] = {'bid_price': bid_price, 'ask_price': ask_price}
        
        if len(depth_map) < len(instruments):
             logger.warning(f"⚠️  Bulk depth fetch: Got data for {len(depth_map)}/{len(instruments)} instruments.")
        
        return depth_map

    except Exception as e:
        logger.error(f"Get Bulk Market Depth error: {e}", exc_info=True)
        return {}

# Moved from utils.helpers to here for microservice context
from utils.helpers import calculate_dte, get_ist_now
def get_spot_details(symbol: str, target_expiry: str = None, use_cache_only: bool = False) -> Optional[Dict]:
    """
    Calculates the definitive spot price (synthetic future) and related details.
    This is the SINGLE SOURCE OF TRUTH for spot price calculations.
    """
    symbol_upper = symbol.upper() # Get symbol_upper at the start

    # --- NEW: Cooldown Cache Check ---
    # If not forcing a specific expiry, check if we have fresh data from the last few seconds.
    # This prevents sequential callers (like different filters) from each triggering expensive API calls.
    if not target_expiry and not use_cache_only:
        now = time.time()
        cached = _spot_details_cache.get(symbol_upper)
        if cached and (now - cached.get('timestamp', 0)) < _spot_details_cooldown_seconds:
            logger.info(f"✅ Returning cached spot details for {symbol_upper} due to cooldown.")
            return cached['data']
    # --- END NEW ---

    try:
        if not xt_m:
            logger.error("❌ XTS Market Data instance not initialized for spot details")
            return None

        # --- Refactored Symbol Config Lookup ---
        base_symbol = None
        # Match longer names first to handle NIFTY vs BANKNIFTY correctly
        for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True):
            if key in symbol_upper:
                base_symbol = key
                break

        if not base_symbol or base_symbol not in SYMBOL_CONFIG:
            logger.error(f"❌ Unsupported symbol: '{symbol}'. Not found in SYMBOL_CONFIG.")
            return None

        sym_config = SYMBOL_CONFIG[base_symbol]
        exchange_segment = sym_config['segment']
        gap = sym_config['gap']
        future_series = sym_config['series_fut']
        option_series = sym_config['series_opt']
        logger.debug(f"✅ Spot Details: {base_symbol}, Segment: {exchange_segment}, Gap: {gap}")

        # --- SYMBOL-SPECIFIC EXPIRY CACHING ---
        today = get_ist_now().date()
        symbol_cache = expiry_cache.get(base_symbol, {})
        
        # Check if the entire cache for this symbol is valid for today
        if symbol_cache.get('date') == today:
            cached_weekly_expiry = symbol_cache.get('weekly')
            cached_nearest_future_expiry = symbol_cache.get('future')
        else:
            # Invalidate cache if date is old
            symbol_cache = {}
            cached_weekly_expiry = None
            cached_nearest_future_expiry = None
            logger.info(f"📅 Invalidating expiry cache for {base_symbol} as date has changed.")
        
        # If in cache-only mode and the cache is invalid, abort immediately.
        if use_cache_only and not (cached_weekly_expiry and cached_nearest_future_expiry):
            logger.warning(f"⚠️ [CACHE-ONLY] Expiry data for {base_symbol} not in cache. Aborting spot details fetch.")
            return None

        # --- MODIFICATION: Use target_expiry if provided ---
        if target_expiry:
            expiry_date = target_expiry
            logger.info(f"📅 Using provided target expiry for chain build: {expiry_date}")
        elif cached_weekly_expiry:
            expiry_date = cached_weekly_expiry
            logger.debug(f"📅 Using cached weekly expiry for {base_symbol}: {expiry_date}")
        else:
            logger.info(f"📅 Weekly expiry cache is stale or empty for {base_symbol}. Fetching...")
            expiry_response = _call_with_retry(xt_m.get_expiry_date, exchangeSegment=exchange_segment, series=option_series, symbol=base_symbol)
            if not expiry_response or not isinstance(expiry_response, dict) or 'result' not in expiry_response or not expiry_response['result']:
                logger.error(f"❌ Could not get weekly expiry list for {base_symbol}. Response: {expiry_response}")
                return None
            
            parsed = sorted([datetime.strptime(d_str.split("T")[0], "%Y-%m-%d") for d_str in expiry_response['result']])
            now = get_ist_now()
            cached_weekly_expiry = next((dt.strftime("%d%b%Y") for dt in parsed if dt.date() >= now.date()), parsed[-1].strftime("%d%b%Y"))
            expiry_date = cached_weekly_expiry
            symbol_cache['weekly'] = expiry_date
            logger.info(f"📅 Nearest weekly expiry determined for {base_symbol}: {expiry_date}")
        
        # This block determines the nearest future expiry date string
        if cached_nearest_future_expiry:
            nearest_future_expiry_for_api = cached_nearest_future_expiry
            logger.debug(f"📅 Using cached future expiry for {base_symbol}: {nearest_future_expiry_for_api}")
        else:
            logger.info(f"📅 Future expiry cache is stale or empty for {base_symbol}. Fetching...")
            future_expiry_response = _call_with_retry(xt_m.get_expiry_date, exchangeSegment=exchange_segment, series=future_series, symbol=base_symbol)
            
            if not future_expiry_response or not isinstance(future_expiry_response, dict) or 'result' not in future_expiry_response or not future_expiry_response['result']:
                logger.error(f"❌ Could not get future expiry list for {base_symbol}. Response: {future_expiry_response}")
                return None
            else:
                try:
                    parsed_futures = sorted([datetime.strptime(d_str.split("T")[0], "%Y-%m-%d") for d_str in future_expiry_response['result'] if d_str])
                    if not parsed_futures:
                        raise ValueError("Parsed future expiries list is empty.")
                    
                    # --- FIX: Find the monthly future, not just the nearest one. ---
                    # Heuristic: The monthly future is usually the first one with an expiry >= 15 days away.
                    # This helps skip weekly futures.
                    now_date = get_ist_now().date()
                    monthly_future_dt = next((dt for dt in parsed_futures if (dt.date() - now_date).days >= 15), parsed_futures[0])
                    nearest_future_expiry_for_api = monthly_future_dt.strftime("%d%b%Y")
                    symbol_cache['future'] = nearest_future_expiry_for_api
                    logger.info(f"📅 Nearest future expiry determined for {base_symbol}: {nearest_future_expiry_for_api}")
                except (ValueError, IndexError) as e:
                    logger.error(f"❌ Could not parse or find nearest future expiry for {base_symbol}: {e}")
                    return None
        
        # Update cache for the symbol
        symbol_cache['date'] = today
        expiry_cache[base_symbol] = symbol_cache
        logger.info(f"📅 Expiries cached for {base_symbol}. Weekly: {symbol_cache.get('weekly')}, Future: {symbol_cache.get('future')}")

        # --- NEW LOGIC: Use the nearest future as the primary source for price and lot size ---
        if not nearest_future_expiry_for_api:
            logger.error(f"❌ Could not determine nearest future expiry for {base_symbol}. Aborting.")
            return None

        logger.info(f"🚚 Using nearest future ({nearest_future_expiry_for_api}) for spot price and lot size for {base_symbol}.")
        
        # Get future contract details (token and lot size)
        future_symbol_resp = _call_with_retry(xt_m.get_future_symbol, exchangeSegment=exchange_segment, series=future_series, symbol=base_symbol, expiryDate=nearest_future_expiry_for_api)
        
        if not future_symbol_resp or not future_symbol_resp.get("result"):
            logger.error(f"❌ Failed to get future symbol details for {base_symbol} with expiry '{nearest_future_expiry_for_api}'. Response: {future_symbol_resp}. Aborting.")
            return None

        future_details = future_symbol_resp["result"][0]
        fut_token = int(future_details.get("ExchangeInstrumentID", 0))
        lot_size = future_details.get("LotSize", 1)
        if lot_size <= 0: lot_size = 1 # safety

        if not fut_token:
            logger.error(f"❌ Future token not found in response for {base_symbol}. Aborting.")
            return None
        
        min_future_price = sym_config.get('min_future_price', 1000) # Add a default fallback
        # Get the future's LTP. This is our definitive underlying price.
        fut_ltp = get_ltp(fut_token, exchange_segment)
        
        # --- FIX: Retry with REST if cache returns bad data (Collision protection) ---
        if fut_ltp > 0 and fut_ltp < min_future_price:
             logger.warning(f"⚠️ Cached LTP {fut_ltp} for future {fut_token} seems invalid (Expected > {min_future_price}). Forcing REST fetch.")
             fut_ltp = get_ltp(fut_token, exchange_segment, ignore_cache=True)
        # --- END FIX ---

        # --- FIX: Add sanity check for future price to avoid using bad data ---
        if not fut_ltp or fut_ltp < min_future_price:
            logger.error(f"❌ Failed to get a VALID LTP for future token {fut_token} ({base_symbol}). Got {fut_ltp}, expected > {min_future_price}. Aborting.")
            return None
        # --- END FIX ---
        
        logger.info(f"✅ Future-based details for {base_symbol}: LTP=₹{fut_ltp:.2f}, Lot Size={lot_size}, Token={fut_token}")

        # Final ATM based on the definitive future price
        final_atm = int(round(fut_ltp / gap) * gap)

        # --- NEW: Calculate Synthetic Future Price using Put-Call Parity ---
        # The "spot" price for all Greek calculations will be derived from the
        # ATM options themselves, which is more accurate than using the raw future price.
        logger.info(f"Calculating synthetic future price for ATM strike {final_atm} using Put-Call Parity...")

        # Get tokens for the ATM strike using the determined option expiry
        ce_resp = _call_with_retry(xt_m.get_option_symbol, exchangeSegment=exchange_segment, series=option_series, symbol=base_symbol, expiryDate=expiry_date, optionType="CE", strikePrice=final_atm)
        pe_resp = _call_with_retry(xt_m.get_option_symbol, exchangeSegment=exchange_segment, series=option_series, symbol=base_symbol, expiryDate=expiry_date, optionType="PE", strikePrice=final_atm)

        atm_ce_token = ce_resp["result"][0]["ExchangeInstrumentID"] if ce_resp.get("result") else None
        atm_pe_token = pe_resp["result"][0]["ExchangeInstrumentID"] if pe_resp.get("result") else None

        synthetic_fut_ltp = fut_ltp  # Fallback to raw future LTP

        if atm_ce_token and atm_pe_token:
            atm_ce_ltp = get_ltp(atm_ce_token, exchange_segment)
            atm_pe_ltp = get_ltp(atm_pe_token, exchange_segment)
            
            if atm_ce_ltp > 0 and atm_pe_ltp > 0:
                # Synthetic Future Price (S) = Strike (K) + Call Price (C) - Put Price (P)
                calc_synthetic = final_atm + atm_ce_ltp - atm_pe_ltp
                
                # Sanity check: Synthetic spot shouldn't be wildly different from Future LTP (e.g. 5%)
                if calc_synthetic <= 0:
                    logger.warning(f"⚠️ Synthetic spot {calc_synthetic:.2f} is negative. Discarding and using Future.")
                    synthetic_fut_ltp = fut_ltp
                elif abs(calc_synthetic - fut_ltp) > (fut_ltp * 0.05):
                    logger.warning(f"⚠️ Synthetic spot {calc_synthetic:.2f} deviates > 5% from Future {fut_ltp:.2f}. Discarding and using Future.")
                    synthetic_fut_ltp = fut_ltp
                else:
                    synthetic_fut_ltp = calc_synthetic
                    logger.info(f"Synthetic Future Calc: {synthetic_fut_ltp:.2f} = {final_atm} (K) + {atm_ce_ltp:.2f} (C) - {atm_pe_ltp:.2f} (P)")
            else:
                logger.warning(f"⚠️ Synthetic calc failed for {base_symbol} at ATM {final_atm}. CE={atm_ce_ltp}, PE={atm_pe_ltp}. Using raw future.")
        
        # --- FIX: Re-calculate ATM based on the more accurate SYNTHETIC future price ---
        final_atm = int(round(synthetic_fut_ltp / gap) * gap)
        logger.info(f"📊 Final Synthetic FUT LTP: ₹{synthetic_fut_ltp:.2f}, Final ATM: {final_atm}, Lot Size: {lot_size}")
        
        dte = calculate_dte(expiry_date) # expiry_date is the option expiry

        result = {
            "fut_ltp": synthetic_fut_ltp, # This is now the synthetic price
            "atm": final_atm,
            "lot_size": lot_size,
            "expiry_date": expiry_date,
            "dte": dte,
            "exchange_segment": exchange_segment,
            "base_symbol": base_symbol,
            "option_series": option_series,
            "fut_token": fut_token, # This is now the actual future token
            "gap": gap
        }
        # --- NEW: Update the cooldown cache before returning ---
        _spot_details_cache[symbol_upper] = {'timestamp': time.time(), 'data': result}
        # --- END NEW ---

        return result

    except Exception as e:
        logger.error(f"❌ Error in get_spot_details: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def _calculate_row_greeks(strike, fut_ltp, dte, ce_ltp, pe_ltp, risk_free_rate=0.0):
    """Helper to calculate greeks for a strike row with ITM IV adjustment."""
    ce_greeks = calculate_all_greeks("call", strike, fut_ltp, dte, ce_ltp, risk_free_rate) if ce_ltp > 0 else {}
    pe_greeks = calculate_all_greeks("put", strike, fut_ltp, dte, pe_ltp, risk_free_rate) if pe_ltp > 0 else {}

    # ITM IV Logic: Use OTM IV for ITM options
    is_ce_itm = strike < fut_ltp
    is_pe_itm = strike > fut_ltp

    if is_ce_itm and pe_greeks.get('iv', 0) > 0:
        otm_iv = pe_greeks.get('iv')
        ce_greeks = calculate_greeks_from_iv("call", strike, fut_ltp, dte, otm_iv, risk_free_rate)
    elif is_pe_itm and ce_greeks.get('iv', 0) > 0:
        otm_iv = ce_greeks.get('iv')
        pe_greeks = calculate_greeks_from_iv("put", strike, fut_ltp, dte, otm_iv, risk_free_rate)
    
    return ce_greeks, pe_greeks


def _update_chain(cached_chain_data: Dict) -> Optional[Dict]:
    """Helper to update prices and greeks for an existing chain."""
    symbol_upper = cached_chain_data['symbol']
    logger.info(f"🔄 Efficiently updating existing option chain for {symbol_upper}...")

    try:
        # --- NEW, EFFICIENT WAY ---
        # All necessary static data is already in the cached chain. We only need to fetch live prices.
        fut_token = cached_chain_data.get('fut_token')
        gap = cached_chain_data.get('gap')
        exchange_segment = cached_chain_data.get('exchange_segment')
        expiry_date = cached_chain_data.get('expiry')
        option_series = cached_chain_data.get('option_series')
        base_symbol = cached_chain_data.get('symbol')

        if not all([fut_token, gap, exchange_segment, expiry_date, option_series, base_symbol]):
            logger.error(f"❌ Corrupt cached chain for {symbol_upper}. Missing critical data (fut_token, gap, series, etc). Returning stale data.")
            return cached_chain_data

        # --- OPTIMIZATION: Bulk fetch prices to warm up cache FIRST ---
        # This ensures that subsequent calls to get_ltp (for Future and ATM options) use fresh data.
        tokens_to_fetch = []
        if fut_token:
            tokens_to_fetch.append(fut_token)
            
        for row in cached_chain_data['chain']:
            if row.get('ce_token'): tokens_to_fetch.append(row['ce_token'])
            if row.get('pe_token'): tokens_to_fetch.append(row['pe_token'])
            
        if tokens_to_fetch:
            # Construct instruments list
            instruments = [{'exchangeSegment': exchange_segment, 'exchangeInstrumentID': t} for t in tokens_to_fetch]
            try:
                # Use get_quote with list
                response = _call_with_retry(
                    xt_m.get_quote,
                    Instruments=instruments,
                    xtsMessageCode=config.MESSAGE_CODE_LTP,
                    publishFormat='JSON'
                )
                if response and response.get('type') == 'success':
                    result = response.get('result', {})
                    list_quotes = result.get('listQuotes', [])
                    for quote_str in list_quotes:
                        quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                        token = quote.get('ExchangeInstrumentID')
                        ltp = float(quote.get('LastTradedPrice', 0.0))
                        if token and ltp > 0:
                            state.update_price(int(token), ltp)
            except Exception as e:
                logger.warning(f"Bulk price fetch failed in _update_chain: {e}")
        # --- END OPTIMIZATION ---

        # 1. Get raw future LTP. This is a cheap call (cache or REST).
        raw_fut_ltp = get_ltp(fut_token, exchange_segment)
        
        # --- FIX: Add sanity check for future price in update loop ---
        min_future_price = SYMBOL_CONFIG.get(base_symbol, {}).get('min_future_price', 1000)
        if not raw_fut_ltp or raw_fut_ltp < min_future_price:
            logger.warning(f"⚠️ Invalid/Missing LTP for future {fut_token} ({raw_fut_ltp}). Expected > {min_future_price}. Using stale LTP.")
            raw_fut_ltp = cached_chain_data.get('fut_ltp', 0)

        # --- FIX: Prefer old synthetic spot for provisional ATM if raw future spikes ---
        # If the Raw Future deviates significantly (> 0.2%) from the last known Synthetic Spot,
        # use the Synthetic Spot to determine the ATM. This prevents the ATM from jumping
        # wildly due to Future/Spot basis volatility or bad ticks.
        old_fut_ltp = cached_chain_data.get('fut_ltp', 0)
        if old_fut_ltp > 0 and raw_fut_ltp > 0 and abs(raw_fut_ltp - old_fut_ltp) > (old_fut_ltp * 0.002):
             base_price_for_atm = old_fut_ltp
        else:
             base_price_for_atm = raw_fut_ltp

        # 2. Calculate a provisional ATM using the stable base price
        provisional_atm = int(round(base_price_for_atm / gap) * gap)
        
        # 3. Find the ATM options from the existing chain data.
        atm_row_for_synthetic = next((row for row in cached_chain_data['chain'] if row['strike'] == provisional_atm), None)
        
        synthetic_fut_ltp = raw_fut_ltp  # Fallback to raw future LTP
        atm_ce_token, atm_pe_token = None, None

        if atm_row_for_synthetic:
            atm_ce_token = atm_row_for_synthetic.get('ce_token')
            atm_pe_token = atm_row_for_synthetic.get('pe_token')
        else:
            # --- NEW LOGIC: ATM has moved out of the cached range. Fetch new tokens. ---
            logger.warning(f"ATM moved to {provisional_atm}, which is outside the cached chain. Fetching new ATM option symbols.")
            
            ce_resp = _call_with_retry(xt_m.get_option_symbol, exchangeSegment=exchange_segment, series=option_series, symbol=base_symbol, expiryDate=expiry_date, optionType="CE", strikePrice=provisional_atm)
            pe_resp = _call_with_retry(xt_m.get_option_symbol, exchangeSegment=exchange_segment, series=option_series, symbol=base_symbol, expiryDate=expiry_date, optionType="PE", strikePrice=provisional_atm)
            
            atm_ce_token = ce_resp["result"][0]["ExchangeInstrumentID"] if ce_resp.get("result") else None
            atm_pe_token = pe_resp["result"][0]["ExchangeInstrumentID"] if pe_resp.get("result") else None
            
            if atm_ce_token and atm_pe_token:
                logger.info(f"Successfully fetched new ATM tokens for {provisional_atm}: CE={atm_ce_token}, PE={atm_pe_token}")
                # Subscribe to these new tokens so their prices are updated.
                instruments_to_subscribe = [
                    {'exchangeSegment': exchange_segment, 'exchangeInstrumentID': int(atm_ce_token)},
                    {'exchangeSegment': exchange_segment, 'exchangeInstrumentID': int(atm_pe_token)}
                ]
                try:
                    xt_m.send_subscription(instruments_to_subscribe, config.MESSAGE_CODE_LTP)
                    if state.socket_connected and md_socket:
                        md_socket.send_subscription(instruments_to_subscribe, config.MESSAGE_CODE_LTP)
                except Exception as e:
                    logger.warning(f"⚠️ Subscription warning for new ATM tokens: {e}")
                
                # --- FIX: Fetch prices immediately for new ATM tokens to allow synthetic calc ---
                # Without this, get_ltp below returns 0, synthetic calc fails, and we fallback to raw future.
                # We force a REST fetch by ignoring cache.
                get_ltp(int(atm_ce_token), exchange_segment, ignore_cache=True)
                get_ltp(int(atm_pe_token), exchange_segment, ignore_cache=True)

        # Now, with tokens resolved (either from cache or new fetch), calculate synthetic price.
        if atm_ce_token and atm_pe_token:
            atm_ce_ltp = get_ltp(atm_ce_token, exchange_segment)
            atm_pe_ltp = get_ltp(atm_pe_token, exchange_segment)
            if atm_ce_ltp > 0 and atm_pe_ltp > 0:
                # 4. Calculate the more accurate synthetic future price using Put-Call Parity.
                calc_synthetic = provisional_atm + atm_ce_ltp - atm_pe_ltp
                if calc_synthetic > 0:
                    synthetic_fut_ltp = calc_synthetic
                    logger.debug(f"Synthetic Future Calc (Update): {synthetic_fut_ltp:.2f} = {provisional_atm} (K) + {atm_ce_ltp:.2f} (C) - {atm_pe_ltp:.2f} (P)")
                else:
                    logger.warning(f"⚠️ Calculated synthetic spot is negative ({calc_synthetic:.2f}). Ignoring.")
            else:
                logger.debug(f"Synthetic calc skipped (missing prices) for {provisional_atm}. CE={atm_ce_ltp}, PE={atm_pe_ltp}")

        # --- FIX: Always try to recover using OLD ATM if synthetic calc failed at provisional ATM ---
        # This handles cases where the Future price implies a new ATM (e.g. 25000) for which we don't have data yet,
        # but we DO have data for the current/old ATM (e.g. 24900). Using the old ATM's synthetic price is
        # much more accurate than falling back to the raw Future price (which might have a large basis).
        old_atm = cached_chain_data.get('atm', 0)
        
        if synthetic_fut_ltp == raw_fut_ltp and old_atm and old_atm != provisional_atm:
            logger.debug(f"Synthetic calc failed at prov ATM {provisional_atm}. Trying old ATM {old_atm}...")
            
            recovered_synthetic = 0.0
            if old_atm:
                # Find old ATM row in the cached chain
                old_atm_row = next((row for row in cached_chain_data['chain'] if row['strike'] == old_atm), None)
                if old_atm_row:
                    ce_t = old_atm_row.get('ce_token')
                    pe_t = old_atm_row.get('pe_token')
                    if ce_t and pe_t:
                        ce_p = get_ltp(ce_t, exchange_segment)
                        pe_p = get_ltp(pe_t, exchange_segment)
                        if ce_p > 0 and pe_p > 0:
                            recovered_synthetic = old_atm + ce_p - pe_p
            
            if recovered_synthetic > 0:
                logger.info(f"✅ Recovered Synthetic Spot using old ATM {old_atm}: {recovered_synthetic:.2f} (vs Raw Fut: {raw_fut_ltp})")
                synthetic_fut_ltp = recovered_synthetic
            # If recovery fails, we stick with raw_fut_ltp (already set in synthetic_fut_ltp)

        # 5. This synthetic price is our definitive underlying for all greeks.
        fut_ltp = synthetic_fut_ltp
        
        # 6. Recalculate final ATM and DTE. These are cheap operations.
        atm = int(round(fut_ltp / gap) * gap)
        dte = calculate_dte(expiry_date)
        
        cached_chain_data.update({ "fut_ltp": fut_ltp, "dte": dte, "atm": atm, "timestamp": time.time() })

        # --- REVERT OPTIMIZATION: Calculate Greeks Inline for Consistency ---
        # Offloading Greeks to a background task caused latency. Calculating inline ensures
        # that every price update has corresponding, up-to-date Greeks.
        risk_free_rate = 0.0

        for row in cached_chain_data['chain']:
            strike = row['strike']
            ce_token = row.get('ce_token')
            pe_token = row.get('pe_token')
            
            ce_ltp = get_ltp(ce_token, exchange_segment) if ce_token else 0.0
            pe_ltp = get_ltp(pe_token, exchange_segment) if pe_token else 0.0
            
            # --- NEW: Add validation for option LTP ---
            # An option price greater than the underlying is almost certainly a data error.
            if ce_ltp > fut_ltp:
                # Retry fetching fresh bypassing cache
                ce_ltp = get_ltp(ce_token, exchange_segment, ignore_cache=True)
                if ce_ltp > fut_ltp:
                    logger.warning(f"⚠️ Invalid CE LTP {ce_ltp:.2f} > Spot {fut_ltp:.2f} for strike {strike} during update. Treating as 0.")
                    ce_ltp = 0.0
            
            if pe_ltp > strike:
                # Retry fetching fresh bypassing cache
                pe_ltp = get_ltp(pe_token, exchange_segment, ignore_cache=True)
                if pe_ltp > strike:
                    logger.warning(f"⚠️ Invalid PE LTP {pe_ltp:.2f} > Strike {strike} for strike {strike} during update. Treating as 0.")
                    pe_ltp = 0.0
            # --- END NEW ---

            # Calculate Greeks
            ce_greeks, pe_greeks = _calculate_row_greeks(strike, fut_ltp, dte, ce_ltp, pe_ltp, risk_free_rate)

            row.update({
                "ce_ltp": ce_ltp,
                "pe_ltp": pe_ltp,
                "ce_iv": round(ce_greeks.get("iv", 0) * 100, 2),
                "ce_delta": ce_greeks.get("delta", 0),
                "ce_gamma": ce_greeks.get("gamma", 0),
                "ce_vega": ce_greeks.get("vega", 0),
                "ce_theta": ce_greeks.get("theta", 0),
                "pe_iv": round(pe_greeks.get("iv", 0) * 100, 2),
                "pe_delta": pe_greeks.get("delta", 0),
                "pe_gamma": pe_greeks.get("gamma", 0),
                "pe_vega": pe_greeks.get("vega", 0),
                "pe_theta": pe_greeks.get("theta", 0),
                "is_atm": row['strike'] == atm
            })
        
        # Update state first
        state.update_option_chain(symbol_upper, cached_chain_data)
        
        # Broadcast the FULLY updated chain
        if hasattr(state, 'broadcast_queue'):
            # --- FIX: Mask fut_token in broadcast to prevent UI from overwriting Synthetic Spot ---
            # We send a copy with fut_token=None so the UI doesn't link the Spot display to the raw Future ticker.
            broadcast_data = cached_chain_data.copy()
            broadcast_data['fut_token'] = None
            
            broadcast_message = {'type': 'option_chain_update', 'symbol': symbol_upper, 'data': broadcast_data}
            state.broadcast_queue.put_nowait(broadcast_message)

        logger.debug(f"✅ Chain update (Prices + Greeks) successful for {symbol_upper}.")
        return cached_chain_data

    except Exception as e:
        logger.error(f"❌ Error during efficient chain update for {symbol_upper}: {e}", exc_info=True)
        # Return stale data on error to prevent crashes
        return cached_chain_data


def _build_new_chain(symbol: str, strike_range: int, target_expiry: Optional[str]) -> Optional[Dict]:
    """Helper to build a new option chain from scratch."""
    logger.info(f"📊 Building new option chain for {symbol}...")
    spot_details = get_spot_details(symbol, target_expiry=target_expiry)
    if not spot_details:
        logger.error(f"❌ Could not get spot details for {symbol}. Aborting chain build.")
        return None

    fut_ltp, atm, lot_size, expiry_date, dte, exchange_segment, base_symbol, option_series, fut_token, gap = (
        spot_details['fut_ltp'], spot_details['atm'], spot_details['lot_size'], spot_details['expiry_date'],
        spot_details['dte'], spot_details['exchange_segment'], spot_details['base_symbol'],
        spot_details['option_series'], spot_details['fut_token'], spot_details['gap']
    )

    strikes = [atm + i * gap for i in range(-strike_range, strike_range + 1)]
    logger.info(f"📝 Strikes: {strikes[0]} to {strikes[-1]} (Total: {len(strikes)})")

    chain = []
    instruments_to_subscribe = []

    # Add future for spot price updates
    instruments_to_subscribe.append({
        "exchangeSegment": exchange_segment,
        "exchangeInstrumentID": fut_token
    })

    for strike in strikes:
        ce_resp = _call_with_retry(xt_m.get_option_symbol, exchangeSegment=exchange_segment, series=option_series, symbol=base_symbol, expiryDate=expiry_date, optionType="CE", strikePrice=strike)
        pe_resp = _call_with_retry(xt_m.get_option_symbol, exchangeSegment=exchange_segment, series=option_series, symbol=base_symbol, expiryDate=expiry_date, optionType="PE", strikePrice=strike)

        ce_token = ce_resp["result"][0]["ExchangeInstrumentID"] if ce_resp.get("result") else None
        pe_token = pe_resp["result"][0]["ExchangeInstrumentID"] if pe_resp.get("result") else None

        ce_ltp = get_ltp(ce_token, exchange_segment) if ce_token else 0.0
        pe_ltp = get_ltp(pe_token, exchange_segment) if pe_token else 0.0

        # --- NEW: Add validation for option LTP ---
        # An option price greater than the underlying is almost certainly a data error.
        if ce_ltp > fut_ltp:
            ce_ltp = get_ltp(ce_token, exchange_segment, ignore_cache=True)
            if ce_ltp > fut_ltp:
                logger.warning(f"⚠️ Invalid CE LTP {ce_ltp:.2f} > Spot {fut_ltp:.2f} for strike {strike}. Treating as 0.")
                ce_ltp = 0.0
        if pe_ltp > strike:
            pe_ltp = get_ltp(pe_token, exchange_segment, ignore_cache=True)
            if pe_ltp > strike:
                logger.warning(f"⚠️ Invalid PE LTP {pe_ltp:.2f} > Strike {strike} for strike {strike}. Treating as 0.")
                pe_ltp = 0.0
        # --- END NEW ---

        # Use r=0.0 to align with hoadley.py default behavior for more consistent IV
        risk_free_rate = 0.0

        ce_greeks, pe_greeks = _calculate_row_greeks(strike, fut_ltp, dte, ce_ltp, pe_ltp, risk_free_rate)

        chain.append({
            "strike": strike,
            "ce_token": int(ce_token) if ce_token else None,
            "ce_symbol": f"{base_symbol}{expiry_date}CE{strike}",
            "ce_ltp": ce_ltp,
            "ce_lot_size": lot_size,
            "ce_iv": round(ce_greeks.get("iv", 0) * 100, 2),
            "ce_delta": ce_greeks.get("delta", 0),
            "ce_gamma": ce_greeks.get("gamma", 0),
            "ce_vega": ce_greeks.get("vega", 0),
            "ce_theta": ce_greeks.get("theta", 0),
            "pe_token": int(pe_token) if pe_token else None,
            "pe_symbol": f"{base_symbol}{expiry_date}PE{strike}",
            "pe_ltp": pe_ltp,
            "pe_lot_size": lot_size,
            "pe_iv": round(pe_greeks.get("iv", 0) * 100, 2),
            "pe_delta": pe_greeks.get("delta", 0),
            "pe_gamma": pe_greeks.get("gamma", 0),
            "pe_vega": pe_greeks.get("vega", 0),
            "pe_theta": pe_greeks.get("theta", 0),
            "is_atm": strike == atm
        })

    # Subscribe to all instruments in the chain
    for row in chain:
        if row['ce_token']: instruments_to_subscribe.append({'exchangeSegment': exchange_segment, 'exchangeInstrumentID': row['ce_token']})
        if row['pe_token']: instruments_to_subscribe.append({'exchangeSegment': exchange_segment, 'exchangeInstrumentID': row['pe_token']})

    if instruments_to_subscribe:
        unique_instruments = [dict(t) for t in {tuple(d.items()) for d in instruments_to_subscribe}]
        logger.info(f"📡 Subscribing to {len(unique_instruments)} instruments")
        try:
            xt_m.send_subscription(unique_instruments, config.MESSAGE_CODE_LTP)
            if state.socket_connected and md_socket:
                md_socket.send_subscription(unique_instruments, config.MESSAGE_CODE_LTP)
        except Exception as e:
            logger.warning(f"⚠️  Subscription warning: {e}")

    logger.info(f"✅ Option chain built successfully: {len(chain)} strikes")
    chain_result = {
        "symbol": base_symbol,
        "expiry": expiry_date,
        "dte": dte,
        "atm": atm,
        "fut_ltp": fut_ltp,
        "fut_token": fut_token,
        "lot_size": lot_size,
        "exchange_segment": exchange_segment,
        "chain": chain,
        "timestamp": time.time(), # Add timestamp
        "gap": gap,
        "option_series": option_series
    }

    # NEW: Broadcast the newly built chain via the queue
    # The caller (get_option_chain) will handle updating the state.
    if hasattr(state, 'broadcast_queue'):
        # --- FIX: Mask fut_token in broadcast ---
        broadcast_data = chain_result.copy()
        broadcast_data['fut_token'] = None
        
        broadcast_message = {'type': 'option_chain_update', 'symbol': base_symbol, 'data': broadcast_data}
        state.broadcast_queue.put_nowait(broadcast_message)


    logger.info(f"✅ Option chain built for {base_symbol}")

    return chain_result


def get_option_chain(symbol: str, strike_range: int = 5, target_expiry: str = None) -> Optional[Dict]:
    """
    Build or update option chain with Greeks calculations.
    This function is the single source of truth for chain creation and updates.
    It updates the local state and queues the result for broadcast to backend clients.
    - If a valid chain is in the cache, it triggers an efficient update.
    - Otherwise, it triggers a full build from scratch.
    """
    symbol_upper = symbol.upper()
    
    # Get or create a lock for this specific symbol, ensuring thread safety.
    if symbol_upper not in _chain_build_locks:
        _chain_build_locks[symbol_upper] = threading.Lock()

    lock = _chain_build_locks[symbol_upper]

    # This lock ensures that only one thread can build or update the chain for a given symbol at a time.
    with lock:
        try:
            # Re-check cache inside the lock in case another thread just finished building.
            cached_chain_data = state.get_option_chain(symbol_upper)
            is_cache_valid = False

            if not target_expiry and cached_chain_data and 'expiry' in cached_chain_data:
                try:
                    expiry_dt = datetime.strptime(cached_chain_data['expiry'], "%d%b%Y".upper()).date()
                    if expiry_dt >= get_ist_now().date():
                        is_cache_valid = True
                except (ValueError, TypeError):
                    is_cache_valid = False

            if is_cache_valid:
                # If cache is valid, just update the prices and greeks.
                # The _update_chain helper now handles state update and broadcasting.
                return _update_chain(cached_chain_data)
            else:
                # If cache is invalid or doesn't exist, build a new chain from scratch.
                new_chain = _build_new_chain(symbol, strike_range, target_expiry)
                if new_chain:
                    state.update_option_chain(symbol_upper, new_chain) # Update state
                return new_chain

        except Exception as e:
            logger.error(f"❌ Option chain build/update error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None