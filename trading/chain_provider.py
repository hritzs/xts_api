"""
Option Chain Provider Service
Supports: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY (NSE) and SENSEX, BANKEX (BSE)
This module acts as the core of the market data service.
"""
from itertools import chain
import time
import json
import threading
import math
from typing import Dict, Optional, List
from datetime import datetime
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from utils.logger import logger
from utils.greeks import calculate_all_greeks, calculate_greeks_from_iv
import config
from copy import deepcopy
from datetime import datetime
from typing import Any
from models.state import state
from zoneinfo import ZoneInfo
from utils.helpers import calculate_dte, get_ist_now
IST = ZoneInfo("Asia/Kolkata")
from copy import deepcopy
# Global XTS instances (set by data_processor)
from utils.helpers import get_ist_now
xt_m = None
md_socket = None

expiry_cache: Dict[str, Dict] = {}

_spot_details_cache: Dict[str, Dict] = {}
_spot_details_cooldown_seconds = 1.0

_chain_build_locks: Dict[str, threading.Lock] = {}

# Key: (symbol, expiry_date, strike, option_type) -> Value: token (int)
_STATIC_TOKEN_CACHE: Dict[tuple, int] = {}


def set_xts_instances(market_api, socket_client):
    """Set global XTS instances"""
    global xt_m, md_socket
    xt_m = market_api
    md_socket = socket_client
    logger.info("✅ XTS instances set in trading.chain_provider module")


def get_xts_market_api():
    """Returns the global market data API instance."""
    return xt_m


def _call_with_retry(func, *args, **kwargs):
    """
    Wrapper to retry a synchronous XTS call that might fail due to network issues.
    """
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except (UnboundLocalError, Exception) as e:
            logger.warning(f"⚠️ Network/API error in {func.__name__}: {e}. Retrying... (Attempt {attempt + 1}/{max_retries})")
            if attempt + 1 == max_retries:
                logger.error(f"❌ All {max_retries} retries failed for {func.__name__}.")
                return {'type': 'error', 'description': f'All retries failed for {func.__name__}'}
            time.sleep(0.1 * (attempt + 1))
    return {'type': 'error', 'description': 'Retry logic failed unexpectedly'}


# Symbol configurations
# SYMBOL_CONFIG: Dict[str, Dict] = {
#     'NIFTY': {
#         'segment': 2,
#         'cash_index_segment': 1,
#         'cash_index_token': 26000,
#         'gap': 50,
#         'series_fut': 'FUTIDX',
#         'series_opt': 'OPTIDX',
#         'min_roll_threshold': 30,
#         'max_order_qty': 1800,
#         'min_future_price': 10000
#     },
#     'BANKNIFTY': {
#         'segment': 2,
#         'cash_index_segment': 1,
#         'cash_index_token': 26001,
#         'gap': 100,
#         'series_fut': 'FUTIDX',
#         'series_opt': 'OPTIDX',
#         'min_roll_threshold': 60,
#         'max_order_qty': 900,
#         'min_future_price': 20000
#     },
#     'FINNIFTY': {
#         'segment': 2,
#         'cash_index_segment': 1,
#         'cash_index_token': 26034,
#         'gap': 50,
#         'series_fut': 'FUTIDX',
#         'series_opt': 'OPTIDX',
#         'min_roll_threshold': 30,
#         'max_order_qty': 1800,
#         'min_future_price': 10000
#     },
#     'MIDCPNIFTY': {
#         'segment': 2,
#         'cash_index_segment': 1,
#         'cash_index_token': 26121,
#         'gap': 25,
#         'series_fut': 'FUTIDX',
#         'series_opt': 'OPTIDX',
#         'min_roll_threshold': 20,
#         'max_order_qty': 4200,
#         'min_future_price': 5000
#     },
#     'SENSEX': {
#         'segment': 12,
#         'cash_index_segment': 11,
#         'cash_index_token': 26065,
#         'gap': 100,
#         'series_fut': 'IF',
#         'series_opt': 'IO',
#         'min_roll_threshold': 65,
#         'max_order_qty': 1000,
#         'min_future_price': 50000
#     },
#     'BANKEX': {
#         'segment': 12,
#         'cash_index_segment': 11,
#         'cash_index_token': 26118,
#         'gap': 100,
#         'series_fut': 'IF',
#         'series_opt': 'IO',
#         'min_roll_threshold': 60,
#         'max_order_qty': 4000,
#         'min_future_price': 20000
#     }
# }
SYMBOL_CONFIG: Dict[str, Dict] = {
    'NIFTY': {
        'segment': 2,
        'cash_index_segment': 1,
        'cash_index_token': 26000,
        'gap': 50,
        'series_fut': 'FUTIDX',
        'series_opt': 'OPTIDX',
        'min_roll_threshold': 30,
        'max_order_qty': 1800,
        'min_future_price': 10000
    },
    'SENSEX': {
        'segment': 12,
        'cash_index_segment': 11,
        'cash_index_token': 26065,
        'gap': 100,
        'series_fut': 'IF',
        'series_opt': 'IO',
        'min_roll_threshold': 65,
        'max_order_qty': 1000,
        'min_future_price': 50000
    }
}


def _extract_ltp_from_quote(quote: Dict) -> float:
    try:
        return float(
            quote.get("LastTradedPrice")
            or quote.get("Touchline", {}).get("LastTradedPrice")
            or quote.get("Close")
            or 0.0
        )
    except Exception:
        return 0.0


def _extract_depth_from_quote(quote: dict) -> Dict:
    touchline = quote.get("Touchline", {}) or {}
    bids_list = quote.get("Bids", []) or []
    asks_list = quote.get("Asks", []) or []
    bid_info = touchline.get("BidInfo", {}) or {}
    ask_info = touchline.get("AskInfo", {}) or {}

    ltp = (
        touchline.get("LastTradedPrice")
        or quote.get("LastTradedPrice")
        or touchline.get("Close")
        or quote.get("Close")
        or 0.0
    )
    ltp = _safe_float(ltp, 0.0)

    top_bid = bids_list[0] if bids_list and isinstance(bids_list, list) else {}
    top_ask = asks_list[0] if asks_list and isinstance(asks_list, list) else {}

    bid = _safe_float(
        top_bid.get("Price")
        or bid_info.get("Price")
        or touchline.get("BidPrice")
        or 0.0,
        0.0,
    )
    bid_qty = _safe_int(
        top_bid.get("Size")
        or bid_info.get("Size")
        or touchline.get("BidSize")
        or 0,
        0,
    )

    ask = _safe_float(
        top_ask.get("Price")
        or ask_info.get("Price")
        or touchline.get("AskPrice")
        or 0.0,
        0.0,
    )
    ask_qty = _safe_int(
        top_ask.get("Size")
        or ask_info.get("Size")
        or touchline.get("AskSize")
        or 0,
        0,
    )

    depth_available = (bid > 0 and ask > 0)

    return {
        "ltp": ltp,
        "last_price": ltp,
        "bid": bid if bid > 0 else None,
        "ask": ask if ask > 0 else None,
        "bid_price": bid if bid > 0 else None,
        "ask_price": ask if ask > 0 else None,
        "bid_qty": bid_qty if bid_qty > 0 else None,
        "ask_qty": ask_qty if ask_qty > 0 else None,
        "depth_available": depth_available,
        "raw_quote": quote,
    }
 
def get_ltp(token: int, exchange_segment: int = config.EXCHANGE_NSEFO, ignore_cache: bool = False) -> float:
    try:
        if not xt_m or not token:
            return 0.0

        token = int(token)

        if not ignore_cache:
            cached = state.get_price(token)
            if cached is not None and cached > 0:
                return cached

        logger.info(f"💰 LTP for {token} not in cache, fetching via REST...")

        instruments = [{
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": token
        }]

        is_cash_segment = exchange_segment in [1, 11]
        primary_code = 1501 if is_cash_segment else 1502

        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=primary_code,
            publishFormat="JSON"
        )

        list_quotes = []
        if response and response.get("type") == "success":
            list_quotes = response.get("result", {}).get("listQuotes", []) or []

        if not list_quotes and not is_cash_segment:
            logger.warning(f"⚠️ 1502 returned no data for token {token}. Retrying with 1501...")
            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=instruments,
                xtsMessageCode=1501,
                publishFormat="JSON"
            )
            if response and response.get("type") == "success":
                list_quotes = response.get("result", {}).get("listQuotes", []) or []

        if list_quotes:
            quote_str = list_quotes[0]
            quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
            parsed = _extract_depth_from_quote(quote)
            ltp = float(parsed.get("ltp") or 0.0)

            if ltp > 0:
                state.update_price(token, ltp)
                logger.info(
                    f"💰 Fetched quote for {token}: "
                    f"LTP={ltp:.2f}, Bid={parsed.get('bid')}, Ask={parsed.get('ask')}"
                )
                return ltp

        if is_cash_segment:
            logger.warning(f"⚠️ get_quote failed for cash index {token}. Trying get_ohlc fallback...")
            try:
                now = get_ist_now()
                start_time = (now - timedelta(minutes=5)).strftime('%b %d %Y %H%M%S')
                end_time = now.strftime('%b %d %Y %H%M%S')
                segment_name = "NSECM" if exchange_segment == 1 else "BSECM"

                ohlc_resp = _call_with_retry(
                    xt_m.get_ohlc,
                    exchangeSegment=segment_name,
                    exchangeInstrumentID=token,
                    startTime=start_time,
                    endTime=end_time,
                    compressionValue=1
                )

                if ohlc_resp and ohlc_resp.get("type") == "success":
                    ohlc_data_str = ohlc_resp.get("result", {}).get("dataPoints", "")
                    if ohlc_data_str:
                        last_candle = ohlc_data_str.strip().split("\n")[-1]
                        parts = last_candle.split("|")
                        if len(parts) >= 5:
                            ltp = float(parts[4])
                            if ltp > 0:
                                state.update_price(token, ltp)
                                return ltp
            except Exception as ohlc_e:
                logger.error(f"❌ OHLC fallback failed for {token}: {ohlc_e}")

        logger.warning(f"⚠️ Failed to fetch LTP for {token} via REST. Response: {response}")

        try:
            sub_code = 1501 if is_cash_segment else 1502
            xt_m.send_subscription(instruments, sub_code)
            state.add_subscription(token)
        except Exception as sub_e:
            logger.warning(f"⚠️ LTP subscription warning for {token}: {sub_e}")

        return 0.0

    except Exception as e:
        logger.error(f"Get LTP error for token {token}: {e}", exc_info=True)
        return 0.0

def get_market_depth(token: int, exchange_segment: int = config.EXCHANGE_NSEFO) -> Optional[Dict]:
    try:
        if not xt_m or not token:
            return None

        token = int(token)
        instruments = [{
            "exchangeSegment": exchange_segment,
            "exchangeInstrumentID": token
        }]

        msg_code = 1501 if exchange_segment in [1, 11] else 1502

        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=msg_code,
            publishFormat="JSON"
        )

        if response and response.get("type") == "success":
            list_quotes = response.get("result", {}).get("listQuotes", []) or []
            if list_quotes:
                quote_str = list_quotes[0]
                quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                parsed = _extract_depth_from_quote(quote)

                if parsed["ltp"] > 0 or parsed["depth_available"]:
                    logger.info(
                        f"✅ Fetched quote/depth for {token} using msg_code={msg_code}: "
                        f"LTP={parsed.get('ltp')}, Bid={parsed.get('bid')}, Ask={parsed.get('ask')}"
                    )
                    return parsed

        logger.warning(
            f"Failed to fetch market depth for {token} via REST using msg_code={msg_code}. Response: {response}"
        )
        return None

    except Exception as e:
        logger.error(f"Get Market Depth error for token {token}: {e}", exc_info=True)
        return None

def get_bulk_market_depth(instruments: List[Dict]) -> Dict[int, Dict]:
    depth_map = {}
    try:
        if not xt_m or not instruments:
            return {}

        instruments_by_segment: Dict[int, List[Dict]] = {}
        for instr in instruments:
            segment = instr.get("exchangeSegment")
            instruments_by_segment.setdefault(segment, []).append(instr)

        max_batch = 50

        for segment, segment_instruments in instruments_by_segment.items():
            message_code = 1501 if segment in [1, 11] else 1502

            for start in range(0, len(segment_instruments), max_batch):
                batch = segment_instruments[start:start + max_batch]

                logger.info(
                    f"🚚 Fetching bulk depth for {len(batch)} instruments "
                    f"in segment {segment} using code {message_code} "
                    f"(batch {start // max_batch + 1})..."
                )

                response = _call_with_retry(
                    xt_m.get_quote,
                    Instruments=batch,
                    xtsMessageCode=message_code,
                    publishFormat="JSON"
                )

                if response and response.get("type") == "success":
                    list_quotes = response.get("result", {}).get("listQuotes", []) or []

                    for quote_str in list_quotes:
                        quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                        token = quote.get("ExchangeInstrumentID")
                        if not token:
                            continue

                        token = int(token)
                        parsed = _extract_depth_from_quote(quote)

                        if parsed["ltp"] > 0 or parsed["depth_available"]:
                            depth_map[token] = parsed
                            if parsed["ltp"] > 0:
                                state.update_price(token, parsed["ltp"])
                else:
                    logger.warning(
                        f"[BULK-DEPTH] non-success response segment={segment} "
                        f"code={message_code} raw={str(response)[:1500]}"
                    )

        if len(depth_map) < len(instruments):
            logger.warning(f"Bulk depth fetch: Got data for {len(depth_map)}/{len(instruments)} instruments.")

        return depth_map

    except Exception as e:
        logger.error(f"Get Bulk Market Depth error: {e}", exc_info=True)
        return {}
    
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

def get_bulk_ltp(tokens: List[int], exchange_segment: int = config.EXCHANGE_NSEFO) -> Dict[int, float]:
    ltp_map = {}
    try:
        if not xt_m or not tokens:
            return {}

        instruments = [{"exchangeSegment": exchange_segment, "exchangeInstrumentID": t} for t in tokens]
        message_code = 1501 if exchange_segment in [1, 11] else 1502

        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=message_code,
            publishFormat="JSON"
        )

        if response and response.get("type") == "success":
            list_quotes = response.get("result", {}).get("listQuotes", []) or []

            for quote_str in list_quotes:
                quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                token = quote.get("ExchangeInstrumentID")
                parsed = _extract_depth_from_quote(quote)
                ltp = float(parsed.get("ltp") or 0.0)

                if token and ltp > 0:
                    token = int(token)
                    ltp_map[token] = ltp
                    state.update_price(token, ltp)

        if len(ltp_map) < len(instruments):
            logger.warning(f"⚠️ Bulk LTP fetch: Got data for {len(ltp_map)}/{len(instruments)} instruments.")

        return ltp_map

    except Exception as e:
        logger.error(f"Get Bulk LTP error: {e}", exc_info=True)
        return {}

def _stamp_now() -> str:
    return datetime.now(IST).isoformat()


def _calculate_row_greeks(strike, fut_ltp, dte, ce_ltp, pe_ltp, risk_free_rate=0.0):
    """Helper to calculate greeks for a strike row with ITM IV adjustment."""
    ce_greeks = calculate_all_greeks("call", strike, fut_ltp, dte, ce_ltp, risk_free_rate) if ce_ltp > 0 else {}
    pe_greeks = calculate_all_greeks("put",  strike, fut_ltp, dte, pe_ltp, risk_free_rate) if pe_ltp > 0 else {}

    is_ce_itm = strike < fut_ltp
    is_pe_itm = strike > fut_ltp

    if is_ce_itm and pe_greeks.get('iv', 0) > 0:
        otm_iv = pe_greeks.get('iv')
        ce_greeks = calculate_greeks_from_iv("call", strike, fut_ltp, dte, otm_iv, risk_free_rate)
    elif is_pe_itm and ce_greeks.get('iv', 0) > 0:
        otm_iv = ce_greeks.get('iv')
        pe_greeks = calculate_greeks_from_iv("put", strike, fut_ltp, dte, otm_iv, risk_free_rate)

    return ce_greeks, pe_greeks
 
def get_spot_details(symbol: str, target_expiry: str = None, use_cache_only: bool = False) -> Optional[Dict]:
    """
    Calculates the definitive spot price (cash index) and related details.
    REST is the source of truth for spot price on every call.
    Only expiry metadata may be reused from cache.
    """
    symbol_upper = symbol.upper()

    try:
        if not xt_m:
            logger.error("❌ XTS Market Data instance not initialized for spot details")
            return None

        base_symbol = None
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
        option_series = sym_config['series_opt']

        cash_token = sym_config.get('cash_index_token')
        cash_segment = sym_config.get('cash_index_segment')
        if not cash_token or not cash_segment:
            logger.error(f"❌ Cash index token/segment not configured for '{symbol}'. Aborting.")
            return None

        logger.debug(f"✅ Spot Details: {base_symbol}, Segment: {exchange_segment}, Gap: {gap}")

        today = get_ist_now().date()
        symbol_cache = expiry_cache.get(base_symbol, {})

        if symbol_cache.get('date') == today:
            cached_weekly_expiry = symbol_cache.get('weekly')
        else:
            symbol_cache = {}
            cached_weekly_expiry = None
            logger.info(f"📅 Invalidating expiry cache for {base_symbol} as date has changed.")

        if use_cache_only and not cached_weekly_expiry:
            logger.warning(f"⚠️ [CACHE-ONLY] Expiry data for {base_symbol} not in cache. Aborting spot details fetch.")
            return None

        if target_expiry:
            expiry_date = target_expiry
            logger.info(f"📅 Using provided target expiry for chain build: {expiry_date}")
        elif cached_weekly_expiry:
            expiry_date = cached_weekly_expiry
            logger.debug(f"📅 Using cached weekly expiry for {base_symbol}: {expiry_date}")
        else:
            logger.info(f"📅 Weekly expiry cache is stale or empty for {base_symbol}. Fetching...")
            expiry_response = _call_with_retry(
                xt_m.get_expiry_date,
                exchangeSegment=exchange_segment,
                series=option_series,
                symbol=base_symbol
            )
            if not expiry_response or not isinstance(expiry_response, dict) or \
               'result' not in expiry_response or not expiry_response['result']:
                logger.error(f"❌ Could not get weekly expiry list for {base_symbol}. Response: {expiry_response}")
                return None

            parsed = sorted(
                [datetime.strptime(d_str.split("T")[0], "%Y-%m-%d") for d_str in expiry_response['result']]
            )
            now = get_ist_now()
            cached_weekly_expiry = next(
                (dt.strftime("%d%b%Y") for dt in parsed if dt.date() >= now.date()),
                parsed[-1].strftime("%d%b%Y")
            )
            expiry_date = cached_weekly_expiry
            symbol_cache['weekly'] = expiry_date
            logger.info(f"📅 Nearest weekly expiry determined for {base_symbol}: {expiry_date}")

        symbol_cache['date'] = today
        expiry_cache[base_symbol] = symbol_cache
        logger.info(f"📅 Expiries cached for {base_symbol}. Weekly: {symbol_cache.get('weekly')}")

        logger.info(f"🚚 Using cash index (Token: {cash_token}, Segment: {cash_segment}) for spot price for {base_symbol}.")

        # REST ONLY: always force fresh spot fetch, never trust old cached spot here
        fut_ltp = get_ltp(cash_token, cash_segment, ignore_cache=True)

        min_price = sym_config.get('min_future_price', 1000)
        if not fut_ltp or fut_ltp < min_price:
            logger.error(
                f"❌ Failed to get a VALID LIVE LTP for cash index token {cash_token} "
                f"({base_symbol}). Got {fut_ltp}, expected > {min_price}. Aborting."
            )
            return None

        provisional_atm = int(round(fut_ltp / gap) * gap)

        option_symbol_resp = _call_with_retry(
            xt_m.get_option_symbol,
            exchangeSegment=exchange_segment,
            series=option_series,
            symbol=base_symbol,
            expiryDate=expiry_date,
            optionType='CE',
            strikePrice=provisional_atm
        )

        if not option_symbol_resp or not option_symbol_resp.get("result"):
            logger.error(f"❌ Failed to get option symbol details for {base_symbol} to determine lot size. Aborting.")
            return None

        option_details = option_symbol_resp["result"][0]
        lot_size = option_details.get("LotSize", 1)
        if lot_size <= 0:
            lot_size = 1

        final_atm = int(round(fut_ltp / gap) * gap)
        dte = calculate_dte(expiry_date)

        result = {
            "fut_ltp": fut_ltp,
            "atm": final_atm,
            "lot_size": lot_size,
            "expiry_date": expiry_date,
            "dte": dte,
            "exchange_segment": exchange_segment,
            "base_symbol": base_symbol,
            "option_series": option_series,
            "fut_token": cash_token,
            "cash_segment": cash_segment,
            "gap": gap,
            "timestamp": time.time(),
        }

        return result

    except Exception as e:
        logger.error(f"❌ Error in get_spot_details: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

def _get_underlying_for_greeks(chain_data: Dict) -> float:
    synthetic_spot = float(chain_data.get("synthetic_spot") or 0.0)
    return synthetic_spot if synthetic_spot > 0 else 0.0
 
def _fetch_token_for_strike(exchange_segment, series, symbol, expiry_date, option_type, strike):
    """Helper to fetch token for a single strike/type in parallel."""
    cache_key = (symbol, expiry_date, strike, option_type)
    if cache_key in _STATIC_TOKEN_CACHE:
        return strike, option_type, _STATIC_TOKEN_CACHE[cache_key]

    try:
        resp = _call_with_retry(
            xt_m.get_option_symbol,
            exchangeSegment=exchange_segment,
            series=series,
            symbol=symbol,
            expiryDate=expiry_date,
            optionType=option_type,
            strikePrice=strike
        )
        token = resp["result"][0]["ExchangeInstrumentID"] if resp and resp.get("result") else None

        if token:
            _STATIC_TOKEN_CACHE[cache_key] = int(token)

        return strike, option_type, token
    except Exception as e:
        logger.error(f"Error fetching token for {symbol} {strike} {option_type}: {e}")
        return strike, option_type, None

def _normalise_iv(iv_value: float) -> float:
    """
    Normalise IV into percentage form once.

    Expected behavior:
    - None / invalid / <= 0 -> 0.0
    - 0.25 -> 25.0
    - 25.0 -> 25.0
    """
    try:
        if iv_value is None:
            return 0.0

        iv = float(iv_value)
        if iv <= 0:
            return 0.0

        # If IV is fractional (e.g. 0.25), convert to percentage.
        if iv <= 1.0:
            return round(iv * 100.0, 2)

        # Already in percentage form.
        return round(iv, 2)

    except Exception:
        return 0.0

def _compute_shared_iv_and_greeks(
    strike,
    synthetic_spot,
    dte,
    ce_ltp,
    pe_ltp,
    prev_ce=None,
    prev_pe=None,
):
    prev_ce = prev_ce or {}
    prev_pe = prev_pe or {}

    prev_ce_iv = float(prev_ce.get("iv", 0.0) or 0.0)
    prev_pe_iv = float(prev_pe.get("iv", 0.0) or 0.0)

    shared_iv_pct = 0.0
    iv_source = "NONE"
    raw_iv = 0.0

    try:
        if strike >= synthetic_spot:
            if ce_ltp > 0:
                ce_iv_greeks = calculate_all_greeks(
                    "call", strike, synthetic_spot, dte, ce_ltp, 0.0
                )
                raw_iv = ce_iv_greeks.get("iv", 0.0)
                if raw_iv > 0:
                    shared_iv_pct = _normalise_iv(raw_iv)
                    iv_source = "CE_OTM"
        else:
            if pe_ltp > 0:
                pe_iv_greeks = calculate_all_greeks(
                    "put", strike, synthetic_spot, dte, pe_ltp, 0.0
                )
                raw_iv = pe_iv_greeks.get("iv", 0.0)
                if raw_iv > 0:
                    shared_iv_pct = _normalise_iv(raw_iv)
                    iv_source = "PE_OTM"
    except Exception as e:
        logger.debug(f"shared iv primary failed strike={strike}: {e}")

    if shared_iv_pct <= 0 and ce_ltp > 0:
        try:
            ce_iv_greeks = calculate_all_greeks(
                "call", strike, synthetic_spot, dte, ce_ltp, 0.0
            )
            raw_iv = ce_iv_greeks.get("iv", 0.0)
            if raw_iv > 0:
                shared_iv_pct = _normalise_iv(raw_iv)
                iv_source = "CE_FALLBACK"
        except Exception as e:
            logger.debug(f"shared iv CE fallback failed strike={strike}: {e}")

    if shared_iv_pct <= 0 and pe_ltp > 0:
        try:
            pe_iv_greeks = calculate_all_greeks(
                "put", strike, synthetic_spot, dte, pe_ltp, 0.0
            )
            raw_iv = pe_iv_greeks.get("iv", 0.0)
            if raw_iv > 0:
                shared_iv_pct = _normalise_iv(raw_iv)
                iv_source = "PE_FALLBACK"
        except Exception as e:
            logger.debug(f"shared iv PE fallback failed strike={strike}: {e}")

    if shared_iv_pct > 0:
        iv_dec = shared_iv_pct / 100.0
        try:
            ce_g = calculate_greeks_from_iv(
                "call", strike, synthetic_spot, dte, iv_dec, 0.0
            )
            pe_g = calculate_greeks_from_iv(
                "put", strike, synthetic_spot, dte, iv_dec, 0.0
            )
        except Exception as e:
            logger.debug(f"shared greek from iv failed strike={strike}: {e}")
            ce_g = {}
            pe_g = {}

        return {
            "ce_iv": shared_iv_pct,
            "pe_iv": shared_iv_pct,
            "ce_delta": ce_g.get("delta", prev_ce.get("delta", 0.0)),
            "ce_gamma": ce_g.get("gamma", prev_ce.get("gamma", 0.0)),
            "ce_theta": ce_g.get("theta", prev_ce.get("theta", 0.0)),
            "ce_vega": ce_g.get("vega", prev_ce.get("vega", 0.0)),
            "pe_delta": pe_g.get("delta", prev_pe.get("delta", 0.0)),
            "pe_gamma": pe_g.get("gamma", prev_pe.get("gamma", 0.0)),
            "pe_theta": pe_g.get("theta", prev_pe.get("theta", 0.0)),
            "pe_vega": pe_g.get("vega", prev_pe.get("vega", 0.0)),
            "_iv_source": iv_source,
        }

    return {
        "ce_iv": prev_ce_iv,
        "pe_iv": prev_pe_iv,
        "ce_delta": prev_ce.get("delta", 0.0),
        "ce_gamma": prev_ce.get("gamma", 0.0),
        "ce_theta": prev_ce.get("theta", 0.0),
        "ce_vega": prev_ce.get("vega", 0.0),
        "pe_delta": prev_pe.get("delta", 0.0),
        "pe_gamma": prev_pe.get("gamma", 0.0),
        "pe_theta": prev_pe.get("theta", 0.0),
        "pe_vega": prev_pe.get("vega", 0.0),
        "_iv_source": "PREV",
    }

def _compute_synthetic_spot_from_atm(chain: dict) -> float:
    rows = chain.get("chain", [])
    atm = chain.get("atm")
    gap = float(chain.get("gap") or 0.0)

    try:
        if atm and gap > 0:
            atm_row = next((r for r in rows if r.get("strike") == atm), None)
            if atm_row:
                ce_p = float(atm_row.get("ce_ltp") or 0.0)
                pe_p = float(atm_row.get("pe_ltp") or 0.0)
                if ce_p > 0 and pe_p > 0:
                    syn = float(atm) + ce_p - pe_p
                    if abs(syn - float(atm)) <= (gap * 2):
                        return syn
    except Exception as e:
        logger.warning(f"synthetic spot calc failed: {e}")

    return 0.0
def _recompute_iv_with_synthetic(chain_data: dict) -> dict:
    """
    Recompute per-leg IV and Greeks using synthetic_spot as underlying,
    but keep the ITM/OTM adjustment logic from _calculate_row_greeks.
    """
    synthetic_spot = float(chain_data.get("synthetic_spot") or 0.0)
    dte = float(chain_data.get("dte") or 0.0)
    risk_free_rate = 0.0

    if synthetic_spot <= 0 or dte <= 0:
        # Nothing to safely recompute
        return chain_data

    for row in chain_data.get("chain", []):
        strike = float(row.get("strike") or 0.0)
        ce_ltp = float(row.get("ce_ltp") or 0.0)
        pe_ltp = float(row.get("pe_ltp") or 0.0)

        ce_greeks, pe_greeks = _calculate_row_greeks(
            strike=strike,
            fut_ltp=synthetic_spot,  # use synthetic spot as underlying
            dte=dte,
            ce_ltp=ce_ltp,
            pe_ltp=pe_ltp,
            risk_free_rate=risk_free_rate,
        )

        row["ce_iv"] = round(ce_greeks.get("iv", 0.0) * 100, 2)
        row["pe_iv"] = round(pe_greeks.get("iv", 0.0) * 100, 2)
        row["ce_delta"] = ce_greeks.get("delta", 0.0)
        row["ce_gamma"] = ce_greeks.get("gamma", 0.0)
        row["ce_theta"] = ce_greeks.get("theta", 0.0)
        row["ce_vega"] = ce_greeks.get("vega", 0.0)
        row["pe_delta"] = pe_greeks.get("delta", 0.0)
        row["pe_gamma"] = pe_greeks.get("gamma", 0.0)
        row["pe_theta"] = pe_greeks.get("theta", 0.0)
        row["pe_vega"] = pe_greeks.get("vega", 0.0)

    return chain_data

def _safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _get_message_code_for_depth(segment: int) -> int:
    return 1501 if segment in (1, 11, 12) else 1502

def _recompute_chain_rows(chain: dict) -> dict:
    """
    Refresh IV/Greeks for all rows using synthetic_spot as underlying,
    with the same ITM adjustment model as _calculate_row_greeks.
    Also recomputes synthetic_spot from ATM row when possible.
    """
    chain = deepcopy(chain)

    rows = chain.get("chain", [])
    dte = float(chain.get("dte") or 0.0)
    gap = int(chain.get("gap") or 50)

    # First, compute synthetic spot from the current ATM row.
    synthetic_spot = _compute_synthetic_spot_from_atm(chain)
    chain["synthetic_spot"] = synthetic_spot
    chain["syn_ltp"] = synthetic_spot
    chain["spot_ltp"] = synthetic_spot
    # Update ATM based on synthetic spot
    if synthetic_spot > 0 and gap > 0:
        chain["atm"] = int(round(synthetic_spot / gap) * gap)

    risk_free_rate = 0.0

    for row in rows:
        strike = float(row.get("strike") or 0.0)
        ce_ltp = float(row.get("ce_ltp") or 0.0)
        pe_ltp = float(row.get("pe_ltp") or 0.0)

        ce_greeks, pe_greeks = _calculate_row_greeks(
            strike=strike,
            fut_ltp=synthetic_spot,  # use synthetic spot as underlying
            dte=dte,
            ce_ltp=ce_ltp,
            pe_ltp=pe_ltp,
            risk_free_rate=risk_free_rate,
        )

        row.update({
            "ce_iv": round(ce_greeks.get("iv", 0.0) * 100, 2),
            "pe_iv": round(pe_greeks.get("iv", 0.0) * 100, 2),
            "ce_delta": ce_greeks.get("delta", 0.0),
            "ce_gamma": ce_greeks.get("gamma", 0.0),
            "ce_theta": ce_greeks.get("theta", 0.0),
            "ce_vega": ce_greeks.get("vega", 0.0),
            "pe_delta": pe_greeks.get("delta", 0.0),
            "pe_gamma": pe_greeks.get("gamma", 0.0),
            "pe_theta": pe_greeks.get("theta", 0.0),
            "pe_vega": pe_greeks.get("vega", 0.0),
            "is_atm": int(row.get("strike") or 0) == int(chain.get("atm") or 0),
        })

        if chain.get("symbol") == "NIFTY" and int(strike) == 24150:
            logger.info(
                f"[CHAIN-SST] NIFTY 24150 | ce_iv={row.get('ce_iv')} pe_iv={row.get('pe_iv')} "
                f"ce_ltp={ce_ltp} pe_ltp={pe_ltp} syn={synthetic_spot}"
            )

    return chain
 
def _finalize_chain_snapshot(chain: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize a chain dict into the canonical published snapshot shape.

    This object is the single source of truth for:
    - API
    - ZMQ get_option_chain
    - websocket option_chain_update
    - frontend option_chain.js
    """
    if not isinstance(chain, dict):
        return {}

    snapshot = deepcopy(chain)

    symbol = str(snapshot.get("symbol", "")).upper().strip()
    snapshot["symbol"] = symbol

    if "published_at" not in snapshot or not snapshot.get("published_at"):
        snapshot["published_at"] = get_ist_now().isoformat()

    if "timestamp" not in snapshot or not snapshot.get("timestamp"):
        snapshot["timestamp"] = time.time()

    rows = snapshot.get("chain") or []
    normalized_rows = []

    atm_value = snapshot.get("atm")

    for row in rows:
        if not isinstance(row, dict):
            continue

        strike = row.get("strike")

        normalized_row = {
            "strike": strike,

            "ce_token": row.get("ce_token"),
            "ce_symbol": row.get("ce_symbol"),
            "ce_ltp": row.get("ce_ltp", 0.0),
            "ce_lot_size": row.get("ce_lot_size"),
            "ce_bid": row.get("ce_bid"),
            "ce_ask": row.get("ce_ask"),
            "ce_bid_qty": row.get("ce_bid_qty"),
            "ce_ask_qty": row.get("ce_ask_qty"),
            "ce_iv": row.get("ce_iv", 0.0),
            "ce_delta": row.get("ce_delta", 0.0),
            "ce_gamma": row.get("ce_gamma", 0.0),
            "ce_vega": row.get("ce_vega", 0.0),
            "ce_theta": row.get("ce_theta", 0.0),

            "pe_token": row.get("pe_token"),
            "pe_symbol": row.get("pe_symbol"),
            "pe_ltp": row.get("pe_ltp", 0.0),
            "pe_lot_size": row.get("pe_lot_size"),
            "pe_bid": row.get("pe_bid"),
            "pe_ask": row.get("pe_ask"),
            "pe_bid_qty": row.get("pe_bid_qty"),
            "pe_ask_qty": row.get("pe_ask_qty"),
            "pe_iv": row.get("pe_iv", 0.0),
            "pe_delta": row.get("pe_delta", 0.0),
            "pe_gamma": row.get("pe_gamma", 0.0),
            "pe_vega": row.get("pe_vega", 0.0),
            "pe_theta": row.get("pe_theta", 0.0),

            "quote_ts": row.get("quote_ts"),
            "ce_quote_ts": row.get("ce_quote_ts"),
            "pe_quote_ts": row.get("pe_quote_ts"),
            "is_atm": bool(row.get("is_atm")) if row.get("is_atm") is not None else (strike == atm_value),
        }

        normalized_rows.append(normalized_row)

    normalized_rows.sort(key=lambda r: float(r.get("strike", 0) or 0))
    snapshot["chain"] = normalized_rows

    if atm_value is not None:
        for r in snapshot["chain"]:
            r["is_atm"] = (r.get("strike") == atm_value)

    snapshot["is_full_snapshot"] = True
    return snapshot

def _publish_chain_snapshot(symbol: str, chain: Dict[str, Any]) -> Dict[str, Any]:
    """
    Finalize and atomically publish canonical snapshot into state.

    Also keeps mutable cache updated for backward compatibility, but the
    published snapshot is the only source of truth for readers.
    """
    symbol = str(symbol or "").upper().strip()
    if symbol not in {"NIFTY", "SENSEX"}:
        raise ValueError(f"Unsupported symbol for publish: {symbol}")

    snapshot = _finalize_chain_snapshot(chain)
    snapshot["symbol"] = symbol
    snapshot["published_at"] = get_ist_now().isoformat()
    if snapshot.get("data"):
        logger.info(f"CHAIN SAMPLE BEFORE PUBLISH {symbol}: {snapshot['data'][0]}")
    published = state.publish_option_chain(symbol, snapshot)

    if not isinstance(state.option_chains, dict):
        state.option_chains = {}
    state.option_chains[symbol] = deepcopy(published)

    if hasattr(state, "broadcast_queue") and state.broadcast_queue:
        try:
            payload = deepcopy(published)
            state.broadcast_queue.put_nowait({
                "type": "option_chain_update",
                "symbol": symbol,
                "data": payload,
            })
        except Exception as e:
            logger.warning(f"Broadcast queue publish failed for {symbol}: {e}")

    return published
def _update_chain(cached_chain_data: Dict[str, Any]) -> Optional[Dict]:
    symbol_upper = str(cached_chain_data.get("symbol", "")).upper().strip()
    if not symbol_upper:
        logger.error("❌ Cannot update chain: 'symbol' missing from cached_chain_data.")
        return cached_chain_data

    try:
        if symbol_upper not in {"NIFTY", "SENSEX"}:
            return cached_chain_data

        base_symbol = cached_chain_data.get("base_symbol", symbol_upper)
        sym_config = SYMBOL_CONFIG.get(base_symbol, {})

        fut_token = cached_chain_data.get("fut_token")
        cash_segment = cached_chain_data.get("cash_segment") or sym_config.get("cash_index_segment")
        option_segment = cached_chain_data.get("exchange_segment") or sym_config.get("segment")
        gap = cached_chain_data.get("gap")
        expiry_date = cached_chain_data.get("expiry")

        if not all([fut_token, cash_segment, option_segment, gap, expiry_date, base_symbol]):
            logger.error(f"❌ Corrupt cached chain for {symbol_upper}. Missing critical data.")
            return cached_chain_data

        option_instruments = []
        for row in cached_chain_data.get("chain", []):
            if row.get("ce_token"):
                option_instruments.append({
                    "exchangeSegment": option_segment,
                    "exchangeInstrumentID": row["ce_token"]
                })
            if row.get("pe_token"):
                option_instruments.append({
                    "exchangeSegment": option_segment,
                    "exchangeInstrumentID": row["pe_token"]
                })

        if option_instruments:
            fo_quote_code = 1501 if option_segment == 12 else config.MESSAGE_CODE_LTP
            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=option_instruments,
                xtsMessageCode=fo_quote_code,
                publishFormat="JSON",
            )
            if response and response.get("type") == "success":
                for quote_str in response.get("result", {}).get("listQuotes", []):
                    quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                    token = quote.get("ExchangeInstrumentID")
                    ltp = float(
                        quote.get("LastTradedPrice")
                        or quote.get("Touchline", {}).get("LastTradedPrice")
                        or quote.get("Close")
                        or 0.0
                    )
                    if token and ltp > 0:
                        state.update_price(int(token), ltp)

        fut_ltp = get_ltp(fut_token, cash_segment)
        min_price = sym_config.get("min_future_price", 1000)

        if not fut_ltp or fut_ltp < min_price:
            fut_ltp = cached_chain_data.get("fut_ltp", 0)
            if not fut_ltp or fut_ltp < min_price:
                return cached_chain_data

        underlying_for_greeks = _get_underlying_for_greeks({
            **cached_chain_data,
            "fut_ltp": fut_ltp
        })

        atm = int(round(underlying_for_greeks / gap) * gap)
        dte = calculate_dte(expiry_date)

        updated = deepcopy(cached_chain_data)
        updated["fut_ltp"] = fut_ltp
        updated["dte"] = dte
        updated["atm"] = atm
        updated["timestamp"] = time.time()
        updated["published_at"] = get_ist_now().isoformat()
        updated["spot_ltp"] = fut_ltp
        updated["syn_ltp"] = None

        depth_instruments = []
        for row in updated.get("chain", []):
            if row.get("ce_token"):
                depth_instruments.append({
                    "exchangeSegment": option_segment,
                    "exchangeInstrumentID": row["ce_token"]
                })
            if row.get("pe_token"):
                depth_instruments.append({
                    "exchangeSegment": option_segment,
                    "exchangeInstrumentID": row["pe_token"]
                })

        depth_map = {}
        try:
            if depth_instruments:
                depth_map = get_bulk_market_depth(depth_instruments)
        except Exception as e:
            logger.warning(f"Depth refresh failed for {symbol_upper}: {e}")

        for row in updated.get("chain", []):
            strike = row["strike"]
            ce_token = row.get("ce_token")
            pe_token = row.get("pe_token")

            ce_ltp = get_ltp(ce_token, option_segment) if ce_token else 0.0
            pe_ltp = get_ltp(pe_token, option_segment) if pe_token else 0.0

            if ce_token and ce_ltp > fut_ltp:
                ce_ltp = get_ltp(ce_token, option_segment, ignore_cache=True)
                if ce_ltp > fut_ltp:
                    ce_ltp = 0.0

            if pe_token and pe_ltp > strike:
                pe_ltp = get_ltp(pe_token, option_segment, ignore_cache=True)
                if pe_ltp > strike:
                    pe_ltp = 0.0

            row["ce_ltp"] = ce_ltp
            row["pe_ltp"] = pe_ltp

            ce_depth = depth_map.get(int(ce_token), {}) if ce_token else {}
            pe_depth = depth_map.get(int(pe_token), {}) if pe_token else {}

            row["ce_bid"] = ce_depth.get("bid") or ce_depth.get("bid_price")
            row["ce_ask"] = ce_depth.get("ask") or ce_depth.get("ask_price")
            row["ce_bid_qty"] = ce_depth.get("bid_qty")
            row["ce_ask_qty"] = ce_depth.get("ask_qty")

            row["pe_bid"] = pe_depth.get("bid") or pe_depth.get("bid_price")
            row["pe_ask"] = pe_depth.get("ask") or pe_depth.get("ask_price")
            row["pe_bid_qty"] = pe_depth.get("bid_qty")
            row["pe_ask_qty"] = pe_depth.get("ask_qty")

        recomputed = _recompute_chain_rows(updated)
        recomputed["spot_ltp"] = recomputed.get("fut_ltp", fut_ltp)
        recomputed["syn_ltp"] = recomputed.get("synthetic_spot")

        for row in recomputed.get("chain", []):
            row["is_atm"] = (row.get("strike") == recomputed.get("atm"))

        return _publish_chain_snapshot(symbol_upper, recomputed)

    except Exception as e:
        logger.error(f"❌ Error during efficient chain update for {symbol_upper}: {e}", exc_info=True)
        return cached_chain_data
    
def _build_new_chain(symbol: str, strike_range: int, target_expiry: Optional[str]) -> Optional[Dict]:
    logger.info(f"📊 Building new option chain for {symbol}...")

    symbol = str(symbol or "").upper().strip()
    if symbol not in {"NIFTY", "SENSEX"}:
        logger.warning(f"Unsupported symbol in build: {symbol}")
        return None

    spot_details = get_spot_details(symbol, target_expiry=target_expiry)
    if not spot_details:
        logger.error(f"❌ Could not get spot details for {symbol}. Aborting chain build.")
        return None

    fut_ltp = spot_details["fut_ltp"]
    atm = spot_details["atm"]
    lot_size = spot_details["lot_size"]
    expiry_date = spot_details["expiry_date"]
    dte = spot_details["dte"]
    exchange_segment = spot_details["exchange_segment"]
    base_symbol = spot_details["base_symbol"]
    option_series = spot_details["option_series"]
    fut_token = spot_details["fut_token"]
    gap = spot_details["gap"]
    cash_segment = spot_details["cash_segment"]

    if base_symbol not in {"NIFTY", "SENSEX"}:
        logger.warning(f"Unsupported base symbol in build: {base_symbol}")
        return None

    is_bse = exchange_segment == 12
    strikes = [atm + i * gap for i in range(-strike_range, strike_range + 1)]

    chain_rows: List[Dict[str, Any]] = []
    instruments_to_subscribe = [{
        "exchangeSegment": cash_segment,
        "exchangeInstrumentID": fut_token
    }]

    strike_token_map: Dict[int, Dict[str, Optional[int]]] = {}
    option_instruments_for_ltp: List[Dict[str, Any]] = []
    option_instruments_for_depth: List[Dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for strike in strikes:
            futures.append(executor.submit(
                _fetch_token_for_strike, exchange_segment, option_series, base_symbol, expiry_date, "CE", strike
            ))
            futures.append(executor.submit(
                _fetch_token_for_strike, exchange_segment, option_series, base_symbol, expiry_date, "PE", strike
            ))

        for future in as_completed(futures):
            s_strike, s_type, s_token = future.result()

            if s_strike not in strike_token_map:
                strike_token_map[s_strike] = {"ce_token": None, "pe_token": None}

            if s_type == "CE":
                strike_token_map[s_strike]["ce_token"] = s_token
            else:
                strike_token_map[s_strike]["pe_token"] = s_token

            if s_token:
                instr = {
                    "exchangeSegment": exchange_segment,
                    "exchangeInstrumentID": int(s_token)
                }
                option_instruments_for_ltp.append(instr)
                option_instruments_for_depth.append(instr)

    depth_data: Dict[int, Dict[str, Any]] = {}

    if option_instruments_for_ltp:
        logger.info(f"🚚 Bulk fetching LTP for {len(option_instruments_for_ltp)} option instruments...")
        chunk_size = 50
        total_fetched = 0
        all_ltp_chunks_ok = True

        for i in range(0, len(option_instruments_for_ltp), chunk_size):
            chunk = option_instruments_for_ltp[i:i + chunk_size]
            bulk_quote_code = 1501 if is_bse else config.MESSAGE_CODE_LTP

            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=chunk,
                xtsMessageCode=bulk_quote_code,
                publishFormat="JSON"
            )

            if response and response.get("type") == "success":
                list_quotes = response.get("result", {}).get("listQuotes", [])
                for quote_str in list_quotes:
                    quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                    token = quote.get("ExchangeInstrumentID")
                    ltp = float(
                        quote.get("LastTradedPrice")
                        or quote.get("Touchline", {}).get("LastTradedPrice")
                        or quote.get("Close")
                        or 0.0
                    )
                    if token and ltp > 0:
                        state.update_price(int(token), ltp)
                total_fetched += len(list_quotes)
            else:
                all_ltp_chunks_ok = False
                logger.warning(f"⚠️ LTP chunk fetch failed for {symbol}. Response: {response}")

        if all_ltp_chunks_ok:
            try:
                logger.info(f"🚚 Bulk fetching depth for {len(option_instruments_for_depth)} option instruments...")
                depth_data = get_bulk_market_depth(option_instruments_for_depth)
                logger.info(f"✅ Fetched depth for {len(depth_data)} instruments.")
            except Exception as e:
                logger.error(f"❌ Bulk depth fetch failed for {symbol}: {e}")

            logger.info(
                f"✅ Cache populated with {total_fetched} prices from {len(option_instruments_for_ltp)} instruments."
            )
        else:
            logger.warning("⚠️ One or more bulk LTP fetch chunks failed. Some prices may be missing.")

    for strike in strikes:
        tokens = strike_token_map.get(strike, {})
        ce_token = tokens.get("ce_token")
        pe_token = tokens.get("pe_token")

        ce_depth = depth_data.get(int(ce_token), {}) if ce_token else {}
        pe_depth = depth_data.get(int(pe_token), {}) if pe_token else {}

        ce_ltp = (state.get_price(int(ce_token)) or 0.0) if ce_token else 0.0
        pe_ltp = (state.get_price(int(pe_token)) or 0.0) if pe_token else 0.0

        if ce_token and ce_ltp > 0 and ce_ltp > fut_ltp:
            new_ce = get_ltp(ce_token, exchange_segment, ignore_cache=True)
            ce_ltp = new_ce if new_ce <= fut_ltp else 0.0

        if pe_token and pe_ltp > 0 and pe_ltp > strike:
            new_pe = get_ltp(pe_token, exchange_segment, ignore_cache=True)
            pe_ltp = new_pe if new_pe <= strike else 0.0

        ce_greeks, pe_greeks = _calculate_row_greeks(strike, fut_ltp, dte, ce_ltp, pe_ltp, 0.0)

        row = {
            "strike": strike,

            "ce_token": int(ce_token) if ce_token else None,
            "ce_symbol": f"{base_symbol}{expiry_date}CE{strike}",
            "ce_ltp": ce_ltp,
            "ce_lot_size": lot_size,
            "ce_bid": ce_depth.get("bid") or ce_depth.get("bid_price"),
            "ce_ask": ce_depth.get("ask") or ce_depth.get("ask_price"),
            "ce_bid_qty": ce_depth.get("bid_qty"),
            "ce_ask_qty": ce_depth.get("ask_qty"),
            "ce_iv": round(ce_greeks.get("iv", 0) * 100, 2),
            "ce_delta": ce_greeks.get("delta", 0),
            "ce_gamma": ce_greeks.get("gamma", 0),
            "ce_vega": ce_greeks.get("vega", 0),
            "ce_theta": ce_greeks.get("theta", 0),

            "pe_token": int(pe_token) if pe_token else None,
            "pe_symbol": f"{base_symbol}{expiry_date}PE{strike}",
            "pe_ltp": pe_ltp,
            "pe_lot_size": lot_size,
            "pe_bid": pe_depth.get("bid") or pe_depth.get("bid_price"),
            "pe_ask": pe_depth.get("ask") or pe_depth.get("ask_price"),
            "pe_bid_qty": pe_depth.get("bid_qty"),
            "pe_ask_qty": pe_depth.get("ask_qty"),
            "pe_iv": round(pe_greeks.get("iv", 0) * 100, 2),
            "pe_delta": pe_greeks.get("delta", 0),
            "pe_gamma": pe_greeks.get("gamma", 0),
            "pe_vega": pe_greeks.get("vega", 0),
            "pe_theta": pe_greeks.get("theta", 0),

            "quote_ts": None,
            "is_atm": (strike == atm),
        }
        chain_rows.append(row)

    atm_row = next((r for r in chain_rows if r["strike"] == atm), None)
    synthetic_spot = fut_ltp

    if atm_row and atm_row["ce_ltp"] > 0 and atm_row["pe_ltp"] > 0:
        syn = float(atm) + float(atm_row["ce_ltp"]) - float(atm_row["pe_ltp"])
        if abs(syn - fut_ltp) <= gap * 3:
            synthetic_spot = syn
            syn_atm = int(round(syn / gap) * gap)
            if syn_atm != atm:
                logger.info(
                    f"📐 [{base_symbol}] ATM corrected: {atm} → {syn_atm} "
                    f"(spot=₹{fut_ltp:.2f}, syn_fut=₹{syn:.2f})"
                )
                atm = syn_atm

    for r in chain_rows:
        r["is_atm"] = (r["strike"] == atm)

    for row in chain_rows:
        if row["ce_token"]:
            instruments_to_subscribe.append({
                "exchangeSegment": exchange_segment,
                "exchangeInstrumentID": row["ce_token"]
            })
        if row["pe_token"]:
            instruments_to_subscribe.append({
                "exchangeSegment": exchange_segment,
                "exchangeInstrumentID": row["pe_token"]
            })

    if instruments_to_subscribe:
        unique_instruments = [dict(t) for t in {tuple(d.items()) for d in instruments_to_subscribe}]
        nse_fo_instruments = [i for i in unique_instruments if i["exchangeSegment"] == 2]
        bse_fo_instruments = [i for i in unique_instruments if i["exchangeSegment"] == 12]
        cash_instruments = [i for i in unique_instruments if i["exchangeSegment"] in (1, 11)]

        try:
            if nse_fo_instruments:
                xt_m.send_subscription(nse_fo_instruments, config.MESSAGE_CODE_LTP)
                xt_m.send_subscription(nse_fo_instruments, 1502)
                if state.socket_connected and md_socket:
                    md_socket.send_subscription(nse_fo_instruments, config.MESSAGE_CODE_LTP)
                    md_socket.send_subscription(nse_fo_instruments, 1502)

            if bse_fo_instruments:
                xt_m.send_subscription(bse_fo_instruments, 1501)
                if state.socket_connected and md_socket:
                    md_socket.send_subscription(bse_fo_instruments, 1501)

            if cash_instruments:
                xt_m.send_subscription(cash_instruments, 1501)
                if state.socket_connected and md_socket:
                    md_socket.send_subscription(cash_instruments, 1501)

            for instr in unique_instruments:
                state.add_subscription(instr["exchangeInstrumentID"])
        except Exception as e:
            logger.warning(f"⚠️ Subscription warning for {symbol}: {e}")

    chain_result = {
        "symbol": base_symbol,
        "base_symbol": base_symbol,
        "expiry": expiry_date,
        "dte": dte,
        "atm": atm,
        "fut_ltp": fut_ltp,
        "synthetic_spot": synthetic_spot,
        "fut_token": fut_token,
        "lot_size": lot_size,
        "exchange_segment": exchange_segment,
        "cash_segment": cash_segment,
        "chain": chain_rows,
        "timestamp": time.time(),
        "gap": gap,
        "option_series": option_series,
        "spot_ltp": fut_ltp,
        "future_ltp": None,
        "syn_ltp": synthetic_spot,
    }

    chain_result = _recompute_iv_with_synthetic(chain_result)
    return _publish_chain_snapshot(base_symbol, chain_result)

def get_option_chain(symbol: str, strike_range: int = 15, target_expiry: str = None) -> Optional[Dict]:
    symbol_upper = str(symbol or "").upper().strip()

    if symbol_upper not in {"NIFTY", "SENSEX"}:
        return None

    if symbol_upper not in _chain_build_locks:
        _chain_build_locks[symbol_upper] = threading.Lock()

    lock = _chain_build_locks[symbol_upper]

    with lock:
        try:
            cached_chain_data = state.get_published_option_chain(symbol_upper)

            is_cache_valid = False
            if not target_expiry and cached_chain_data and cached_chain_data.get("expiry"):
                try:
                    expiry_dt = datetime.strptime(cached_chain_data["expiry"], "%d%b%Y").date()
                    if expiry_dt >= get_ist_now().date():
                        is_cache_valid = True
                except (ValueError, TypeError):
                    is_cache_valid = False

            if is_cache_valid:
                return _update_chain(deepcopy(cached_chain_data))

            return _build_new_chain(symbol_upper, strike_range, target_expiry)

        except Exception as e:
            logger.error(f"❌ Option chain build/update error for {symbol_upper}: {e}", exc_info=True)
            return None
