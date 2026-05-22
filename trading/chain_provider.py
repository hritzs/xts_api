"""
Option Chain Provider Service
Supports: NIFTY, BANKNIFTY, FINNIFTY, MIDCPNIFTY (NSE) and SENSEX, BANKEX (BSE)
This module acts as the core of the market data service.
"""
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
from models.state import state


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
    logger.info("✅ XTS instances set in marketdata.chain_provider module")


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
    'BANKNIFTY': {
        'segment': 2,
        'cash_index_segment': 1,
        'cash_index_token': 26001,
        'gap': 100,
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
        'series_fut': 'FUTIDX',
        'series_opt': 'OPTIDX',
        'min_roll_threshold': 20,
        'max_order_qty': 4200,
        'min_future_price': 5000
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
    },
    'BANKEX': {
        'segment': 12,
        'cash_index_segment': 11,
        'cash_index_token': 26118,
        'gap': 100,
        'series_fut': 'IF',
        'series_opt': 'IO',
        'min_roll_threshold': 60,
        'max_order_qty': 4000,
        'min_future_price': 20000
    }
}


def get_ltp(token: int, exchange_segment: int = config.EXCHANGE_NSEFO, ignore_cache: bool = False) -> float:
    """Get LTP from cache or fetch via REST if not available."""
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

        if is_cash_segment:
            # ✅ FIX: Cash indices need Touchline (1501), not 1512
            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=instruments,
                xtsMessageCode=1501,
                publishFormat='JSON'
            )
        else:
            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=instruments,
                xtsMessageCode=config.MESSAGE_CODE_LTP,
                publishFormat='JSON'
            )

        list_quotes = []
        if response and response.get('type') == 'success':
            result = response.get('result', {})
            list_quotes = result.get('listQuotes', [])

        # ✅ FIX: For BSE FO (segment 12), fallback to 1501 if 1512 returns 0
        if not list_quotes and exchange_segment == 12:
            logger.warning(f"⚠️ 1512 returned no data for BSE FO token {token}. Retrying with 1501...")
            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=instruments,
                xtsMessageCode=1501,
                publishFormat='JSON'
            )
            if response and response.get('type') == 'success':
                result = response.get('result', {})
                list_quotes = result.get('listQuotes', [])

        if list_quotes:
            quote_str = list_quotes[0]
            quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
            ltp = float(
                quote.get('LastTradedPrice') or
                quote.get('Touchline', {}).get('LastTradedPrice') or
                quote.get('Close') or
                0.0
            )
            if ltp > 0:
                state.update_price(token, ltp)
                logger.info(f"💰 Fetched LTP for {token}: ₹{ltp:.2f}")
                return ltp

        # OHLC fallback for cash indices
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

                if ohlc_resp and ohlc_resp.get('type') == 'success':
                    result = ohlc_resp.get('result', {})
                    ohlc_data_str = result.get('dataPoints', '')
                    if ohlc_data_str:
                        last_candle = ohlc_data_str.strip().split('\n')[-1]
                        parts = last_candle.split('|')
                        if len(parts) >= 5:
                            ltp = float(parts[4])
                            if ltp > 0:
                                state.update_price(token, ltp)
                                logger.info(f"💰 Fetched LTP for {token} via get_ohlc fallback: ₹{ltp:.2f}")
                                return ltp
            except Exception as ohlc_e:
                logger.error(f"❌ OHLC fallback failed for {token}: {ohlc_e}")

        logger.warning(f"⚠️  Failed to fetch LTP for {token} via REST. Response: {response}")

        try:
            sub_code = 1501 if is_cash_segment or exchange_segment == 12 else config.MESSAGE_CODE_LTP
            xt_m.send_subscription(instruments, sub_code)
            state.add_subscription(token)
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

        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=1501,
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


def get_bulk_ltp(tokens: List[int], exchange_segment: int = config.EXCHANGE_NSEFO) -> Dict[int, float]:
    """Get LTP for multiple instruments via a single REST call."""
    ltp_map = {}
    try:
        if not xt_m or not tokens:
            return {}

        instruments = [{"exchangeSegment": exchange_segment, "exchangeInstrumentID": t} for t in tokens]

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
                for quote_str in list_quotes:
                    quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                    token = quote.get('ExchangeInstrumentID')
                    ltp = float(quote.get('LastTradedPrice', 0.0))
                    if token and ltp > 0:
                        ltp_map[int(token)] = ltp

        if len(ltp_map) < len(instruments):
            logger.warning(f"⚠️  Bulk LTP fetch: Got data for {len(ltp_map)}/{len(instruments)} instruments.")

        return ltp_map
    except Exception as e:
        logger.error(f"Get Bulk LTP error: {e}", exc_info=True)
        return {}


def get_bulk_market_depth(instruments: List[Dict]) -> Dict[int, Dict]:
    """Get L1 market depth (best bid/ask) for multiple instruments via a single REST call."""
    depth_map = {}
    try:
        if not xt_m or not instruments:
            return {}

        response = _call_with_retry(
            xt_m.get_quote,
            Instruments=instruments,
            xtsMessageCode=1501,
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


from utils.helpers import calculate_dte, get_ist_now


def get_spot_details(symbol: str, target_expiry: str = None, use_cache_only: bool = False) -> Optional[Dict]:
    """
    Calculates the definitive spot price (cash index) and related details.
    This is the SINGLE SOURCE OF TRUTH for spot price calculations.
    """
    symbol_upper = symbol.upper()

    if not target_expiry and not use_cache_only:
        now = time.time()
        cached = _spot_details_cache.get(symbol_upper)
        if cached and (now - cached.get('timestamp', 0)) < _spot_details_cooldown_seconds:
            logger.info(f"✅ Returning cached spot details for {symbol_upper} due to cooldown.")
            return cached['data']

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

            parsed = sorted([datetime.strptime(d_str.split("T")[0], "%Y-%m-%d") for d_str in expiry_response['result']])
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
        fut_ltp = get_ltp(cash_token, cash_segment)

        min_price = sym_config.get('min_future_price', 1000)
        if not fut_ltp or fut_ltp < min_price:
            logger.error(f"❌ Failed to get a VALID LTP for cash index token {cash_token} ({base_symbol}). Got {fut_ltp}, expected > {min_price}. Aborting.")
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

        logger.info(f"✅ Cash-based details for {base_symbol}: Spot LTP=₹{fut_ltp:.2f}, Lot Size={lot_size}")

        final_atm = int(round(fut_ltp / gap) * gap)
        logger.info(f"📊 Final Spot LTP: ₹{fut_ltp:.2f}, Final ATM: {final_atm}, Lot Size: {lot_size}")

        dte = calculate_dte(expiry_date)

        result = {
            "fut_ltp":          fut_ltp,
            "atm":              final_atm,
            "lot_size":         lot_size,
            "expiry_date":      expiry_date,
            "dte":              dte,
            "exchange_segment": exchange_segment,
            "base_symbol":      base_symbol,
            "option_series":    option_series,
            "fut_token":        cash_token,
            "cash_segment":     cash_segment,
            "gap":              gap
        }

        _spot_details_cache[symbol_upper] = {'timestamp': time.time(), 'data': result}
        return result

    except Exception as e:
        logger.error(f"❌ Error in get_spot_details: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


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


def _get_underlying_for_greeks(chain_data: Dict) -> float:
    """
    Determines the best underlying price for Greeks calculations.
    Prioritizes a freshly calculated synthetic spot, falls back to cash index.
    """
    fut_ltp = chain_data.get('fut_ltp', 0.0)
    atm = chain_data.get('atm', 0)
    gap = chain_data.get('gap', 50)
    
    # Attempt to calculate synthetic spot from ATM prices
    atm_row = next((r for r in chain_data.get('chain', []) if r.get("strike") == atm), None)
    
    if atm_row and atm_row.get("ce_ltp", 0) > 0 and atm_row.get("pe_ltp", 0) > 0:
        # Use live LTPs from the row for calculation
        ce_p = get_ltp(atm_row.get('ce_token'), chain_data.get('exchange_segment')) or atm_row.get("ce_ltp")
        pe_p = get_ltp(atm_row.get('pe_token'), chain_data.get('exchange_segment')) or atm_row.get("pe_ltp")
        syn_fut = float(atm) + ce_p - pe_p
        # Sanity check: ensure synthetic is reasonably close to cash
        if abs(syn_fut - fut_ltp) <= gap * 3:
            return syn_fut
            
    return fut_ltp # Fallback to cash index price

def _update_chain(cached_chain_data: Dict) -> Optional[Dict]:
    """Helper to update prices and greeks for an existing chain."""
    symbol_upper = cached_chain_data.get('symbol')
    if not symbol_upper:
        logger.error("❌ Cannot update chain: 'symbol' missing from cached_chain_data.")
        return cached_chain_data

    logger.debug(f"🔄 Efficiently updating existing option chain for {symbol_upper}...")

    try:
        base_symbol    = cached_chain_data.get('base_symbol', symbol_upper)
        sym_config     = SYMBOL_CONFIG.get(base_symbol, {})

        cash_token     = cached_chain_data.get('fut_token')
        cash_segment   = cached_chain_data.get('cash_segment') or sym_config.get('cash_index_segment')
        option_segment = cached_chain_data.get('exchange_segment') or sym_config.get('segment')
        gap            = cached_chain_data.get('gap')
        expiry_date    = cached_chain_data.get('expiry')

        if not all([cash_token, cash_segment, option_segment, gap, expiry_date, base_symbol]):
            logger.error(f"❌ Corrupt cached chain for {symbol_upper}. Missing critical data. Returning stale data.")
            return cached_chain_data

        # ✅ FIX: Only FO tokens in bulk fetch with 1512 — cash index EXCLUDED
        fo_instruments_to_fetch = []
        for row in cached_chain_data['chain']:
            if row.get('ce_token'):
                fo_instruments_to_fetch.append({'exchangeSegment': option_segment, 'exchangeInstrumentID': row['ce_token']})
            if row.get('pe_token'):
                fo_instruments_to_fetch.append({'exchangeSegment': option_segment, 'exchangeInstrumentID': row['pe_token']})

        if fo_instruments_to_fetch:
            is_bse_fo = option_segment == 12
            fo_quote_code = 1501 if is_bse_fo else config.MESSAGE_CODE_LTP

            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=fo_instruments_to_fetch,
                xtsMessageCode=fo_quote_code,
                publishFormat='JSON'
            )
            if response and response.get('type') == 'success':
                result = response.get('result', {})
                list_quotes = result.get('listQuotes', [])
                for quote_str in list_quotes:
                    quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                    token = quote.get('ExchangeInstrumentID')
                    ltp = float(
                        quote.get('LastTradedPrice') or
                        quote.get('Touchline', {}).get('LastTradedPrice') or
                        quote.get('Close') or
                        0.0
                    )
                    if token and ltp > 0:
                        state.update_price(int(token), ltp)

        # ✅ FIX: Cash index fetched separately with 1501 (Touchline) — works for NSE + BSE
        cash_instruments_rest = [{'exchangeSegment': cash_segment, 'exchangeInstrumentID': cash_token}]
        cash_response = _call_with_retry(
            xt_m.get_quote,
            Instruments=cash_instruments_rest,
            xtsMessageCode=1501,
            publishFormat='JSON'
        )
        if cash_response and cash_response.get('type') == 'success':
            cash_quotes = cash_response.get('result', {}).get('listQuotes', [])
            for quote_str in cash_quotes:
                quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                token = quote.get('ExchangeInstrumentID')
                ltp = float(
                    quote.get('LastTradedPrice') or
                    quote.get('Touchline', {}).get('LastTradedPrice') or
                    quote.get('Close') or
                    0.0
                )
                if token and ltp > 0:
                    state.update_price(int(token), ltp)
                    logger.debug(f"💰 Cash index {token} updated via 1501: ₹{ltp:.2f}")

        fut_ltp = get_ltp(cash_token, cash_segment)

        min_price = sym_config.get('min_future_price', 1000)
        if not fut_ltp or fut_ltp < min_price:
            logger.warning(f"⚠️ Invalid/Missing LTP for cash index {cash_token} ({fut_ltp}). Using stale LTP.")
            fut_ltp = cached_chain_data.get('fut_ltp', 0)
            if not fut_ltp or fut_ltp < min_price:
                logger.error(f"❌ Stale LTP for cash index is also invalid. Aborting update.")
                return cached_chain_data
        
        cached_chain_data['fut_ltp'] = fut_ltp # Update the cash price in the chain
        underlying_for_greeks = _get_underlying_for_greeks(cached_chain_data)
        atm = int(round(underlying_for_greeks / gap) * gap)
        dte = calculate_dte(expiry_date)
        cached_chain_data['synthetic_spot'] = underlying_for_greeks

        cached_chain_data.update({"fut_ltp": fut_ltp, "dte": dte, "atm": atm, "timestamp": time.time()})
        dte = calculate_dte(expiry_date)

        cached_chain_data.update({"fut_ltp": fut_ltp, "dte": dte, "atm": atm, "timestamp": time.time()})

        risk_free_rate = 0.0
        for row in cached_chain_data['chain']:
            strike   = row['strike']
            ce_token = row.get('ce_token')
            pe_token = row.get('pe_token')

            ce_ltp = get_ltp(ce_token, option_segment) if ce_token else 0.0
            pe_ltp = get_ltp(pe_token, option_segment) if pe_token else 0.0

            if ce_ltp > fut_ltp:
                ce_ltp = get_ltp(ce_token, option_segment, ignore_cache=True)
                if ce_ltp > fut_ltp:
                    logger.warning(f"⚠️ Invalid CE LTP {ce_ltp:.2f} > Spot {fut_ltp:.2f} for strike {strike}. Treating as 0.")
                    ce_ltp = 0.0

            if pe_ltp > strike:
                pe_ltp = get_ltp(pe_token, option_segment, ignore_cache=True)
                if pe_ltp > strike:
                    logger.warning(f"⚠️ Invalid PE LTP {pe_ltp:.2f} > Strike {strike}. Treating as 0.")
                    pe_ltp = 0.0

            ce_greeks, pe_greeks = _calculate_row_greeks(strike, underlying_for_greeks, dte, ce_ltp, pe_ltp, risk_free_rate)

            row.update({
                "ce_ltp":   ce_ltp,
                "pe_ltp":   pe_ltp,
                "ce_iv":    round(ce_greeks.get("iv",    0) * 100, 2),
                "ce_delta": ce_greeks.get("delta", 0),
                "ce_gamma": ce_greeks.get("gamma", 0),
                "ce_vega":  ce_greeks.get("vega",  0),
                "ce_theta": ce_greeks.get("theta", 0),
                "pe_iv":    round(pe_greeks.get("iv",    0) * 100, 2),
                "pe_delta": pe_greeks.get("delta", 0),
                "pe_gamma": pe_greeks.get("gamma", 0),
                "pe_vega":  pe_greeks.get("vega",  0),
                "pe_theta": pe_greeks.get("theta", 0),
                "is_atm":   row['strike'] == atm
            })

        state.update_option_chain(symbol_upper, cached_chain_data)

        if hasattr(state, 'broadcast_queue'):
            broadcast_data = cached_chain_data.copy()
            broadcast_data['fut_token'] = None
            state.broadcast_queue.put_nowait({
                'type': 'option_chain_update',
                'symbol': symbol_upper,
                'data': broadcast_data
            })

        logger.debug(f"✅ Chain update (Prices + Greeks) successful for {symbol_upper}.")
        return cached_chain_data

    except Exception as e:
        logger.error(f"❌ Error during efficient chain update for {symbol_upper}: {e}", exc_info=True)
        return cached_chain_data


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


def _build_new_chain(symbol: str, strike_range: int, target_expiry: Optional[str]) -> Optional[Dict]:
    """Helper to build a new option chain from scratch."""
    logger.info(f"📊 Building new option chain for {symbol}...")
    spot_details = get_spot_details(symbol, target_expiry=target_expiry)
    if not spot_details:
        logger.error(f"❌ Could not get spot details for {symbol}. Aborting chain build.")
        return None

    fut_ltp, atm, lot_size, expiry_date, dte, exchange_segment, base_symbol, option_series, fut_token, gap, cash_segment = (
        spot_details['fut_ltp'], spot_details['atm'], spot_details['lot_size'], spot_details['expiry_date'],
        spot_details['dte'], spot_details['exchange_segment'], spot_details['base_symbol'],
        spot_details['option_series'], spot_details['fut_token'], spot_details['gap'],
        spot_details['cash_segment']
    )

    is_bse = exchange_segment == 12  # SENSEX / BANKEX

    strikes = [atm + i * gap for i in range(-strike_range, strike_range + 1)]
    logger.info(f"📝 Strikes: {strikes[0]} to {strikes[-1]} (Total: {len(strikes)})")

    chain = []
    instruments_to_subscribe = []

    # Cash index subscribes on its own segment
    instruments_to_subscribe.append({
        "exchangeSegment": cash_segment,
        "exchangeInstrumentID": fut_token
    })

    strike_token_map = {}
    option_instruments_for_bulk_fetch = []

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for strike in strikes:
            futures.append(executor.submit(_fetch_token_for_strike, exchange_segment, option_series, base_symbol, expiry_date, "CE", strike))
            futures.append(executor.submit(_fetch_token_for_strike, exchange_segment, option_series, base_symbol, expiry_date, "PE", strike))

        for future in as_completed(futures):
            s_strike, s_type, s_token = future.result()

            if s_strike not in strike_token_map:
                strike_token_map[s_strike] = {'ce_token': None, 'pe_token': None}

            if s_type == "CE":
                strike_token_map[s_strike]['ce_token'] = s_token
            else:
                strike_token_map[s_strike]['pe_token'] = s_token

            if s_token:
                option_instruments_for_bulk_fetch.append({
                    'exchangeSegment': exchange_segment,
                    'exchangeInstrumentID': int(s_token)
                })

    if option_instruments_for_bulk_fetch:
        logger.info(f"🚚 Bulk fetching LTP for {len(option_instruments_for_bulk_fetch)} option instruments...")
        
        # --- FIX: Chunk the bulk request to avoid API limits (e.g., > 50 instruments) ---
        chunk_size = 50
        total_fetched = 0
        all_chunks_successful = True

        for i in range(0, len(option_instruments_for_bulk_fetch), chunk_size):
            chunk = option_instruments_for_bulk_fetch[i:i + chunk_size]
            bulk_quote_code = 1501 if is_bse else config.MESSAGE_CODE_LTP
            response = _call_with_retry(
                xt_m.get_quote,
                Instruments=chunk,
                xtsMessageCode=bulk_quote_code,
                publishFormat='JSON'
            )
            if response and response.get('type') == 'success':
                result = response.get('result', {})
                list_quotes = result.get('listQuotes', [])
                for quote_str in list_quotes:
                    quote = json.loads(quote_str) if isinstance(quote_str, str) else quote_str
                    token = quote.get('ExchangeInstrumentID')
                    ltp = float(quote.get('LastTradedPrice') or quote.get('Touchline', {}).get('LastTradedPrice') or quote.get('Close') or 0.0)
                    if token and ltp > 0:
                        state.update_price(int(token), ltp)
                total_fetched += len(list_quotes)
            else:
                all_chunks_successful = False
                logger.warning(f"⚠️ A chunk of the bulk LTP fetch failed. Response: {response}")

        if all_chunks_successful:
            logger.info(f"✅ Cache populated with {total_fetched} prices from {len(option_instruments_for_bulk_fetch)} instruments.")
        else:
            logger.warning(f"⚠️ One or more bulk LTP fetch chunks failed. Some prices may be missing.")

    for strike in strikes:
        tokens   = strike_token_map.get(strike, {})
        ce_token = tokens.get('ce_token')
        pe_token = tokens.get('pe_token')

        # ✅ Read directly from cache — bulk fetch already populated it above.
        # If bulk returned LTP=0, the strike is illiquid/untradeable; skip REST
        # fallback entirely to avoid serial blocking calls (1-2s each).
        ce_ltp = (state.get_price(int(ce_token)) or 0.0) if ce_token else 0.0
        pe_ltp = (state.get_price(int(pe_token)) or 0.0) if pe_token else 0.0

        # Sanity check: only retry REST for non-zero prices that look corrupt
        if ce_ltp > 0 and ce_ltp > fut_ltp:
            new_ce = get_ltp(ce_token, exchange_segment, ignore_cache=True)
            if new_ce > fut_ltp:
                logger.warning(f"⚠️ Invalid CE LTP {new_ce:.2f} > Spot {fut_ltp:.2f} for strike {strike}. Treating as 0.")
                ce_ltp = 0.0
            else:
                ce_ltp = new_ce

        if pe_ltp > 0 and pe_ltp > strike:
            new_pe = get_ltp(pe_token, exchange_segment, ignore_cache=True)
            if new_pe > strike:
                logger.warning(f"⚠️ Invalid PE LTP {new_pe:.2f} > Strike {strike}. Treating as 0.")
                pe_ltp = 0.0
            else:
                pe_ltp = new_pe

        risk_free_rate = 0.0
        ce_greeks, pe_greeks = _calculate_row_greeks(strike, fut_ltp, dte, ce_ltp, pe_ltp, risk_free_rate)

        chain.append({
            "strike":      strike,
            "ce_token":    int(ce_token) if ce_token else None,
            "ce_symbol":   f"{base_symbol}{expiry_date}CE{strike}",
            "ce_ltp":      ce_ltp,
            "ce_lot_size": lot_size,
            "ce_iv":       round(ce_greeks.get("iv",    0) * 100, 2),
            "ce_delta":    ce_greeks.get("delta", 0),
            "ce_gamma":    ce_greeks.get("gamma", 0),
            "ce_vega":     ce_greeks.get("vega",  0),
            "ce_theta":    ce_greeks.get("theta", 0),
            "pe_token":    int(pe_token) if pe_token else None,
            "pe_symbol":   f"{base_symbol}{expiry_date}PE{strike}",
            "pe_ltp":      pe_ltp,
            "pe_lot_size": lot_size,
            "pe_iv":       round(pe_greeks.get("iv",    0) * 100, 2),
            "pe_delta":    pe_greeks.get("delta", 0),
            "pe_gamma":    pe_greeks.get("gamma", 0),
            "pe_vega":     pe_greeks.get("vega",  0),
            "pe_theta":    pe_greeks.get("theta", 0),
            "is_atm":      strike == atm       # preliminary — corrected below
        })

    # ── Syn.Fut ATM correction ─────────────────────────────────────────────────
    # spot_details.atm is cash-index-based and can lag by 1 strike (~₹75 carry).
    # Put-call parity gives the correct forward: Syn.Fut = ATM_strike + CE - PE
    # We correct ATM now that we have real CE/PE LTPs from the bulk fetch.
    _atm_row = next((r for r in chain if r["strike"] == atm), None)
    syn_fut  = fut_ltp  # safe fallback if parity can't be computed

    if _atm_row and _atm_row["ce_ltp"] > 0 and _atm_row["pe_ltp"] > 0:
        _syn = float(atm) + _atm_row["ce_ltp"] - _atm_row["pe_ltp"]
        if abs(_syn - fut_ltp) <= gap * 3:          # sanity: ≤3 strikes carry drift
            syn_fut     = _syn
            syn_fut_atm = int(round(_syn / gap) * gap)
            if syn_fut_atm != atm:
                logger.info(
                    f"📐 [{base_symbol}] ATM corrected: {atm} → {syn_fut_atm} "
                    f"(spot=₹{fut_ltp:.2f}, syn_fut=₹{_syn:.2f})"
                )
                atm = syn_fut_atm
        else:
            logger.warning(
                f"⚠️ [{base_symbol}] Syn.Fut sanity failed: ₹{_syn:.2f} vs "
                f"spot ₹{fut_ltp:.2f}. Keeping spot-based ATM={atm}."
            )
    else:
        logger.debug(f"[{base_symbol}] ATM row CE/PE LTP unavailable — skipping syn_fut ATM correction.")

    # ── Add new ATM if it's outside the current range ──────────────────────────
    if atm not in strikes:
        logger.warning(
            f"Corrected ATM {atm} is outside the initial strike range. "
            "Fetching data for this new strike..."
        )
        new_strike = atm
        with ThreadPoolExecutor(max_workers=2) as executor:
            future_ce = executor.submit(_fetch_token_for_strike, exchange_segment, option_series, base_symbol, expiry_date, "CE", new_strike)
            future_pe = executor.submit(_fetch_token_for_strike, exchange_segment, option_series, base_symbol, expiry_date, "PE", new_strike)
            _, _, ce_token_new = future_ce.result()
            _, _, pe_token_new = future_pe.result()

        ce_ltp_new = get_ltp(ce_token_new, exchange_segment) if ce_token_new else 0.0
        pe_ltp_new = get_ltp(pe_token_new, exchange_segment) if pe_token_new else 0.0

        if ce_ltp_new > fut_ltp: ce_ltp_new = 0.0
        if pe_ltp_new > new_strike: pe_ltp_new = 0.0

        ce_greeks_new, pe_greeks_new = _calculate_row_greeks(new_strike, fut_ltp, dte, ce_ltp_new, pe_ltp_new, 0.0)

        chain.append({
            "strike":      new_strike,
            "ce_token":    int(ce_token_new) if ce_token_new else None,
            "ce_symbol":   f"{base_symbol}{expiry_date}CE{new_strike}",
            "ce_ltp":      ce_ltp_new,
            "ce_lot_size": lot_size,
            "ce_iv":       round(ce_greeks_new.get("iv",    0) * 100, 2),
            "ce_delta":    ce_greeks_new.get("delta", 0), "ce_gamma":    ce_greeks_new.get("gamma", 0),
            "ce_vega":     ce_greeks_new.get("vega",  0), "ce_theta":    ce_greeks_new.get("theta", 0),
            "pe_token":    int(pe_token_new) if pe_token_new else None,
            "pe_symbol":   f"{base_symbol}{expiry_date}PE{new_strike}",
            "pe_ltp":      pe_ltp_new,
            "pe_lot_size": lot_size,
            "pe_iv":       round(pe_greeks_new.get("iv",    0) * 100, 2),
            "pe_delta":    pe_greeks_new.get("delta", 0), "pe_gamma":    pe_greeks_new.get("gamma", 0),
            "pe_vega":     pe_greeks_new.get("vega",  0), "pe_theta":    pe_greeks_new.get("theta", 0),
            "is_atm":      True
        })
        chain.sort(key=lambda x: x['strike'])

    # Final ATM flag update across the potentially expanded chain
    for _r in chain:
        _r["is_atm"] = (_r["strike"] == atm)

    for row in chain:
        if row['ce_token']:
            instruments_to_subscribe.append({'exchangeSegment': exchange_segment, 'exchangeInstrumentID': row['ce_token']})
        if row['pe_token']:
            instruments_to_subscribe.append({'exchangeSegment': exchange_segment, 'exchangeInstrumentID': row['pe_token']})

    if instruments_to_subscribe:
        unique_instruments = [dict(t) for t in {tuple(d.items()) for d in instruments_to_subscribe}]

        # ✅ NSE FO (seg 2) → 1512 | BSE FO (seg 12) → 1501 | Cash (seg 1/11) → 1501
        nse_fo_instruments = [i for i in unique_instruments if i['exchangeSegment'] == 2]
        bse_fo_instruments = [i for i in unique_instruments if i['exchangeSegment'] == 12]
        cash_instruments   = [i for i in unique_instruments if i['exchangeSegment'] in (1, 11)]

        logger.info(
            f"📡 [{base_symbol}] Subscription Plan: "
            f"{len(nse_fo_instruments)} NSE FO (1512) + "
            f"{len(bse_fo_instruments)} BSE FO (1501) + "
            f"{len(cash_instruments)} cash index (1501 Touchline)"
        )

        try:
            if nse_fo_instruments:
                xt_m.send_subscription(nse_fo_instruments, config.MESSAGE_CODE_LTP)
                if state.socket_connected and md_socket:
                    md_socket.send_subscription(nse_fo_instruments, config.MESSAGE_CODE_LTP)

            if bse_fo_instruments:
                xt_m.send_subscription(bse_fo_instruments, 1501)
                if state.socket_connected and md_socket:
                    md_socket.send_subscription(bse_fo_instruments, 1501)

            if cash_instruments:
                xt_m.send_subscription(cash_instruments, 1501)
                if state.socket_connected and md_socket:
                    md_socket.send_subscription(cash_instruments, 1501)

            for instr in unique_instruments:
                state.add_subscription(instr['exchangeInstrumentID'])

        except Exception as e:
            logger.warning(f"⚠️  Subscription warning: {e}")

    logger.info(f"✅ Option chain built successfully: {len(chain)} strikes")

    chain_result = {
        "symbol":           base_symbol,
        "expiry":           expiry_date,
        "dte":              dte,
        "atm":              atm,            # ✅ syn_fut-corrected
        "fut_ltp":          fut_ltp,
        "synthetic_spot":   syn_fut,        # ✅ seeds _update_chain + tick processor
        "fut_token":        fut_token,
        "lot_size":         lot_size,
        "exchange_segment": exchange_segment,
        "cash_segment":     cash_segment,
        "chain":            chain,
        "timestamp":        time.time(),
        "gap":              gap,
        "option_series":    option_series,
    }

    if hasattr(state, 'broadcast_queue'):
        broadcast_data = chain_result.copy()
        broadcast_data['fut_token'] = None
        state.broadcast_queue.put_nowait({
            'type':   'option_chain_update',
            'symbol': base_symbol,
            'data':   broadcast_data
        })

    logger.info(f"✅ Option chain built for {base_symbol}")
    return chain_result

def get_option_chain(symbol: str, strike_range: int = 15, target_expiry: str = None) -> Optional[Dict]:
    """
    Build or update option chain with Greeks calculations.
    Single source of truth for chain creation and updates.
    - Valid cached chain → efficient price+greeks update via _update_chain
    - No/expired cache  → full build from scratch via _build_new_chain
    """
    symbol_upper = symbol.upper()

    if symbol_upper not in _chain_build_locks:
        _chain_build_locks[symbol_upper] = threading.Lock()

    lock = _chain_build_locks[symbol_upper]

    with lock:
        try:
            cached_chain_data = state.get_option_chain(symbol_upper)
            is_cache_valid = False

            if not target_expiry and cached_chain_data and 'expiry' in cached_chain_data:
                try:
                    expiry_dt = datetime.strptime(cached_chain_data['expiry'], "%d%b%Y".upper()).date()
                    if expiry_dt >= get_ist_now().date():
                        is_cache_valid = True
                except (ValueError, TypeError):
                    is_cache_valid = False

            # NEW: Check if the ATM has drifted too far from the center
            if is_cache_valid:
                try:
                    gap = cached_chain_data.get('gap')
                    cash_token = cached_chain_data.get('fut_token')
                    cash_segment = cached_chain_data.get('cash_segment')

                    if gap and cash_token and cash_segment:
                        current_spot = get_ltp(cash_token, cash_segment, ignore_cache=True)
                        if current_spot > 0:
                            new_atm = int(round(current_spot / gap) * gap)
                            chain_strikes = [r['strike'] for r in cached_chain_data.get('chain', [])]
                            if chain_strikes:
                                center_strike = chain_strikes[len(chain_strikes) // 2]
                                drift_threshold_strikes = 10
                                drift_in_strikes = abs(new_atm - center_strike) / gap

                                if drift_in_strikes > drift_threshold_strikes:
                                    logger.info(f"ATM for {symbol_upper} drifted by {drift_in_strikes:.1f} strikes. Rebuilding chain around new ATM {new_atm}.")
                                    is_cache_valid = False  # Force a rebuild
                except Exception as e:
                    logger.warning(f"Error during ATM drift check for {symbol_upper}: {e}. Proceeding with update.")

            if is_cache_valid:
                return _update_chain(cached_chain_data)
            else:
                new_chain = _build_new_chain(symbol, strike_range, target_expiry)
                if new_chain:
                    state.update_option_chain(symbol_upper, new_chain)
                return new_chain

        except Exception as e:
            logger.error(f"❌ Option chain build/update error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
