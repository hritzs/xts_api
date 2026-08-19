"""
Standalone NIFTY ATM Option Depth via REST (1502)
-------------------------------------------------
- Logs into XTS Market Data (once)
- Finds NEAREST FUTIDX NIFTY expiry >= today
- Computes ATM strike from FUT LTP
- Finds ATM NIFTY CE/PE tokens (OPTIDX, same expiry, same strike)
- Fetches depth via get_quote(xtsMessageCode=1502) for CE + PE
- Prints token, strike, LTP, bid, ask, bid_qty, ask_qty for both
"""

import json
from datetime import datetime, date

from Connect import XTSConnect
import cred
from utils.logger import logger


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


def _pick_nearest_future_expiry_iso(expiry_list):
    """
    Given a list of ISO datetime strings from get_expiry_date,
    return the nearest expiry >= today as ISO string.
    """
    today = date.today()
    parsed = []
    for e in expiry_list:
        try:
            dt = datetime.fromisoformat(e)
            parsed.append(dt)
        except Exception:
            continue

    future = [dt for dt in parsed if dt.date() >= today]
    if not future:
        raise RuntimeError("No future expiries available for NIFTY FUTIDX")

    nearest = min(future, key=lambda dt: dt.date())
    return nearest.strftime("%Y-%m-%dT%H:%M:%S")


def _extract_depth_from_1502_quote(quote: dict):
    """
    Given one 1502 quote dict (already json.loads'ed),
    return a clean depth dict: ltp, bid, ask, bid_qty, ask_qty.

    For this feed:
    - Bids / Asks are LISTS of levels [{Size, Price, ...}, ...]
    - Touchline.BidInfo / AskInfo contain the best levels too
    """
    touchline = quote.get("Touchline", {}) or {}
    bids_list = quote.get("Bids", []) or []
    asks_list = quote.get("Asks", []) or []
    bid_info = touchline.get("BidInfo", {}) or {}
    ask_info = touchline.get("AskInfo", {}) or {}

    # LTP
    ltp = (
        touchline.get("LastTradedPrice")
        or quote.get("LastTradedPrice")
        or touchline.get("Close")
        or quote.get("Close")
    )
    ltp = _safe_float(ltp, 0.0)

    # Best bid from Bids[0] or BidInfo
    if bids_list and isinstance(bids_list, list):
        top_bid = bids_list[0]
    else:
        top_bid = {}

    bid_price = _safe_float(
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
        or 0
    )

    # Best ask from Asks[0] or AskInfo
    if asks_list and isinstance(asks_list, list):
        top_ask = asks_list[0]
    else:
        top_ask = {}

    ask_price = _safe_float(
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
        or 0
    )

    return {
        "ltp": ltp,
        "bid": bid_price if bid_price > 0 else None,
        "ask": ask_price if ask_price > 0 else None,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
    }


def discover_nifty_atm_tokens(xt_m):
    """
    Pure discovery:
    - NEAREST FUTIDX expiry for NIFTY on segment 2
    - fut_token + fut_ltp -> ATM strike (gap=50)
    - OPTIDX CE/PE at that strike/expiry -> ce_token, pe_token
    Returns dict with segment, expiry_str, atm_strike, ce_token, pe_token.
    """
    NSEFO = 2
    symbol = "NIFTY"
    gap = 50

    # 1) FUTIDX expiries -> pick nearest future one (not just first)
    fut_exp_list = xt_m.get_expiry_date(
        exchangeSegment=NSEFO, series="FUTIDX", symbol=symbol
    )["result"]
    fut_exp_iso = _pick_nearest_future_expiry_iso(fut_exp_list)
    fut_exp_str = datetime.strptime(
        fut_exp_iso, "%Y-%m-%dT%H:%M:%S"
    ).strftime("%d%b%Y")

    # 2) FUTIDX token for that expiry
    fut_tokens = xt_m.get_future_symbol(
        exchangeSegment=NSEFO,
        series="FUTIDX",
        symbol=symbol,
        expiryDate=fut_exp_str,
    ).get("result", [])
    fut_token = fut_tokens[0].get("ExchangeInstrumentID") if fut_tokens else None
    if not fut_token:
        raise RuntimeError("No FUTIDX token found for NIFTY")

    # 3) FUT LTP via 1502 get_quote
    instruments = [{"exchangeSegment": NSEFO, "exchangeInstrumentID": fut_token}]
    resp_quote = xt_m.get_quote(
        Instruments=instruments, xtsMessageCode=1502, publishFormat="JSON"
    )
    list_quotes = resp_quote.get("result", {}).get("listQuotes", [])
    if not list_quotes:
        raise RuntimeError("No quote for FUTIDX NIFTY")
    fut_quote = json.loads(list_quotes[0])
    fut_ltp = fut_quote.get("Touchline", {}).get("LastTradedPrice") or fut_quote.get(
        "LastTradedPrice"
    )
    fut_ltp = _safe_float(fut_ltp, 0.0)
    if fut_ltp <= 0:
        raise RuntimeError("Invalid FUT LTP for NIFTY")

    atm_strike = round(fut_ltp / gap) * gap

    logger.info(
        f"FUT expiry={fut_exp_str}, fut_token={fut_token}, "
        f"fut_ltp={fut_ltp:.2f}, ATM strike={atm_strike}"
    )

    # 4) CE/PE tokens for that strike/expiry via OPTIDX
    ce_res = xt_m.get_option_symbol(
        exchangeSegment=NSEFO,
        series="OPTIDX",
        symbol=symbol,
        expiryDate=fut_exp_str,
        optionType="CE",
        strikePrice=atm_strike,
    ).get("result", [])
    pe_res = xt_m.get_option_symbol(
        exchangeSegment=NSEFO,
        series="OPTIDX",
        symbol=symbol,
        expiryDate=fut_exp_str,
        optionType="PE",
        strikePrice=atm_strike,
    ).get("result", [])

    if not ce_res or not pe_res:
        raise RuntimeError(
            f"No CE/PE tokens for NIFTY OPTIDX at strike={atm_strike}, expiry={fut_exp_str}"
        )

    ce_token = int(ce_res[0].get("ExchangeInstrumentID"))
    pe_token = int(pe_res[0].get("ExchangeInstrumentID"))

    logger.debug(f"✅ ATM CE token={ce_token}, PE token={pe_token} (seg={NSEFO})")

    return {
        "segment": NSEFO,
        "expiry": fut_exp_str,
        "atm_strike": atm_strike,
        "ce_token": ce_token,
        "pe_token": pe_token,
    }


def get_atm_depth_once():
    """
    End-to-end:
    - login market data
    - discover ATM tokens
    - call get_quote(1502) for CE+PE
    - print clean depth
    """
    xt_m = XTSConnect(cred.API_KEY_M, cred.API_SECRET_M, "WEBAPI")
    resp_m = xt_m.marketdata_login()
    if resp_m.get("type") != "success":
        raise RuntimeError(f"Market Data login failed: {resp_m}")
    user_id_m = resp_m["result"]["userID"]
    logger.info(f"✅ Market Data login OK, userID={user_id_m}")

    nifty_info = discover_nifty_atm_tokens(xt_m)

    instruments = [
        {
            "exchangeSegment": nifty_info["segment"],
            "exchangeInstrumentID": nifty_info["ce_token"],
        },
        {
            "exchangeSegment": nifty_info["segment"],
            "exchangeInstrumentID": nifty_info["pe_token"],
        },
    ]

    resp = xt_m.get_quote(
        Instruments=instruments,
        xtsMessageCode=1502,
        publishFormat="JSON",
    )
    logger.info(f"Raw get_quote(1502) for ATM CE/PE:\n{json.dumps(resp, indent=4)}")

    depths = {}
    list_quotes = resp.get("result", {}).get("listQuotes", [])
    for q_str in list_quotes:
        quote = json.loads(q_str)
        token = quote.get("ExchangeInstrumentID")
        depth = _extract_depth_from_1502_quote(quote)
        depths[int(token)] = depth

    # Pretty print
    logger.debug("=" * 80)
    logger.info(
        f"ATM NIFTY depth (expiry={nifty_info['expiry']}, strike={nifty_info['atm_strike']})"
    )
    for role, token in [("CE", nifty_info["ce_token"]), ("PE", nifty_info["pe_token"])]:
        d = depths.get(int(token), {})
        logger.info(
            f"{role} token={token} | "
            f"LTP={d.get('ltp')} | "
            f"Bid={d.get('bid')} (qty={d.get('bid_qty')}) | "
            f"Ask={d.get('ask')} (qty={d.get('ask_qty')})"
        )
    logger.debug("=" * 80)

    return {
        "symbol": "NIFTY",
        "expiry": nifty_info["expiry"],
        "atm_strike": nifty_info["atm_strike"],
        "ce_token": nifty_info["ce_token"],
        "pe_token": nifty_info["pe_token"],
        "depths": depths,
    }


if __name__ == "__main__":
    get_atm_depth_once()