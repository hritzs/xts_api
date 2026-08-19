"""
Socket.IO Callbacks - Fixed Queue Access
Live websocket callbacks for:
- 1512: LTP / FO market data
- 1501: Cash index touchline
- 1502: Market depth / top-of-book
"""

import asyncio
import queue
import time

from utils.logger import logger
from models.state import state


_depth_msg_count = 0
_depth_last_log_ts = 0.0


# Event loop reference
main_event_loop = None
market_data_queue = queue.Queue(maxsize=2000)


def set_main_event_loop(loop):
    """Set reference to main event loop"""
    global main_event_loop
    main_event_loop = loop
    logger.info("✅ Main event loop reference set")


def on_socket_connect():
    """Socket.IO connected"""
    state.socket_connected = True
    state.data_source = "WEBSOCKET"
    logger.info("✅ Market Data Socket CONNECTED. Data source is WEBSOCKET.")


def on_socket_disconnect():
    """Socket.IO disconnected"""
    state.socket_connected = False
    state.data_source = "REST_POLL"
    logger.warning("🔌 Market Data Socket DISCONNECTED. Data source changed to REST_POLL.")


def on_socket_error(error):
    """Socket.IO error"""
    logger.error(f"❌ Socket error: {error}")


def _safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(v)
    except Exception:
        return default


def _safe_int(v, default=0):
    try:
        if v is None or v == "":
            return default
        return int(float(v))
    except Exception:
        return default


def _extract_ltp(data: dict) -> float:
    touchline = data.get("Touchline", {}) or {}
    return _safe_float(
        data.get("LastTradedPrice")
        or touchline.get("LastTradedPrice")
        or touchline.get("Close")
        or data.get("Close")
        or 0.0
    )


def _extract_best_bid_ask(data: dict):
    """
    Normalize best bid/ask from multiple possible vendor payload shapes.
    """
    touchline = data.get("Touchline", {}) or {}

    bid_price = 0.0
    ask_price = 0.0
    bid_qty = 0
    ask_qty = 0

    bids = (
        data.get("Bids")
        or data.get("bids")
        or data.get("Bid")
        or data.get("bid")
        or []
    )
    asks = (
        data.get("Asks")
        or data.get("asks")
        or data.get("Ask")
        or data.get("ask")
        or []
    )

    if isinstance(bids, list) and bids:
        top_bid = bids[0] or {}
        bid_price = _safe_float(
            top_bid.get("Price")
            or top_bid.get("price")
            or top_bid.get("BidPrice")
            or top_bid.get("bid_price")
            or 0.0
        )
        bid_qty = _safe_int(
            top_bid.get("Quantity")
            or top_bid.get("quantity")
            or top_bid.get("Qty")
            or top_bid.get("qty")
            or top_bid.get("BidQty")
            or top_bid.get("bid_qty")
            or 0
        )

    if isinstance(asks, list) and asks:
        top_ask = asks[0] or {}
        ask_price = _safe_float(
            top_ask.get("Price")
            or top_ask.get("price")
            or top_ask.get("AskPrice")
            or top_ask.get("ask_price")
            or 0.0
        )
        ask_qty = _safe_int(
            top_ask.get("Quantity")
            or top_ask.get("quantity")
            or top_ask.get("Qty")
            or top_ask.get("qty")
            or top_ask.get("AskQty")
            or top_ask.get("ask_qty")
            or 0
        )

    if bid_price <= 0:
        bid_price = _safe_float(
            data.get("BidPrice")
            or data.get("bid_price")
            or data.get("Bid")
            or data.get("bid")
            or touchline.get("BidInfo", {}).get("Price")
            or touchline.get("BidPrice")
            or 0.0
        )

    if ask_price <= 0:
        ask_price = _safe_float(
            data.get("AskPrice")
            or data.get("ask_price")
            or data.get("Ask")
            or data.get("ask")
            or touchline.get("AskInfo", {}).get("Price")
            or touchline.get("AskPrice")
            or 0.0
        )

    if bid_qty <= 0:
        bid_qty = _safe_int(
            data.get("BidQty")
            or data.get("bid_qty")
            or touchline.get("BidInfo", {}).get("Size")
            or touchline.get("BidInfo", {}).get("Quantity")
            or touchline.get("BidQty")
            or 0
        )

    if ask_qty <= 0:
        ask_qty = _safe_int(
            data.get("AskQty")
            or data.get("ask_qty")
            or touchline.get("AskInfo", {}).get("Size")
            or touchline.get("AskInfo", {}).get("Quantity")
            or touchline.get("AskQty")
            or 0
        )

    return bid_price, ask_price, bid_qty, ask_qty


def _normalize_depth_payload(data: dict) -> dict:
    ltp = _extract_ltp(data)
    bid_price, ask_price, bid_qty, ask_qty = _extract_best_bid_ask(data)

    return {
        "ltp": ltp,
        "last_price": ltp,
        "bid": bid_price,
        "ask": ask_price,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "depth_available": bool(bid_price > 0 or ask_price > 0),
    }


def _push_ltp_to_queue(token: int, ltp: float, timestamp=None):
    """Push LTP tick to async queue."""
    if main_event_loop and hasattr(state, 'market_data_queue') and state.market_data_queue:
        try:
            asyncio.run_coroutine_threadsafe(
                state.market_data_queue.put({
                    'ExchangeInstrumentID': token,
                    'LastTradedPrice': ltp,
                    'ExchangeTimeStamp': timestamp,
                }),
                main_event_loop
            )
        except Exception as e:
            logger.error(f"❌ LTP queue put error: {e}")


def _push_depth_to_queue(token: int, normalized_depth: dict, timestamp=None):
    """Push normalized depth tick to async queue."""
    if main_event_loop and hasattr(state, 'market_data_queue') and state.market_data_queue:
        try:
            asyncio.run_coroutine_threadsafe(
                state.market_data_queue.put({
                    'ExchangeInstrumentID': token,
                    'LastTradedPrice': normalized_depth.get("ltp", 0.0),
                    'ExchangeTimeStamp': timestamp,
                    '_normalized_depth': normalized_depth,
                }),
                main_event_loop
            )
        except Exception as e:
            logger.error(f"❌ Depth queue put error: {e}")


def on_message1512_json_full(data):
    """
    Full market data update (1512) — FO options, futures, equities
    Contains: LTP, Volume, OI, Bid/Ask may also appear, etc.
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        ltp = _extract_ltp(data)

        if token:
            token = int(token)

            normalized_depth = _normalize_depth_payload(data)
            if normalized_depth.get("depth_available"):
                state.set_market_depth(token, normalized_depth)
                _push_depth_to_queue(token, normalized_depth, data.get('ExchangeTimeStamp'))

            if ltp > 0:
                state.update_price(token, ltp)
                logger.debug(f"⚡️ [1512 FULL] Token={token}, LTP={ltp}")
                _push_ltp_to_queue(token, ltp, data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1512 Full processing error: {e}")


def on_message1512_json_partial(data):
    """
    Partial market data update (1512) — usually LTP-first fast updates.
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        ltp = _extract_ltp(data)

        if token:
            token = int(token)

            normalized_depth = _normalize_depth_payload(data)
            if normalized_depth.get("depth_available"):
                state.set_market_depth(token, normalized_depth)
                _push_depth_to_queue(token, normalized_depth, data.get('ExchangeTimeStamp'))

            if ltp > 0:
                state.update_price(token, ltp)
                logger.debug(f"⚡️ [1512 PARTIAL] Token={token}, LTP={ltp}")
                _push_ltp_to_queue(token, ltp, data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1512 Partial processing error: {e}")


def on_message1501_json_full(data):
    """
    Touchline update (1501) — Cash indices.
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        ltp = _extract_ltp(data)

        if token:
            token = int(token)
            if ltp > 0:
                state.update_price(token, ltp)
                logger.debug(f"⚡️ [1501 FULL] Cash Index Token={token}, LTP={ltp}")
                _push_ltp_to_queue(token, ltp, data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1501 Full processing error: {e}")


def on_message1501_json_partial(data):
    """
    Touchline partial update (1501) — Cash indices spot price tick.
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        ltp = _extract_ltp(data)

        if token:
            token = int(token)
            if ltp > 0:
                state.update_price(token, ltp)
                logger.debug(f"⚡️ [1501 PARTIAL] Cash Index Token={token}, LTP={ltp}")
                _push_ltp_to_queue(token, ltp, data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1501 Partial processing error: {e}")


def on_message1502_json_full(data):
    """
    Full market depth update (1502) — top-of-book bid/ask path.
    This is the main websocket path for live quote caching.
    """
    global _depth_msg_count, _depth_last_log_ts

    try:
        if not isinstance(data, dict):
            return

        token = data.get("ExchangeInstrumentID")
        if not token:
            return

        token = int(token)
        normalized_depth = _normalize_depth_payload(data)

        _depth_msg_count += 1
        now = time.time()
        if now - _depth_last_log_ts >= 2:
            logger.info(
                f"[1502 LIVE] count={_depth_msg_count} token={token} "
                f"bid={normalized_depth.get('bid_price')} "
                f"ask={normalized_depth.get('ask_price')} "
                f"ltp={normalized_depth.get('ltp')}"
            )
            _depth_last_log_ts = now

        state.set_market_depth(token, normalized_depth)
        _push_depth_to_queue(token, normalized_depth, data.get("ExchangeTimeStamp"))

        ltp = normalized_depth.get("ltp", 0.0)
        if ltp > 0:
            state.update_price(token, ltp)

        logger.debug(
            f"📘 [1502 FULL] Token={token}, "
            f"Bid={normalized_depth.get('bid_price')}, Ask={normalized_depth.get('ask_price')}, "
            f"LTP={normalized_depth.get('ltp')}"
        )

    except Exception as e:
        logger.error(f"❌ 1502 Full processing error: {e}")


def on_message1502_json_partial(data):
    """
    Partial market depth update (1502) — fast quote updates.
    """
    global _depth_msg_count, _depth_last_log_ts

    try:
        if not isinstance(data, dict):
            return

        token = data.get("ExchangeInstrumentID")
        if not token:
            return

        token = int(token)
        normalized_depth = _normalize_depth_payload(data)

        _depth_msg_count += 1
        now = time.time()
        if now - _depth_last_log_ts >= 2:
            logger.info(
                f"[1502 LIVE] count={_depth_msg_count} token={token} "
                f"bid={normalized_depth.get('bid_price')} "
                f"ask={normalized_depth.get('ask_price')} "
                f"ltp={normalized_depth.get('ltp')}"
            )
            _depth_last_log_ts = now

        state.set_market_depth(token, normalized_depth)
        _push_depth_to_queue(token, normalized_depth, data.get("ExchangeTimeStamp"))

        ltp = normalized_depth.get("ltp", 0.0)
        if ltp > 0:
            state.update_price(token, ltp)

        logger.debug(
            f"📘 [1502 PARTIAL] Token={token}, "
            f"Bid={normalized_depth.get('bid_price')}, Ask={normalized_depth.get('ask_price')}, "
            f"LTP={normalized_depth.get('ltp')}"
        )

    except Exception as e:
        logger.error(f"❌ 1502 Partial processing error: {e}")