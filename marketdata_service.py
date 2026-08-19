"""
marketdata_service.py — Market Data Microservice (ZMQ-based, no FastAPI)

Responsibilities:
  - XTS Market Data API login + Socket.IO connection
  - Processes tick data → shared memory price array (PriceSHM)
  - Processes quote data → in-memory quote cache
  - Builds and caches option chains with Greeks → ChainSHM
  - Publishes ZeroMQ tick signals on every chain update
  - Broadcasts price batches via ZMQ PUB
  - Broadcasts quote batches via ZMQ PUB
  - Broadcasts chain_header_update on every spot/syn_fut change
  - REST fallback polling when socket disconnected

ZMQ Ports (config.py):
  ZMQ_MARKETDATA_REQ_PORT  (REQ/REP  — option chain, LTP, health queries)
  ZMQ_MARKETDATA_PUB_PORT  (PUB/SUB  — price_update + chain_header_update broadcasts)
  ZMQ_MARKETDATA_SUB_PORT  (PULL     — subscription commands, reserved)
  ZMQ_TICK_PUB_PORT        (PUB/SUB  — tick signals → run_dev)
"""

import asyncio
import threading
import time
import json
import copy
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional
from datetime import datetime
from zoneinfo import ZoneInfo

IST = ZoneInfo("Asia/Kolkata")

import zmq
import zmq.asyncio

from utils.logger import logger
from models.state import state
from trading.chain_provider import (
    set_xts_instances,
    get_option_chain as build_get_option_chain,
    get_spot_details,
    SYMBOL_CONFIG,
    _extract_depth_from_quote,
)
from market_data.tasks import (
    update_option_chain_cache_loop,
    rest_polling_loop,
    monitor_xts_socket_status,
)
from core.shared_memory import PriceSHM, ChainSHM
from core.zmq_bus import TickPublisher
from core.resilient_task import resilient_task
from utils.helpers import get_ist_now
import config
import cred
from Connect import XTSConnect
from MarketDataSocketClient import MDSocket_io


# ── Module-level globals ──────────────────────────────────────────────────────
xt_m: XTSConnect = None
md_socket: MDSocket_io = None
main_event_loop: asyncio.AbstractEventLoop = None


# ── Snapshot helpers ──────────────────────────────────────────────────────────

def make_chain_snapshot(chain: dict) -> dict:
    """
    Create a detached snapshot of a chain so API / SHM / ZMQ consumers
    all see the same stable object for a publish cycle.
    """
    if not isinstance(chain, dict):
        return {}

    snap = copy.deepcopy(chain)
    snap["published_at"] = get_ist_now().isoformat()

    atm = snap.get("atm")
    atm_row = next(
        (r for r in snap.get("chain", [])
         if r.get("strike") == atm),
        None,
    )
    if atm_row:
        pass

    return snap


def _ensure_quote_cache():
    """
    Ensure quote cache exists on state.
    """
    if not hasattr(state, "quotes") or state.quotes is None:
        state.quotes = {}


def _state_get_quote(token: Optional[int]) -> Dict:
    """
    Safely get quote data from the central in-memory quote cache.
    """
    if not token:
        return {}
    try:
        _ensure_quote_cache()
        q = state.quotes.get(int(token))
        if isinstance(q, dict):
            return q
    except Exception:
        pass
    return {}


def _state_set_quote(token: Optional[int], quote: Dict) -> None:
    """
    Safely store quote data in central state.
    """
    if not token or not isinstance(quote, dict):
        return
    try:
        _ensure_quote_cache()
        state.quotes[int(token)] = quote
    except Exception as e:
        logger.debug(f"Quote cache set failed for token={token}: {e}")


# ── Socket Callbacks (called from socket thread) ──────────────────────────────

def on_socket_connect():
    state.socket_connected = True
    state.data_source = "WEBSOCKET"
    logger.info("✅ [MarketData] Socket CONNECTED — data source: WEBSOCKET")


def on_socket_disconnect():
    state.socket_connected = False
    state.data_source = "REST_POLL"
    logger.warning("🔌 [MarketData] Socket DISCONNECTED — falling back to REST_POLL")


def on_socket_error(error):
    logger.error(f"❌ [MarketData] Socket error: {error}")


def _queue_quote_data(data: dict):
    """
    Thread-safe: push quote data from socket thread → asyncio queue.
    Accepts packets that contain BidInfo / AskInfo.
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get("ExchangeInstrumentID")
        if not token:
            return

        # Use the robust, centralized depth extraction logic
        depth = _extract_depth_from_quote(data)
        if depth.get("depth_available") and main_event_loop and not main_event_loop.is_closed():
            asyncio.run_coroutine_threadsafe(
                state.market_data_queue.put({"type": "quote", "data": data}),
                main_event_loop
            )
    except Exception as e:
        logger.error(f"❌ [MarketData] Error queuing quote tick: {e}")


def _queue_tick_data(data: dict):
    """
    Thread-safe: push tick from socket thread → asyncio queue.
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get("ExchangeInstrumentID")
        if not token:
            return

        ltp = None

        raw_ltp = data.get("LastTradedPrice")
        if raw_ltp and float(raw_ltp) > 0:
            ltp = float(raw_ltp)

        if ltp is None:
            touchline = data.get("Touchline", {})
            if isinstance(touchline, dict):
                t_ltp = touchline.get("LastTradedPrice")
                if t_ltp and float(t_ltp) > 0:
                    ltp = float(t_ltp)

                if ltp is None:
                    t_close = touchline.get("Close")
                    if t_close and float(t_close) > 0:
                        ltp = float(t_close)

        if ltp is None:
            idx_val = data.get("IndexValue")
            if idx_val and float(idx_val) > 0:
                ltp = float(idx_val)

        if ltp is not None and main_event_loop and not main_event_loop.is_closed():
            logger.debug(f"TICK RECEIVED: Token={token}, LTP={ltp}")
            if hasattr(state, "market_data_queue") and state.market_data_queue:
                asyncio.run_coroutine_threadsafe(
                    state.market_data_queue.put({
                        "type": "ltp",
                        "data": {"ExchangeInstrumentID": token, "ltp": ltp}
                    }),
                    main_event_loop
                )
    except Exception as e:
        logger.error(f"❌ [MarketData] Error queuing tick: {e}")


# ── Message code 1512 — options / futures LTP ─────────────────────────────────
def _refresh_published_chain_quotes_for_token(token: int) -> None:
    if not token:
        return

    published = getattr(state, "published_option_chains", {}) or {}
    q = _state_get_quote(token)
    if not q:
        return

    for symbol, snap in list(published.items()):
        if not isinstance(snap, dict):
            continue

        changed = False
        for row in snap.get("chain", []):
            if not isinstance(row, dict):
                continue

            if _safe_int(row.get("ce_token")) == token:
                row["ce_ltp"] = q.get("ltp") or q.get("last_price") or row.get("ce_ltp", 0.0)
                row["ce_bid"] = q.get("bid") or q.get("bid_price") or row.get("ce_bid", 0.0)
                row["ce_ask"] = q.get("ask") or q.get("ask_price") or row.get("ce_ask", 0.0)
                row["ce_bid_qty"] = q.get("bid_qty") or row.get("ce_bid_qty", 0)
                row["ce_ask_qty"] = q.get("ask_qty") or row.get("ce_ask_qty", 0)
                row["ce_quote_ts"] = q.get("quote_ts")
                changed = True

            if _safe_int(row.get("pe_token")) == token:
                row["pe_ltp"] = q.get("ltp") or q.get("last_price") or row.get("pe_ltp", 0.0)
                row["pe_bid"] = q.get("bid") or q.get("bid_price") or row.get("pe_bid", 0.0)
                row["pe_ask"] = q.get("ask") or q.get("ask_price") or row.get("pe_ask", 0.0)
                row["pe_bid_qty"] = q.get("bid_qty") or row.get("pe_bid_qty", 0)
                row["pe_ask_qty"] = q.get("ask_qty") or row.get("pe_ask_qty", 0)
                row["pe_quote_ts"] = q.get("quote_ts")
                changed = True

        if changed and hasattr(state, "broadcast_queue") and state.broadcast_queue:
            try:
                state.broadcast_queue.put_nowait({
                    "type": "option_chain_update",
                    "symbol": symbol,
                    "data": copy.deepcopy(snap),
                })
            except Exception as e:
                logger.debug(f"Immediate quote rebroadcast failed for {symbol}: {e}")

def _normalize_1512_ltp(data: dict) -> Dict:
    token = _safe_int(
        data.get("ExchangeInstrumentID")
        or data.get("exchangeInstrumentID")
        or data.get("InstrumentID")
    )
    ltp = _safe_float(
        data.get("LastTradedPrice")
        or data.get("Touchline", {}).get("LastTradedPrice")
        or data.get("ltp"),
        0.0,
    )
    return {"token": token, "ltp": ltp}


def on_message1512_json_full(data):
    norm = _normalize_1512_ltp(data)
    token = norm.get("token")
    ltp = norm.get("ltp", 0.0)

    if token and ltp > 0:
        _merge_quote_into_state(token, {
            "ltp": ltp,
            "last_price": ltp,
            "source_1512": True,
        })
        _refresh_published_chain_quotes_for_token(token)

    _queue_quote_data(data)


def on_message1512_json_partial(data):
    on_message1512_json_full(data)
# ── Message code 1501 — Touchline / cash index spot ───────────────────────────
# If your broker feed sends top-of-book inside touchline for some instruments,
# you may also call _queue_quote_data(data) here after validating payload shape.

def on_message1501_json_full(data):
    _queue_tick_data(data)


def on_message1501_json_partial(data):
    _queue_tick_data(data)


# ── Message code 1510 — NSE index LTP ─────────────────────────────────────────

def on_message1510_json_full(data):
    _queue_tick_data(data)


def on_message1510_json_partial(data):
    _queue_tick_data(data)


# ── Message code 1502 — Market depth / quote path ─────────────────────────────
# XTS docs indicate 1502 is used for market depth / quote retrieval and contains
# best bid / ask information. [web:46][web:64]


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
        return int(v)
    except Exception:
        return default


def _normalize_1502_quote(data: dict) -> dict:
    touchline = data.get("Touchline", {}) or {}
    bids_list = data.get("Bids", []) or []
    asks_list = data.get("Asks", []) or []
    bid_info = touchline.get("BidInfo", {}) or {}
    ask_info = touchline.get("AskInfo", {}) or {}

    top_bid = bids_list[0] if bids_list and isinstance(bids_list, list) else {}
    top_ask = asks_list[0] if asks_list and isinstance(asks_list, list) else {}

    ltp = _safe_float(
        touchline.get("LastTradedPrice")
        or data.get("LastTradedPrice")
        or touchline.get("Close")
        or data.get("Close"),
        0.0,
    )

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
        or 0,
        0,
    )

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
        or 0,
        0,
    )

    normalized = dict(data)
    normalized["_normalized_depth"] = {
        "ltp": ltp,
        "last_price": ltp,
        "bid": bid_price,
        "ask": ask_price,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "bid_qty": bid_qty,
        "ask_qty": ask_qty,
        "depth_available": bool(
            ltp > 0 or bid_price > 0 or ask_price > 0 or bids_list or asks_list
        ),
    }
    return normalized

def _merge_quote_into_state(token: Optional[int], incoming: Dict) -> None:
    if not token or not isinstance(incoming, dict):
        return

    existing = _state_get_quote(token) or {}
    merged = dict(existing)

    for k, v in incoming.items():
        if v is None:
            continue
        if isinstance(v, (int, float)) and v == 0 and k in merged and merged.get(k):
            continue
        merged[k] = v

    merged["quote_ts"] = time.time()
    _state_set_quote(token, merged)

def on_message1502_json_full(data):
    normalized = _normalize_1502_quote(data)
    token = _safe_int(
        normalized.get("ExchangeInstrumentID")
        or normalized.get("exchangeInstrumentID")
        or normalized.get("InstrumentID")
    )
    depth = normalized.get("_normalized_depth", {}) or {}

    if token and depth:
        _merge_quote_into_state(token, {
            "ltp": _safe_float(depth.get("ltp"), 0.0),
            "last_price": _safe_float(depth.get("last_price"), 0.0),
            "bid": _safe_float(depth.get("bid"), 0.0),
            "ask": _safe_float(depth.get("ask"), 0.0),
            "bid_price": _safe_float(depth.get("bid_price"), 0.0),
            "ask_price": _safe_float(depth.get("ask_price"), 0.0),
            "bid_qty": _safe_int(depth.get("bid_qty"), 0),
            "ask_qty": _safe_int(depth.get("ask_qty"), 0),
            "depth_available": bool(depth.get("depth_available")),
            "source_1502": True,
        })
        _refresh_published_chain_quotes_for_token(token)

    _queue_quote_data(normalized)


def on_message1502_json_partial(data):
    on_message1502_json_full(data)

# ── Syn.Fut helper ────────────────────────────────────────────────────────────

def _recompute_syn_fut(ch: dict) -> float | None:
    """
    Compute synthetic future from ATM CE/PE prices via put-call parity:
        Syn.Fut = ATM_Strike + CE_price - PE_price
    Returns None if prices are unavailable or sanity check fails.
    """
    try:
        atm = ch.get("atm")
        gap = ch.get("gap", 50)
        if not atm:
            return None

        atm_row = next(
            (r for r in ch.get("chain", []) if r.get("strike") == atm),
            None
        )
        if not atm_row:
            return None

        ce_tok = atm_row.get("ce_token")
        pe_tok = atm_row.get("pe_token")
        if not ce_tok or not pe_tok:
            return None

        price_shm = getattr(state, "price_shm", None)
        ce_p = (price_shm.get(int(ce_tok)) if price_shm else None) or atm_row.get("ce_ltp", 0.0)
        pe_p = (price_shm.get(int(pe_tok)) if price_shm else None) or atm_row.get("pe_ltp", 0.0)

        if not ce_p or not pe_p or ce_p <= 0 or pe_p <= 0:
            return None

        syn = float(atm) + float(ce_p) - float(pe_p)

        if abs(syn - float(atm)) > gap * 2:
            return None

        return syn
    except Exception:
        return None


# ── Background Tasks ──────────────────────────────────────────────────────────

async def process_and_broadcast_market_data_queue():
    """
    Core tick processor:
      1. Drain market_data_queue
      2. Update PriceSHM
      3. Update quote cache
      4. Batch price_update -> broadcast_queue every 200ms or 200 tokens
      5. Batch depth_update -> broadcast_queue every 200ms
      6. Batch chain_quote_update -> broadcast_queue every 200ms
      7. On cash-index tick -> update chain fut_ltp + recompute syn_fut
                           -> broadcast chain_header_update immediately
      8. On ATM CE/PE tick  -> recompute syn_fut
                           -> broadcast chain_header_update immediately
    """
    logger.info("⚡ Market Data Queue Processor started")
    price_batch: Dict[int, float] = {}
    quote_batch: Dict[int, Dict] = {}
    depth_batch: Dict[int, Dict] = {}
    last_broadcast_time = time.time()

    while True:
        try:
            message = await asyncio.wait_for(
                state.market_data_queue.get(), timeout=0.5
            )
            msg_type = message.get("type")
            msg_data = message.get("data", {})

            if msg_type == "quote":
                token = msg_data.get("ExchangeInstrumentID")
                if token:
                    token_int = int(token)
                    depth = _extract_depth_from_quote(msg_data)
                    depth["quote_ts"] = get_ist_now().isoformat()
                    _state_set_quote(token_int, depth)
                    quote_batch[token_int] = depth

                    if depth.get("depth_available"):
                        bid_val = (
                            depth.get("bid")
                            or depth.get("bid_price")
                            or depth.get("best_bid")
                        )

                        ask_val = (
                            depth.get("ask")
                            or depth.get("ask_price")
                            or depth.get("best_ask")
                        )

                        depth_batch[token_int] = {
                            "bid": bid_val,
                            "ask": ask_val,
                            "bid_price": bid_val,
                            "ask_price": ask_val,
                            "best_bid": bid_val,
                            "best_ask": ask_val,
                            "bid_qty": depth.get("bid_qty"),
                            "ask_qty": depth.get("ask_qty"),
                            "ltp": depth.get("ltp"),
                            "quote_ts": depth.get("quote_ts"),
                            "depth_available": True,
                        }

            elif msg_type == "ltp":
                token = msg_data.get("ExchangeInstrumentID")
                ltp = msg_data.get("ltp")

                if token is None or ltp is None:
                    state.market_data_queue.task_done()
                    continue

                ltp_float = float(ltp)
                token_int = int(token)

                if hasattr(state, "price_shm") and state.price_shm:
                    state.price_shm.update(token_int, ltp_float)
                price_batch[token_int] = ltp_float

                chains = getattr(state, "option_chains", {})
                for sym, ch in list(chains.items()):
                    if not ch:
                        continue

                    header_dirty = False

                    fut_tok = ch.get("fut_token")
                    if fut_tok and token_int == int(fut_tok):
                        ch["fut_ltp"] = ltp_float
                        header_dirty = True

                    atm = ch.get("atm")
                    if atm:
                        atm_row = next(
                            (r for r in ch.get("chain", []) if r.get("strike") == atm),
                            None
                        )
                        if atm_row:
                            ce_tok = atm_row.get("ce_token")
                            pe_tok = atm_row.get("pe_token")
                            if (ce_tok and token_int == int(ce_tok)) or (pe_tok and token_int == int(pe_tok)):
                                header_dirty = True

                    if header_dirty:
                        syn = _recompute_syn_fut(ch)
                        if syn is not None:
                            ch["synthetic_spot"] = syn

                        spot_val = ch.get("fut_ltp", 0.0)
                        syn_val = ch.get("synthetic_spot", spot_val)

                        await state.broadcast_queue.put({
                            "type": "chain_header_update",
                            "symbol": sym,
                            "spot": spot_val,
                            "syn_fut": syn_val,
                            "atm": ch.get("atm"),
                            "expiry": ch.get("expiry", ""),
                            "dte": ch.get("dte", 0),
                        })
                        break

            state.market_data_queue.task_done()

            now = time.time()
            if len(price_batch) >= 200 or (now - last_broadcast_time) > 0.2:
                if price_batch:
                    await state.broadcast_queue.put({
                        "type": "price_update",
                        "ts": time.time(),
                        "data": price_batch.copy()
                    })
                    price_batch.clear()

                if depth_batch:
                    await state.broadcast_queue.put({
                        "type": "depth_update",
                        "ts": time.time(),
                        "data": depth_batch.copy()
                    })
                    depth_batch.clear()

                if quote_batch:
                    await state.broadcast_queue.put({
                        "type": "chain_quote_update",
                        "data": quote_batch.copy()
                    })
                    quote_batch.clear()

                last_broadcast_time = now

        except asyncio.TimeoutError:
            if price_batch:
                await state.broadcast_queue.put({
                    "type": "price_update",
                    "ts": time.time(),
                    "data": price_batch.copy()
                })
                price_batch.clear()

            if depth_batch:
                await state.broadcast_queue.put({
                    "type": "depth_update",
                    "ts": time.time(),
                    "data": depth_batch.copy()
                })
                depth_batch.clear()

            if quote_batch:
                await state.broadcast_queue.put({
                    "type": "chain_quote_update",
                    "data": quote_batch.copy()
                })
                quote_batch.clear()

            last_broadcast_time = time.time()

        except asyncio.CancelledError:
            logger.info("⚡ Queue Processor cancelled")
            break

        except Exception as e:
            logger.error(f"❌ Queue Processor error: {e}", exc_info=True)

async def broadcast_manager(pub_socket: zmq.asyncio.Socket):
    """
    Drains broadcast_queue → publishes via ZMQ PUB.
    Topic = message type bytes.
    """
    logger.info("📡 ZMQ Broadcast Manager started")
    while True:
        try:
            message = await state.broadcast_queue.get()
            topic = message.get("type", "general").encode("utf-8")
            payload = json.dumps(message).encode("utf-8")
            await pub_socket.send_multipart([topic, payload])
            state.broadcast_queue.task_done()

        except asyncio.CancelledError:
            logger.info("📡 Broadcast Manager cancelled")
            break

        except Exception as e:
            logger.error(f"❌ Broadcast Manager error: {e}", exc_info=True)


async def request_handler(rep_socket: zmq.asyncio.Socket):
    """
    ZMQ REP handler — answers queries from run_dev / snapshot / any client.

    Single-source-of-truth rule:
    - Option chain replies must come only from state.published_option_chains
    - No on-demand chain building inside this REP path
    """
    logger.info("🔧 ZMQ Request Handler started")
    loop = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=4)

    ALLOWED_CHAIN_SYMBOLS = {"NIFTY", "SENSEX"}

    while True:
        try:
            request_bytes = await rep_socket.recv()
            request = json.loads(request_bytes.decode("utf-8"))
            command = request.get("command", "")
            payload = request.get("payload") or {}
            response = {"success": False, "error": "Unknown command"}

            if command == "get_option_chain":
                symbol = str(payload.get("symbol", "")).upper().strip()

                if symbol not in ALLOWED_CHAIN_SYMBOLS:
                    response = {
                        "success": False,
                        "error": f"Unsupported symbol: {symbol}. Allowed symbols: NIFTY, SENSEX"
                    }
                else:
                    published = state.get_published_option_chain(symbol)
                    # Fallback to working chain if published snapshot is temporarily missing during startup
                    if not published or not published.get("fut_ltp"):
                        if hasattr(state, "option_chains") and symbol in state.option_chains:
                            published = state.option_chains.get(symbol)

                    if published and published.get("fut_ltp"):
                        response = {
                            "success": True,
                            "data": make_chain_snapshot(published)
                        }
                    else:
                        response = {
                            "success": False,
                            "error": f"Published chain unavailable for {symbol}"
                        }

            elif command == "get_spot_details":
                symbol = str(payload.get("symbol", "")).upper().strip()

                if symbol not in ALLOWED_CHAIN_SYMBOLS:
                    response = {
                        "success": False,
                        "error": f"Unsupported symbol: {symbol}. Allowed symbols: NIFTY, SENSEX"
                    }
                else:
                    details = await asyncio.wait_for(
                        loop.run_in_executor(executor, get_spot_details, symbol),
                        timeout=5.0
                    )
                    if details:
                        response = {"success": True, "data": details}
                    else:
                        response = {"success": False, "error": f"Spot details not found: {symbol}"}

            elif command == "get_bulk_ltp":
                tokens = payload.get("tokens", [])
                prices = {}

                if hasattr(state, "price_shm") and state.price_shm:
                    for tok in tokens:
                        try:
                            p = state.price_shm.get(int(tok))
                            if p is not None:
                                prices[str(tok)] = p
                        except Exception:
                            continue
                else:
                    for tok in tokens:
                        try:
                            p = state.get_price(int(tok))
                            if p is not None:
                                prices[str(tok)] = p
                        except Exception:
                            continue

                response = {"success": True, "data": prices}

            elif command == "get_bulk_market_depth":
                from trading.chain_provider import get_bulk_market_depth

                instruments = payload.get("instruments", [])
                depth_map = await asyncio.wait_for(
                    loop.run_in_executor(executor, get_bulk_market_depth, instruments),
                    timeout=5.0
                )
                response = {"success": True, "data": depth_map}

            elif command == "get_bulk_quotes":
                tokens = payload.get("tokens", [])
                quotes = {}

                for tok in tokens:
                    try:
                        q = _state_get_quote(tok)
                        if q:
                            quotes[str(tok)] = q
                    except Exception:
                        continue

                response = {"success": True, "data": quotes}

            elif command == "subscribe":
                symbol = str(payload.get("symbol", "")).upper().strip()
                tokens = payload.get("tokens", [])

                if symbol not in ALLOWED_CHAIN_SYMBOLS:
                    response = {
                        "success": False,
                        "error": f"Unsupported symbol: {symbol}. Allowed symbols: NIFTY, SENSEX"
                    }
                else:
                    seg = SYMBOL_CONFIG[symbol].get("segment")
                    instr = []

                    for t in tokens:
                        try:
                            t_int = int(t)
                            instr.append({
                                "exchangeSegment": seg,
                                "exchangeInstrumentID": t_int
                            })
                        except (TypeError, ValueError):
                            continue

                    if not instr:
                        response = {"success": False, "error": "No valid tokens provided"}
                    else:
                        logger.info(
                            f"📡 Subscribing {len(instr)} instruments for symbol={symbol} | "
                            f"LTP_code={config.MESSAGE_CODE_LTP} and depth_code=1502"
                        )

                        ltp_resp = md_socket.send_subscription(instr, config.MESSAGE_CODE_LTP)
                        depth_resp = md_socket.send_subscription(instr, 1502)

                        ltp_ok = bool(ltp_resp and ltp_resp.get("type") == "success")
                        depth_ok = bool(depth_resp and depth_resp.get("type") == "success")

                        logger.info(
                            f"📡 Subscription results | "
                            f"ltp_ok={ltp_ok} depth_ok={depth_ok} "
                            f"ltp_resp={ltp_resp} depth_resp={depth_resp}"
                        )

                        if ltp_ok or depth_ok:
                            for item in instr:
                                state.add_subscription(item["exchangeInstrumentID"])
                            response = {
                                "success": True,
                                "subscribed": len(instr),
                                "ltp_subscription": ltp_ok,
                                "depth_subscription": depth_ok,
                            }
                        else:
                            response = {
                                "success": False,
                                "error": "XTS subscription failed for both LTP and 1502 depth"
                            }

            elif command == "health_check":
                response = {
                    "success": True,
                    "status": "ok",
                    "service": "MarketData",
                    "socket_connected": getattr(state, "socket_connected", False),
                    "data_source": getattr(state, "data_source", "UNKNOWN"),
                    "cached_chains": list(getattr(state, "option_chains", {}).keys()),
                    "published_chains": list(getattr(state, "published_option_chains", {}).keys()),
                    "quote_cache_size": len(getattr(state, "quotes", {}) or {}),
                }

            await rep_socket.send_json(response)

        except asyncio.TimeoutError:
            await rep_socket.send_json({"success": False, "error": "Handler timed out"})

        except asyncio.CancelledError:
            logger.info("🔧 Request Handler cancelled")
            break

        except Exception as e:
            logger.error(f"❌ Request Handler error: {e}", exc_info=True)
            try:
                await rep_socket.send_json({"success": False, "error": str(e)})
            except Exception:
                pass 
 
async def chain_shm_writer():
    logger.info("🔗 ChainSHM writer started")

    last_state = {}
    last_write_ts = {}
    FORCE_WRITE_SECONDS = 3.0

    while True:
        try:
            await asyncio.sleep(0.5)

            published = getattr(state, "published_option_chains", {}) or {}
            published_snapshot = dict(published)
            now = time.monotonic()

            for symbol, chain in published_snapshot.items():
                if not isinstance(chain, dict) or not chain.get("fut_ltp"):
                    continue

                publish_base = make_chain_snapshot(chain)
                chain_rows = publish_base.get("chain", []) or []

                fut_token = publish_base.get("fut_token")
                fut_ltp = float(publish_base.get("fut_ltp", 0.0) or 0.0)

                if fut_token:
                    live_fut = state.get_price(int(fut_token))
                    if live_fut and live_fut > 0:
                        fut_ltp = float(live_fut)
                        publish_base["fut_ltp"] = fut_ltp

                for row in chain_rows:
                    try:
                        ce_tok = int(row.get("ce_token") or 0)
                        pe_tok = int(row.get("pe_token") or 0)

                        if ce_tok > 0:
                            live_ce = state.get_price(ce_tok)
                            if live_ce and live_ce > 0:
                                row["ce_ltp"] = float(live_ce)
                            ce_quote = _state_get_quote(ce_tok)
                            if ce_quote:
                                row["ce_bid"] = ce_quote.get("bid")
                                row["ce_ask"] = ce_quote.get("ask")
                                row["ce_bid_qty"] = ce_quote.get("bid_qty")
                                row["ce_ask_qty"] = ce_quote.get("ask_qty")
                                row["ce_quote_ts"] = ce_quote.get("quote_ts")

                        if pe_tok > 0:
                            live_pe = state.get_price(pe_tok)
                            if live_pe and live_pe > 0:
                                row["pe_ltp"] = float(live_pe)
                            pe_quote = _state_get_quote(pe_tok)
                            if pe_quote:
                                row["pe_bid"] = pe_quote.get("bid")
                                row["pe_ask"] = pe_quote.get("ask")
                                row["pe_bid_qty"] = pe_quote.get("bid_qty")
                                row["pe_ask_qty"] = pe_quote.get("ask_qty")
                                row["pe_quote_ts"] = pe_quote.get("quote_ts")
                    except Exception as row_e:
                        logger.debug(f"[{symbol}] row live refresh failed at strike {row.get('strike')}: {row_e}")

                atm = publish_base.get("atm")
                gap = int(publish_base.get("gap", 50) or 50)
                synthetic_spot = fut_ltp

                if atm:
                    atm_row = next((r for r in chain_rows if r.get("strike") == atm), None)
                    if atm_row:
                        atm_ce_ltp = float(atm_row.get("ce_ltp", 0.0) or 0.0)
                        atm_pe_ltp = float(atm_row.get("pe_ltp", 0.0) or 0.0)
                        if atm_ce_ltp > 0 and atm_pe_ltp > 0:
                            calculated_syn = float(atm) + atm_ce_ltp - atm_pe_ltp
                            if abs(calculated_syn - float(atm)) <= (gap * 2):
                                synthetic_spot = calculated_syn

                publish_base["synthetic_spot"] = synthetic_spot
                publish_base["syn_ltp"] = synthetic_spot

                current_key = (
                    round(fut_ltp, 2),
                    round(synthetic_spot, 2),
                    atm,
                    len(chain_rows),
                )

                force_write = (now - last_write_ts.get(symbol, 0.0)) >= FORCE_WRITE_SECONDS
                if not force_write and last_state.get(symbol) == current_key:
                    continue

                if symbol not in state.chain_shms:
                    try:
                        state.chain_shms[symbol] = ChainSHM(symbol, create=True)
                        logger.info(f"✅ ChainSHM created for {symbol}")
                    except Exception as e:
                        logger.error(f"❌ ChainSHM create failed for {symbol}: {e}")
                        continue

                publish_chain = make_chain_snapshot(publish_base)
                state.chain_shms[symbol].write(publish_chain)

                last_state[symbol] = current_key
                last_write_ts[symbol] = now

                if hasattr(state, "tick_publisher") and state.tick_publisher:
                    await state.tick_publisher.publish(symbol)

        except asyncio.CancelledError:
            logger.info("🔗 ChainSHM writer cancelled")
            break
        except Exception as e:
            logger.error(f"❌ ChainSHM writer error: {e}", exc_info=True)

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    global xt_m, md_socket, main_event_loop

    logger.debug("=" * 100)
    logger.info("🚀 STARTING MARKET DATA MICROSERVICE")
    logger.debug("=" * 100)

    ctx = zmq.asyncio.Context()
    rep_socket = ctx.socket(zmq.REP)
    pub_socket = ctx.socket(zmq.PUB)
    pull_socket = ctx.socket(zmq.PULL)

    rep_port = getattr(config, "ZMQ_MARKETDATA_REQ_PORT", 5560)
    pub_port = getattr(config, "ZMQ_MARKETDATA_PUB_PORT", 5561)
    sub_port = getattr(config, "ZMQ_MARKETDATA_SUB_PORT", 5562)

    try:
        rep_socket.bind(f"tcp://*:{rep_port}")
        logger.info(f"🔧 REQ/REP  listening on tcp://*:{rep_port}")

        pub_socket.bind(f"tcp://*:{pub_port}")
        logger.info(f"📡 PUB/SUB  listening on tcp://*:{pub_port}")

        pull_socket.bind(f"tcp://*:{sub_port}")
        logger.info(f"📬 PUSH/PULL listening on tcp://*:{sub_port}")
    except Exception as e:
        logger.error(f"❌ ZMQ socket bind failed: {e}")
        raise

    state.market_data_queue = asyncio.Queue(
        maxsize=getattr(config, "MARKET_DATA_QUEUE_SIZE", 10000)
    )
    state.broadcast_queue = asyncio.Queue()
    logger.info("✅ Async queues initialized")

    state.price_shm = PriceSHM(create=True)
    state.chain_shms = {}
    state.tick_publisher = TickPublisher()
    state.option_chains = {}
    state.published_option_chains = {}
    state.quotes = {}
    logger.info("✅ Shared memory and quote cache initialized")

    logger.info("📈 Logging into XTS Market Data API...")
    xt_m = XTSConnect(cred.API_KEY_M, cred.API_SECRET_M, "WEBAPI")
    response_m = xt_m.marketdata_login()
    if response_m.get("type") != "success":
        raise RuntimeError(
            f"Market data login failed: {response_m.get('description', 'Unknown')}"
        )
    logger.info("✅ Market Data API logged in")

    xts_token = response_m["result"]["token"]
    user_id = response_m["result"]["userID"]

    md_socket = MDSocket_io(xts_token, user_id)

    md_socket.on_connect = on_socket_connect
    md_socket.on_disconnect = on_socket_disconnect
    md_socket.on_error = on_socket_error

    md_socket.on_message1512_json_full = on_message1512_json_full
    md_socket.on_message1512_json_partial = on_message1512_json_partial

    md_socket.on_message1501_json_full = on_message1501_json_full
    md_socket.on_message1501_json_partial = on_message1501_json_partial

    md_socket.on_message1510_json_full = on_message1510_json_full
    md_socket.on_message1510_json_partial = on_message1510_json_partial

    md_socket.on_message1502_json_full = on_message1502_json_full
    md_socket.on_message1502_json_partial = on_message1502_json_partial

    logger.info("✅ Socket.IO callbacks wired: 1512 LTP | 1501 Touchline | 1510 NSE Index | 1502 Quote/Depth")

    main_event_loop = asyncio.get_event_loop()
    set_xts_instances(xt_m, md_socket)

    def _socket_thread():
        try:
            logger.info("🔗 Socket thread connecting...")
            md_socket.connect()
        except Exception as e:
            logger.error(f"❌ Socket thread error: {e}")

    threading.Thread(target=_socket_thread, daemon=True).start()
    logger.info("✅ Socket thread launched")

    await asyncio.sleep(2)

    logger.debug("=" * 100)
    logger.info("✅ MARKET DATA SERVICE READY")
    logger.info(f"   REQ/REP   port: {rep_port}")
    logger.info(f"   PUB/SUB   port: {pub_port}")
    logger.info(f"   PUSH/PULL port: {sub_port}")
    logger.debug("=" * 100)

    try:
        await asyncio.gather(
            resilient_task("queue_processor", process_and_broadcast_market_data_queue),
            resilient_task("rest_polling", rest_polling_loop),
            resilient_task("chain_cache_loop", update_option_chain_cache_loop),
            resilient_task("socket_monitor", monitor_xts_socket_status),
            resilient_task("broadcast_manager", broadcast_manager, pub_socket),
            resilient_task("request_handler", request_handler, rep_socket),
            resilient_task("chain_shm_writer", chain_shm_writer),
        )
    except asyncio.CancelledError:
        logger.info("🛑 Main gather cancelled")
    finally:
        logger.debug("=" * 100)
        logger.info("🛑 SHUTTING DOWN MARKET DATA SERVICE")
        logger.debug("=" * 100)

        if md_socket:
            try:
                md_socket.disconnect()
            except Exception:
                pass


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Market Data Service stopped by user")