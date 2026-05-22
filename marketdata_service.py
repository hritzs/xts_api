"""
marketdata_service.py — Market Data Microservice (ZMQ-based, no FastAPI)

Responsibilities:
  - XTS Market Data API login + Socket.IO connection
  - Processes tick data → shared memory price array (PriceSHM)
  - Builds and caches option chains with Greeks → ChainSHM
  - Publishes ZeroMQ tick signals on every chain update
  - Broadcasts price batches via ZMQ PUB
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
from concurrent.futures import ThreadPoolExecutor
from typing import Dict

import zmq
import zmq.asyncio

from utils.logger       import logger
from models.state       import state
from trading.chain_provider import (
    set_xts_instances,
    get_option_chain as build_get_option_chain,
    get_spot_details,
    SYMBOL_CONFIG,
)
from market_data.tasks import (
    update_option_chain_cache_loop,
    rest_polling_loop,
    monitor_xts_socket_status,
    calculate_greeks_loop,
)
from core.shared_memory import PriceSHM, ChainSHM
from core.zmq_bus       import TickPublisher
from core.resilient_task import resilient_task
import config
import cred
from Connect                import XTSConnect
from MarketDataSocketClient import MDSocket_io


# ── Module-level globals ──────────────────────────────────────────────────────
xt_m:            XTSConnect  = None
md_socket:       MDSocket_io = None
main_event_loop: asyncio.AbstractEventLoop = None


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


def _queue_tick_data(data: dict):
    """Thread-safe: push tick from socket thread → asyncio queue."""
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        if not token:
            return

        # ✅ FIX: Robust LTP extraction covering all message codes:
        #   1512 → top-level LastTradedPrice
        #   1501 → Touchline.LastTradedPrice (cash index Touchline)
        #   1510 → IndexValue (NSE index)
        #   1502 → IndexValue (BSE index)
        ltp = None

        # 1. Top-level LastTradedPrice (1512 options/futures)
        raw_ltp = data.get('LastTradedPrice')
        if raw_ltp and float(raw_ltp) > 0:
            ltp = float(raw_ltp)

        # 2. Touchline.LastTradedPrice (1501 cash index Touchline)
        if ltp is None:
            touchline = data.get('Touchline', {})
            if isinstance(touchline, dict):
                t_ltp = touchline.get('LastTradedPrice')
                if t_ltp and float(t_ltp) > 0:
                    ltp = float(t_ltp)
                # Also try Close as last resort within Touchline
                if ltp is None:
                    t_close = touchline.get('Close')
                    if t_close and float(t_close) > 0:
                        ltp = float(t_close)

        # 3. IndexValue (1510 NSE index / 1502 BSE index)
        if ltp is None:
            idx_val = data.get('IndexValue')
            if idx_val and float(idx_val) > 0:
                ltp = float(idx_val)

        if ltp is not None and main_event_loop and not main_event_loop.is_closed():
            logger.debug(f"TICK RECEIVED: Token={token}, LTP={ltp}")
            if hasattr(state, 'market_data_queue') and state.market_data_queue:
                asyncio.run_coroutine_threadsafe(
                    state.market_data_queue.put({
                        'ExchangeInstrumentID': token,
                        'ltp': ltp
                    }),
                    main_event_loop
                )
    except Exception as e:
        logger.error(f"❌ [MarketData] Error queuing tick: {e}")


# ── Message code 1512 — options / futures LTP ─────────────────────────────────

def on_message1512_json_full(data):
    _queue_tick_data(data)

def on_message1512_json_partial(data):
    _queue_tick_data(data)


# ── Message code 1501 — Touchline / cash index spot (NSE + BSE) ✅ ────────────
# All cash indices (NIFTY 26000, BANKNIFTY 26001, SENSEX 26065, etc.)
# stream real-time spot via 1501 Touchline. This is the PRIMARY spot feed.

def on_message1501_json_full(data):
    _queue_tick_data(data)

def on_message1501_json_partial(data):
    _queue_tick_data(data)


# ── Message code 1510 — NSE index LTP (secondary fallback) ───────────────────

def on_message1510_json_full(data):
    _queue_tick_data(data)

def on_message1510_json_partial(data):
    _queue_tick_data(data)


# ── Message code 1502 — BSE index LTP (secondary fallback) ───────────────────

def on_message1502_json_full(data):
    _queue_tick_data(data)

def on_message1502_json_partial(data):
    _queue_tick_data(data)


# ── Syn.Fut helper ────────────────────────────────────────────────────────────

def _recompute_syn_fut(ch: dict) -> float | None:
    """
    Compute synthetic future from ATM CE/PE prices via put-call parity:
        Syn.Fut = ATM_Strike + CE_price - PE_price
    Returns None if prices are unavailable or sanity check fails.
    """
    try:
        atm = ch.get('atm')
        gap = ch.get('gap', 50)
        if not atm:
            return None

        atm_row = next(
            (r for r in ch.get('chain', []) if r.get('strike') == atm),
            None
        )
        if not atm_row:
            return None

        ce_tok = atm_row.get('ce_token')
        pe_tok = atm_row.get('pe_token')
        if not ce_tok or not pe_tok:
            return None

        price_shm = getattr(state, 'price_shm', None)
        ce_p = (price_shm.get(int(ce_tok)) if price_shm else None) or atm_row.get('ce_ltp', 0.0)
        pe_p = (price_shm.get(int(pe_tok)) if price_shm else None) or atm_row.get('pe_ltp', 0.0)

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
      3. Batch price_update → broadcast_queue every 200ms or 200 tokens
      4. On cash-index tick → update chain fut_ltp + recompute syn_fut
                            → broadcast chain_header_update immediately
      5. On ATM CE/PE tick  → recompute syn_fut
                            → broadcast chain_header_update immediately
    """
    logger.info("⚡ Market Data Queue Processor started")
    price_batch: Dict[int, float] = {}
    last_broadcast_time = time.time()

    while True:
        try:
            tick = await asyncio.wait_for(
                state.market_data_queue.get(), timeout=0.5
            )
            token = tick.get('ExchangeInstrumentID')
            ltp   = tick.get('ltp')

            if token and ltp:
                ltp_float = float(ltp)
                token_int = int(token)

                # ── 1. Write to PriceSHM ────────────────────────────────────
                if hasattr(state, 'price_shm') and state.price_shm:
                    state.price_shm.update(token_int, ltp_float)
                price_batch[token] = ltp_float

                # ── 2. Chain header updates ─────────────────────────────────
                chains = getattr(state, 'option_chains', {})
                for sym, ch in list(chains.items()):
                    if not ch:
                        continue

                    header_dirty = False

                    # Case A: Cash index token ticked → update Spot
                    fut_tok = ch.get('fut_token')
                    if fut_tok and token_int == int(fut_tok):
                        ch['fut_ltp'] = ltp_float
                        header_dirty = True

                    # Case B: ATM CE or PE ticked → update Syn.Fut
                    atm = ch.get('atm')
                    if atm:
                        atm_row = next(
                            (r for r in ch.get('chain', []) if r.get('strike') == atm),
                            None
                        )
                        if atm_row:
                            ce_tok = atm_row.get('ce_token')
                            pe_tok = atm_row.get('pe_token')
                            if (ce_tok and token_int == int(ce_tok)) or \
                               (pe_tok and token_int == int(pe_tok)):
                                header_dirty = True

                    if header_dirty:
                        syn = _recompute_syn_fut(ch)
                        if syn is not None:
                            ch['synthetic_spot'] = syn

                        spot_val = ch.get('fut_ltp', 0.0)
                        syn_val  = ch.get('synthetic_spot', spot_val)

                        await state.broadcast_queue.put({
                            'type':    'chain_header_update',
                            'symbol':  sym,
                            'spot':    spot_val,
                            'syn_fut': syn_val,
                            'atm':     ch.get('atm'),
                            'expiry':  ch.get('expiry', ''),
                            'dte':     ch.get('dte', 0),
                        })
                        break  # One symbol per tick — avoids O(N) on every tick

            state.market_data_queue.task_done()

            # ── 3. Batch price_update broadcast ────────────────────────────
            now = time.time()
            if len(price_batch) >= 200 or (now - last_broadcast_time) > 0.2:
                if price_batch:
                    await state.broadcast_queue.put({
                        'type': 'price_update',
                        'data': price_batch.copy()
                    })
                    price_batch.clear()
                    last_broadcast_time = now

        except asyncio.TimeoutError:
            if price_batch:
                await state.broadcast_queue.put({
                    'type': 'price_update',
                    'data': price_batch.copy()
                })
                price_batch.clear()
                last_broadcast_time = time.time()
        except asyncio.CancelledError:
            logger.info("⚡ Queue Processor cancelled")
            break
        except Exception as e:
            logger.error(f"❌ Queue Processor error: {e}", exc_info=True)


async def broadcast_manager(pub_socket: zmq.asyncio.Socket):
    """
    Drains broadcast_queue → publishes via ZMQ PUB.
    Topic = message type bytes (e.g. b'price_update', b'chain_header_update')
    """
    logger.info("📡 ZMQ Broadcast Manager started")
    while True:
        try:
            message = await state.broadcast_queue.get()
            topic   = message.get('type', 'general').encode('utf-8')
            payload = json.dumps(message).encode('utf-8')
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

    Supported commands:
      get_option_chain      {symbol}
      get_spot_details      {symbol}
      get_bulk_ltp          {tokens: [int, ...]}
      get_bulk_market_depth {instruments: [...]}
      subscribe             {symbol, tokens: [int, ...]}
      health_check          {}
    """
    logger.info("🔧 ZMQ Request Handler started")
    loop     = asyncio.get_event_loop()
    executor = ThreadPoolExecutor(max_workers=4)

    while True:
        try:
            request_bytes = await rep_socket.recv()
            request       = json.loads(request_bytes.decode('utf-8'))
            command       = request.get("command", "")
            payload       = request.get("payload") or {}
            response      = {"success": False, "error": "Unknown command"}

            # ── Option chain ─────────────────────────────────────────────────
            if command == "get_option_chain":
                symbol = payload.get("symbol", "").upper()
                cached = getattr(state, 'option_chains', {}).get(symbol)
                if cached and cached.get('fut_ltp'):
                    response = {"success": True, "data": cached}
                else:
                    logger.info(f"📥 ZMQ REQ: building chain for {symbol}")
                    chain = await asyncio.wait_for(
                        loop.run_in_executor(executor, build_get_option_chain, symbol),
                        timeout=10.0
                    )
                    if chain:
                        response = {"success": True, "data": chain}
                    else:
                        response = {"success": False, "error": f"Chain unavailable for {symbol}"}

            # ── Spot details ─────────────────────────────────────────────────
            elif command == "get_spot_details":
                symbol  = payload.get("symbol", "").upper()
                details = await asyncio.wait_for(
                    loop.run_in_executor(executor, get_spot_details, symbol),
                    timeout=5.0
                )
                if details:
                    response = {"success": True, "data": details}
                else:
                    response = {"success": False, "error": f"Spot details not found: {symbol}"}

            # ── Bulk LTP (from PriceSHM — sub-millisecond) ───────────────────
            elif command == "get_bulk_ltp":
                tokens = payload.get("tokens", [])
                prices = {}
                if hasattr(state, 'price_shm') and state.price_shm:
                    for tok in tokens:
                        p = state.price_shm.get(int(tok))
                        if p:
                            prices[tok] = p
                response = {"success": True, "data": prices}

            # ── Bulk market depth ─────────────────────────────────────────────
            elif command == "get_bulk_market_depth":
                from trading.chain_provider import get_bulk_market_depth
                instruments = payload.get("instruments", [])
                depth_map   = await asyncio.wait_for(
                    loop.run_in_executor(executor, get_bulk_market_depth, instruments),
                    timeout=5.0
                )
                response = {"success": True, "data": depth_map}

            # ── Subscribe tokens ──────────────────────────────────────────────
            elif command == "subscribe":
                symbol = payload.get("symbol", "").upper()
                tokens = payload.get("tokens", [])
                base   = next(
                    (k for k in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True)
                     if k in symbol), None
                )
                if not base:
                    response = {"success": False, "error": f"Symbol not in SYMBOL_CONFIG: {symbol}"}
                else:
                    seg   = SYMBOL_CONFIG[base].get('segment')
                    instr = [{'exchangeSegment': seg, 'exchangeInstrumentID': t} for t in tokens]
                    resp  = md_socket.send_subscription(instr, config.MESSAGE_CODE_LTP)
                    if resp and resp.get('type') == 'success':
                        for t in tokens:
                            state.add_subscription(t)
                        response = {"success": True, "subscribed": len(tokens)}
                    else:
                        response = {"success": False, "error": "XTS subscription failed"}

            # ── Health ────────────────────────────────────────────────────────
            elif command == "health_check":
                response = {
                    "success":          True,
                    "status":           "ok",
                    "service":          "MarketData",
                    "socket_connected": getattr(state, 'socket_connected', False),
                    "data_source":      getattr(state, 'data_source', 'UNKNOWN'),
                    "cached_chains":    list(getattr(state, 'option_chains', {}).keys()),
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
    """
    Watches state.option_chains for updates.
    On change to fut_ltp OR synthetic_spot: writes to ChainSHM + publishes ZeroMQ tick signal.
    Runs every 500ms — low overhead.
    """
    logger.info("🔗 ChainSHM writer started")
    last_state = {}  # symbol → (fut_ltp, syn_fut) tuple

    while True:
        try:
            await asyncio.sleep(0.5)
            chains = getattr(state, 'option_chains', {})

            for symbol, chain in chains.items():
                if not chain or not chain.get('fut_ltp'):
                    continue

                fut_ltp     = chain.get('fut_ltp', 0.0)
                syn_fut     = chain.get('synthetic_spot', fut_ltp)
                current_key = (fut_ltp, round(syn_fut, 2))

                if last_state.get(symbol) == current_key:
                    continue

                if symbol not in state.chain_shms:
                    try:
                        state.chain_shms[symbol] = ChainSHM(symbol, create=True)
                        logger.info(f"✅ ChainSHM created for {symbol}")
                    except Exception as e:
                        logger.error(f"❌ ChainSHM create failed for {symbol}: {e}")
                        continue

                state.chain_shms[symbol].write(chain)
                last_state[symbol] = current_key

                if hasattr(state, 'tick_publisher') and state.tick_publisher:
                    await state.tick_publisher.publish(symbol)

        except asyncio.CancelledError:
            logger.info("🔗 ChainSHM writer cancelled")
            break
        except Exception as e:
            logger.error(f"❌ ChainSHM writer error: {e}", exc_info=True)


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    global xt_m, md_socket, main_event_loop

    logger.info("=" * 100)
    logger.info("🚀 STARTING MARKET DATA MICROSERVICE")
    logger.info("=" * 100)

    # ── ZMQ context + sockets ─────────────────────────────────────────────────
    ctx         = zmq.asyncio.Context()
    rep_socket  = ctx.socket(zmq.REP)
    pub_socket  = ctx.socket(zmq.PUB)
    pull_socket = ctx.socket(zmq.PULL)

    rep_port = getattr(config, 'ZMQ_MARKETDATA_REQ_PORT', 5560)
    pub_port = getattr(config, 'ZMQ_MARKETDATA_PUB_PORT', 5561)
    sub_port = getattr(config, 'ZMQ_MARKETDATA_SUB_PORT', 5562)

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

    # ── Queues ────────────────────────────────────────────────────────────────
    state.market_data_queue = asyncio.Queue(
        maxsize=getattr(config, 'MARKET_DATA_QUEUE_SIZE', 10000)
    )
    state.broadcast_queue = asyncio.Queue()
    logger.info("✅ Async queues initialized")

    # ── Shared Memory ─────────────────────────────────────────────────────────
    state.price_shm      = PriceSHM(create=True)
    state.chain_shms     = {}
    state.tick_publisher = TickPublisher()
    state.option_chains  = {}
    logger.info("✅ Shared memory initialized")

    # ── XTS Login ─────────────────────────────────────────────────────────────
    logger.info("📈 Logging into XTS Market Data API...")
    xt_m       = XTSConnect(cred.API_KEY_M, cred.API_SECRET_M, "WEBAPI")
    response_m = xt_m.marketdata_login()
    if response_m.get('type') != 'success':
        raise RuntimeError(
            f"Market data login failed: {response_m.get('description', 'Unknown')}"
        )
    logger.info("✅ Market Data API logged in")

    xts_token = response_m['result']['token']
    user_id   = response_m['result']['userID']

    # ── Socket.IO — Wire ALL callbacks ────────────────────────────────────────
    md_socket = MDSocket_io(xts_token, user_id)

    md_socket.on_connect    = on_socket_connect
    md_socket.on_disconnect = on_socket_disconnect
    md_socket.on_error      = on_socket_error

    # ✅ 1512 — NSE/BSE FO options + futures LTP
    md_socket.on_message1512_json_full    = on_message1512_json_full
    md_socket.on_message1512_json_partial = on_message1512_json_partial

    # ✅ 1501 — Touchline (ALL cash indices: NIFTY, BANKNIFTY, SENSEX, BANKEX etc.)
    #          PRIMARY real-time Spot feed — was MISSING, caused frozen Spot price
    md_socket.on_message1501_json_full    = on_message1501_json_full
    md_socket.on_message1501_json_partial = on_message1501_json_partial

    # ✅ 1510 — NSE index LTP (secondary fallback)
    md_socket.on_message1510_json_full    = on_message1510_json_full
    md_socket.on_message1510_json_partial = on_message1510_json_partial

    # ✅ 1502 — BSE index LTP (secondary fallback)
    md_socket.on_message1502_json_full    = on_message1502_json_full
    md_socket.on_message1502_json_partial = on_message1502_json_partial

    logger.info("✅ Socket.IO callbacks wired: 1512 FO | 1501 Touchline | 1510 NSE | 1502 BSE")

    main_event_loop = asyncio.get_event_loop()
    set_xts_instances(xt_m, md_socket)
    logger.info("✅ XTS instances set in chain_provider")

    # TEMPORARY token discovery — remove after confirming tokens
    for _seg in ["NSECM", "NSEIX", "NSECD", "NSECO"]:
        try:
            _mr = xt_m.get_master(exchangeSegmentList=[_seg])
            for _l in _mr.get('result', '').strip().split('\n'):
                _c = _l.split('|')
                if len(_c) > 3 and any(x in _c[3].upper() for x in ['BANKNIFTY', 'MIDCPNIFTY', 'FINNIFTY']):
                    logger.info(f"🔍 [{_seg}] TOKEN: {_c[1]} | NAME: {_c[3]} | TYPE: {_c[2]}")
        except Exception as _e:
            logger.info(f"🔍 [{_seg}] skipped: {_e}")

    # ── Socket thread ─────────────────────────────────────────────────────────
    def _socket_thread():
        try:
            logger.info("🔗 Socket thread connecting...")
            md_socket.connect()
        except Exception as e:
            logger.error(f"❌ Socket thread error: {e}")

    threading.Thread(target=_socket_thread, daemon=True).start()
    logger.info("✅ Socket thread launched")
    await asyncio.sleep(2)
    logger.info(f"🔍 DIAG: socket_connected={state.socket_connected}, "
                f"data_source={getattr(state,'data_source','?')}, "
                f"market_data_queue size={state.market_data_queue.qsize() if hasattr(state,'market_data_queue') else '?'}")
    for _ in range(10):
        if getattr(state, 'socket_connected', False):
            break
        await asyncio.sleep(0.5)

    logger.info("=" * 100)
    logger.info("✅ MARKET DATA SERVICE READY")
    logger.info(f"   REQ/REP   port: {rep_port}")
    logger.info(f"   PUB/SUB   port: {pub_port}")
    logger.info(f"   PUSH/PULL port: {sub_port}")
    logger.info("=" * 100)

    # ── Launch all tasks ──────────────────────────────────────────────────────
    try:
        await asyncio.gather(
            resilient_task("queue_processor",   process_and_broadcast_market_data_queue),
            resilient_task("rest_polling",      rest_polling_loop),
            resilient_task("chain_cache_loop",  update_option_chain_cache_loop),
            resilient_task("socket_monitor",    monitor_xts_socket_status),
            resilient_task("greeks_loop",       calculate_greeks_loop),
            resilient_task("broadcast_manager", broadcast_manager, pub_socket),
            resilient_task("request_handler",   request_handler,   rep_socket),
            resilient_task("chain_shm_writer",  chain_shm_writer),
        )
    except asyncio.CancelledError:
        logger.info("🛑 Main gather cancelled")
    finally:
        logger.info("=" * 100)
        logger.info("🛑 SHUTTING DOWN MARKET DATA SERVICE")
        logger.info("=" * 100)

        if md_socket:
            try:
                md_socket.disconnect()
            except Exception:
                pass

        for sock in (rep_socket, pub_socket, pull_socket):
            try:
                sock.close(linger=0)
            except Exception:
                pass
        try:
            ctx.term()
        except Exception:
            pass

        if hasattr(state, 'price_shm') and state.price_shm:
            state.price_shm.close(unlink=True)
        for shm in getattr(state, 'chain_shms', {}).values():
            try:
                shm.close(unlink=True)
            except Exception:
                pass
        if hasattr(state, 'tick_publisher') and state.tick_publisher:
            state.tick_publisher.close()

        logger.info("✅ MARKET DATA SERVICE SHUTDOWN COMPLETE")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Market Data Service stopped by user")
