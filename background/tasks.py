"""
background/tasks.py — ACTION TASKS ONLY

All snapshot computation (Greeks, PnL, IV) → snapshot_service.py (port 8003)
All order verification                      → verifier_service.py  (port 8004)

This module handles ONLY:
  - WebSocket keepalive
  - XTS socket status monitor
  - DB cleanup
  - start_verification_task()        → ZMQ PUSH to verifier_service
  - trigger_snapshot_and_broadcast() → ZMQ PUSH to snapshot_service (DEBOUNCED)
  - broadcast_message / broadcast_log (main app WebSocket)
  - snapshot_bridge_loop()           → WS bridge 8003 → 5000
  - marketdata_bridge_loop()         → ZMQ SUB bridge marketdata PUB → 5000
                                       also caches chain_header_update for new WS clients
"""
import asyncio
import zmq
import zmq.asyncio
import time as _time
import json
import httpx
from typing import Set, List, Dict, Optional
from utils.logger import logger
from models.state import state
from core.zmq_bus import FillsSubscriber
from core.shared_memory import OrderSHM
from utils.helpers import get_ist_now

import config


_websocket_clients: Set = set()

SNAPSHOT_URL    = f"http://localhost:{getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}"
VERIFIER_URL    = f"http://localhost:{getattr(config, 'VERIFIER_SERVICE_PORT', 8004)}"
MARKET_DATA_URL = f"http://localhost:{getattr(config, 'MARKET_DATA_PORT', 8001)}"

# ── Snapshot debounce ─────────────────────────────────────────────────────────
_snapshot_debounce:   Dict[str, float] = {}
_SNAPSHOT_DEBOUNCE_S: float            = 1.0

# ── ZMQ Sockets ───────────────────────────────────────────────────────────────
_zmq_ctx:                     zmq.asyncio.Context = None
_verifier_push_socket:        zmq.asyncio.Socket  = None
_snapshot_force_push_socket:  zmq.asyncio.Socket  = None
_snapshot_verify_push_socket: zmq.asyncio.Socket  = None


def _get_verifier_socket() -> zmq.asyncio.Socket:
    """Initializes and returns a singleton ZMQ PUSH socket to the verifier/reconciler."""
    global _zmq_ctx, _verifier_push_socket
    if _verifier_push_socket is None:
        _zmq_ctx = zmq.asyncio.Context.instance()
        _verifier_push_socket = _zmq_ctx.socket(zmq.PUSH)
        _verifier_push_socket.connect(f"tcp://localhost:{config.ZMQ_VERIFIER_PULL_PORT}")
        logger.info(f"🔌 ZMQ PUSH socket connected to verifier/reconciler on port {config.ZMQ_VERIFIER_PULL_PORT}")
    return _verifier_push_socket


def _get_snapshot_force_push_socket() -> zmq.asyncio.Socket:
    """Initializes and returns a singleton ZMQ PUSH socket to the snapshot service for force updates."""
    global _zmq_ctx, _snapshot_force_push_socket
    if _snapshot_force_push_socket is None:
        _zmq_ctx = _zmq_ctx or zmq.asyncio.Context.instance()
        _snapshot_force_push_socket = _zmq_ctx.socket(zmq.PUSH)
        _snapshot_force_push_socket.connect(f"tcp://localhost:{config.ZMQ_SNAPSHOT_FORCE_PULL_PORT}")
        logger.info(f"🔌 ZMQ PUSH socket connected to snapshot service (force) on port {config.ZMQ_SNAPSHOT_FORCE_PULL_PORT}")
    return _snapshot_force_push_socket


def _get_snapshot_verify_push_socket() -> zmq.asyncio.Socket:
    """Initializes and returns a singleton ZMQ PUSH socket to the snapshot service for verification notifications."""
    global _zmq_ctx, _snapshot_verify_push_socket
    if _snapshot_verify_push_socket is None:
        _zmq_ctx = _zmq_ctx or zmq.asyncio.Context.instance()
        _snapshot_verify_push_socket = _zmq_ctx.socket(zmq.PUSH)
        _snapshot_verify_push_socket.connect(f"tcp://localhost:{config.ZMQ_SNAPSHOT_PULL_PORT}")
        logger.info(f"🔌 ZMQ PUSH socket connected to snapshot service (verify) on port {config.ZMQ_SNAPSHOT_PULL_PORT}")
    return _snapshot_verify_push_socket


# ── WebSocket helpers (main app WS :5000) ─────────────────────────────────────

def set_websocket_clients(clients: Set):
    global _websocket_clients
    _websocket_clients = clients


async def broadcast_message(message: dict):
    if not _websocket_clients:
        return
    dead = set()
    for client in list(_websocket_clients):
        try:
            await asyncio.wait_for(client.send_json(message), timeout=2.0)
        except Exception:
            dead.add(client)
    for c in dead:
        _websocket_clients.discard(c)


async def broadcast_log(level: str, message: str):
    await broadcast_message({
        'type': 'log_message',
        'data': {
            'level':     level.upper(),
            'message':   message,
            'timestamp': get_ist_now().isoformat()
        }
    })


# ── Background loops ──────────────────────────────────────────────────────────

async def websocket_keepalive_loop():
    logger.info("💓 WebSocket keep-alive loop started")
    try:
        while True:
            await asyncio.sleep(30)
            await broadcast_message({
                "type":      "ping",
                "timestamp": get_ist_now().timestamp()
            })
    except asyncio.CancelledError:
        logger.info("💓 WebSocket keep-alive stopped")


async def monitor_xts_socket_status():
    logger.info("🚦 XTS Socket Status Monitor started")
    last_status      = None
    last_data_source = None
    while True:
        try:
            await asyncio.sleep(2)
            cur_status      = state.socket_connected
            cur_data_source = getattr(state, 'data_source', 'MICROSERVICE')
            if cur_status != last_status or cur_data_source != last_data_source:
                await broadcast_message({
                    'type': 'xts_socket_status',
                    'data': {
                        'connected':  cur_status,
                        'dataSource': cur_data_source,
                    }
                })
                last_status      = cur_status
                last_data_source = cur_data_source
        except asyncio.CancelledError:
            logger.info("🚦 XTS Socket Status Monitor stopped")
            break
        except Exception as e:
            logger.error(f"Socket monitor error: {e}")


async def cleanup_old_data():
    logger.info("🧹 Cleanup task started (runs every 6h)")
    try:
        while True:
            await asyncio.sleep(21600)
            if state.db:
                state.db.cleanup_old_orders(days=30)
                logger.info("✅ DB cleanup complete")
    except asyncio.CancelledError:
        pass


def get_live_pnl_data() -> dict:
    """Used by /health API — simple aggregate from DB."""
    try:
        if not state.db:
            return {}
        straddles = state.db.get_active_straddles()
        if not straddles:
            return {
                'total_pnl': 0.0, 'realized_pnl': 0.0,
                'unrealized_pnl': 0.0, 'active_trades': 0
            }
        from trading.pnl_calculator import calculate_aggregate_pnl
        return calculate_aggregate_pnl(straddles, state.prices)
    except Exception as e:
        logger.error(f"❌ Live PnL error: {e}")
        return {
            'total_pnl': 0.0, 'realized_pnl': 0.0,
            'unrealized_pnl': 0.0, 'active_trades': 0
        }


# ── Marketdata bridge: marketdata_service ZMQ PUB → UI clients (5000) ─────────

async def marketdata_bridge_loop():
    from api.websocket import update_chain_header_cache

    pub_port = getattr(config, 'ZMQ_MARKETDATA_PUB_PORT', 5561)
    ctx      = zmq.asyncio.Context.instance()

    while True:
        sub_socket = None
        try:
            sub_socket = ctx.socket(zmq.SUB)
            sub_socket.setsockopt(zmq.RCVTIMEO,      5000)  # 5s — prevents silent hang
            sub_socket.setsockopt(zmq.RECONNECT_IVL, 100)   # 100ms reconnect
            sub_socket.connect(f"tcp://localhost:{pub_port}")

            sub_socket.setsockopt(zmq.SUBSCRIBE, b'price_update')
            sub_socket.setsockopt(zmq.SUBSCRIBE, b'chain_header_update')
            sub_socket.setsockopt(zmq.SUBSCRIBE, b'option_chain_update')  # ✅

            await asyncio.sleep(0.05)   # slow-joiner fix

            logger.info(
                f"📡 Marketdata bridge connected to ZMQ PUB on port {pub_port} "
                f"(topics: price_update, chain_header_update, option_chain_update)"
            )

            while True:
                try:
                    parts = await sub_socket.recv_multipart()
                    if len(parts) < 2:
                        continue

                    msg_type = parts[0].decode('utf-8', errors='ignore')

                    try:
                        msg = json.loads(parts[1].decode('utf-8'))
                    except Exception:
                        continue

                    if msg_type == 'chain_header_update':
                        update_chain_header_cache(msg)
                        await broadcast_message(msg)

                    elif msg_type == 'option_chain_update':
                        # Forward full chain refresh to frontend
                        await broadcast_message(msg)

                    elif msg_type == 'price_update':
                        await broadcast_message(msg)

                    else:
                        await broadcast_message(msg)

                except asyncio.CancelledError:
                    raise
                except zmq.Again:
                    # RCVTIMEO expired — no messages, loop and try again
                    continue
                except Exception as e:
                    logger.error(f"❌ Marketdata bridge recv error: {e}", exc_info=True)
                    await asyncio.sleep(0.1)

        except asyncio.CancelledError:
            logger.info("📡 Marketdata bridge cancelled.")
            break
        except Exception as e:
            logger.warning(f"⚠️ Marketdata bridge disconnected ({e}). Reconnecting in 2s...")
            await asyncio.sleep(2)
        finally:
            if sub_socket:
                try:
                    sub_socket.close(linger=0)
                except Exception:
                    pass


# ── Verification — delegates fully to verifier_service (port 8004) ────────────

async def start_verification_task(
    order_ids: List[str],
    batch_name: str,
    trade_uid: str
) -> bool:
    """Submit order IDs to verifier/reconciler service for async background verification via ZMQ."""
    try:
        socket  = _get_verifier_socket()
        payload = {
            'batch_name': batch_name,
            'order_ids':  order_ids,
            'trade_uid':  trade_uid,
        }
        await socket.send_json(payload)
        logger.info(
            f"📬 Verification queued via ZMQ: {batch_name} "
            f"({len(order_ids)} orders)"
        )
        await broadcast_log(
            'INFO',
            f"[{batch_name}] Queued {len(order_ids)} orders for verification"
        )
        return True
    except Exception as e:
        logger.error(f"❌ Could not submit verification for {batch_name} via ZMQ: {e}", exc_info=True)
    return False


async def verify_orders_task(
    order_ids: List[str],
    batch_name: str = "BATCH",
    trade_uid: str  = ""
) -> Dict:
    """
    Submits a verification job and polls the shared memory state for the result.

    Polling schedule (fast-path first, then slower cadence):
      - First  5 polls: every 0.3s  → resolves instant fills within ~1.5s
      - Next   5 polls: every 0.5s  → catches slower broker ACKs within ~4s total
      - Final  5 polls: every 1.0s  → timeout safety net, ~9s total max wait
    Total max wait: (5×0.3) + (5×0.5) + (5×1.0) = 1.5 + 2.5 + 5.0 = 9.0s
    """
    if not trade_uid:
        uid_prefixes = ('ny', 'sx', 'bn', 'fn', 'mc')
        parts        = batch_name.split('_')
        found_uid    = next((part for part in parts if part.startswith(uid_prefixes)), None)
        trade_uid    = found_uid if found_uid else (parts[1] if len(parts) > 1 else batch_name)

    queued = await start_verification_task(order_ids, batch_name, trade_uid)
    if not queued:
        return {
            'verified_success': [],
            'verified_failed': [
                {'order_id': oid, 'status': 'SUBMIT_FAILED'}
                for oid in order_ids
            ]
        }

    POLL_SCHEDULE = (
        [(0.3, i) for i in range(5)] +
        [(0.5, i) for i in range(5)] +
        [(1.0, i) for i in range(5)]
    )

    order_shm        = None
    verified_success = []
    verified_failed  = []
    still_pending    = list(order_ids)

    try:
        try:
            order_shm = OrderSHM(create=False)
        except FileNotFoundError:
            logger.error("verify_orders_task: OrderSHM not found. Cannot verify orders.")
            return {
                'verified_success': [],
                'verified_failed': [
                    {'order_id': oid, 'status': 'VERIFY_ERROR'} for oid in order_ids
                ]
            }

        for (sleep_s, _) in POLL_SCHEDULE:
            if not still_pending:
                break

            await asyncio.sleep(sleep_s)

            shm_data   = order_shm.read()
            ORDER_DICT = shm_data.get('orders', {})

            next_pending = []
            for oid in still_pending:
                order_info = ORDER_DICT.get(str(oid))
                if not order_info:
                    next_pending.append(oid)
                    continue

                status = order_info.get('status', 'UNKNOWN').upper()
                if status in ('FILLED', 'COMPLETE', 'TRADED', 'EXECUTED'):
                    verified_success.append(order_info.get('raw', {'AppOrderID': oid}))
                elif status in ('CANCELLED', 'REJECTED', 'CANCELED'):
                    verified_failed.append({
                        'order_id': oid,
                        'status':   status,
                        'reason':   order_info.get('raw', {}).get('CancelRejectReason', 'Unknown')
                    })
                else:
                    next_pending.append(oid)

            still_pending = next_pending

        if still_pending:
            logger.warning(
                f"⚠️ Verification timed out for {batch_name} after polling SHM. "
                f"{len(still_pending)} orders unresolved."
            )
            for oid in still_pending:
                verified_failed.append({'order_id': oid, 'status': 'TIMEOUT'})
        else:
            logger.info(
                f"✅ [{batch_name}] Verification complete via SHM: "
                f"{len(verified_success)}/{len(order_ids)} filled"
            )

        # Notify snapshot service on completion
        try:
            snapshot_socket = _get_snapshot_verify_push_socket()
            await snapshot_socket.send_json({
                "command": "verification_complete",
                "data": {
                    "trade_uid":  trade_uid,
                    "batch_name": batch_name,
                    "success":    len(verified_failed) == 0
                }
            })
            logger.debug(f"📬 Pushed verification_complete for {trade_uid} to snapshot service.")
        except Exception as e:
            logger.error(f"❌ Failed to push verification_complete to snapshot service: {e}")

        return {'verified_success': verified_success, 'verified_failed': verified_failed}

    finally:
        if order_shm:
            order_shm.close()


def start_verification_task_sync(
    order_ids: List[str],
    batch_name: str = "BATCH",
    trade_uid: str  = ""
) -> Optional[asyncio.Task]:
    """BACKWARD COMPAT shim for fire-and-forget callers using asyncio.create_task."""
    loop = asyncio.get_event_loop()
    return loop.create_task(
        start_verification_task(order_ids, batch_name, trade_uid)
    )


async def get_verification_result(batch_name: str) -> Optional[Dict]:
    """Poll verifier_service for a completed result."""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            resp = await client.get(f"{VERIFIER_URL}/api/verify/{batch_name}")
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        logger.error(f"❌ Could not fetch verification result: {e}")
    return None


# ── Snapshot trigger — delegates to snapshot_service (port 8003) ──────────────

async def trigger_snapshot_and_broadcast(
    trade_uid:       str,
    trade_data:      dict  = None,
    log_level:       str   = 'INFO',
    bypass_debounce: bool  = False,
):
    """
    ZMQ PUSH to snapshot_service /force-snapshot.
    DEBOUNCED — collapsed into at most 1 request per trade per _SNAPSHOT_DEBOUNCE_S.
    Pass bypass_debounce=True for critical one-shot events (fill confirmed, square-off).
    """
    global _snapshot_debounce
    now  = _time.time()
    last = _snapshot_debounce.get(trade_uid, 0.0)

    if not bypass_debounce and (now - last) < _SNAPSHOT_DEBOUNCE_S:
        return

    _snapshot_debounce[trade_uid] = now

    try:
        snapshot_socket = _get_snapshot_force_push_socket()
        await snapshot_socket.send_json({
            'command': 'force_snapshot',
            'data':    {'trade_uid': trade_uid, 'log_level': log_level}
        })
        logger.info(f"⚡ Force snapshot triggered for {trade_uid} via ZMQ.")
    except Exception as e:
        logger.error(
            f"❌ Could not trigger force snapshot for {trade_uid} via ZMQ: {e}",
            exc_info=True
        )


async def create_snapshot_for_trade(
    trade_uid:  str,
    trade_data: dict = None,
    log_level:  str  = 'DEBUG'
):
    """BACKWARD COMPAT shim — routes to trigger_snapshot_and_broadcast."""
    await trigger_snapshot_and_broadcast(trade_uid, trade_data, log_level)


# ── Snapshot bridge: snapshot_service WS (8003) → UI clients (5000) ──────────

async def snapshot_bridge_loop():
    """
    Connects to snapshot_service WebSocket and forwards all messages to UI clients.
    Reconnects automatically on disconnect.
    """
    import websockets

    snapshot_ws_url = (
        f"ws://localhost:{getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}/ws/snapshots"
    )

    while True:
        try:
            logger.info(f"🔌 Snapshot bridge connecting to {snapshot_ws_url}...")
            async with websockets.connect(snapshot_ws_url, ping_interval=20) as ws:
                logger.info("✅ Snapshot bridge connected — forwarding to UI clients.")
                async for raw in ws:
                    try:
                        message = json.loads(raw)
                        if not hasattr(state, 'trade_snapshots'):
                            state.trade_snapshots = {}

                        if message.get('type') == 'straddle_update':
                            snap_data = message.get('data')
                            if snap_data and 'trade_uid' in snap_data:
                                state.trade_snapshots[snap_data['trade_uid']] = snap_data
                        elif message.get('type') == 'pnl_batch_update':
                            batch_data = message.get('data', [])
                            for snap_data in batch_data:
                                if snap_data and 'trade_uid' in snap_data:
                                    state.trade_snapshots[snap_data['trade_uid']] = snap_data
                    except Exception as e:
                        logger.error(f"Bridge failed to parse/update state: {e}")

                    if not _websocket_clients:
                        continue
                    dead = set()
                    for client in list(_websocket_clients):
                        try:
                            await asyncio.wait_for(client.send_text(raw), timeout=2.0)
                        except Exception:
                            dead.add(client)
                    for c in dead:
                        _websocket_clients.discard(c)

        except asyncio.CancelledError:
            logger.info("🔌 Snapshot bridge cancelled.")
            break
        except Exception as e:
            logger.warning(f"⚠️ Snapshot bridge disconnected ({e}). Reconnecting in 2s...")
            await asyncio.sleep(2)


# ── No-op stubs ───────────────────────────────────────────────────────────────

async def create_trade_snapshots_loop(interval_seconds: float = 0.5):
    """No-op stub. All snapshot computation is in snapshot_service.py (port 8003)."""
    logger.info(
        "📸 Snapshot loop fully delegated to snapshot_service.py (port 8003)."
    )


async def update_order_book_loop():
    """No-op stub. Order book managed by order_book_service.py (port 8002)."""
    logger.info(
        "📋 Order book loop delegated to order_book_service.py (port 8002)."
    )


async def reconciliation_listener():
    """
    Listens for FILLS_UPDATED signal from reconciler.
    On signal: reads OrderSHM → updates in-process state.
    """
    order_shm        = None
    fills_subscriber = None
    try:
        while order_shm is None:
            try:
                order_shm = OrderSHM(create=False)
                logger.info("✅ Fills listener attached to OrderSHM.")
            except FileNotFoundError:
                logger.warning(
                    "Waiting for 'order_shm' to be created by reconciler service... retrying in 2s."
                )
                await asyncio.sleep(2)

        fills_subscriber = FillsSubscriber()
        logger.info("✅ Fills listener started, connected to SHM and ZMQ.")

        while True:
            await fills_subscriber.recv()
            shm_data = order_shm.read()
            if hasattr(state, 'shared_data') and state.shared_data:
                orders   = shm_data.get('orders', {})
                verified = shm_data.get('verified', {})
                state.shared_data.order_book_cache.clear()
                state.shared_data.order_book_cache.update(orders)
                state.shared_data.verified_trades.clear()
                state.shared_data.verified_trades.update(verified)
                logger.debug(
                    f"Synced {len(orders)} orders and {len(verified)} verified trades from SHM"
                )
    except Exception as e:
        logger.error(f"❌ Fills listener error: {e}", exc_info=True)
    finally:
        if order_shm:        order_shm.close()
        if fills_subscriber: fills_subscriber.close()
