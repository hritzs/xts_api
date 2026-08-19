"""
order_reconciler.py — Order Reconciliation Service (PROCESS 3)

Responsibilities:
  - Polls XTS order book every 2s
  - Reconciles all orders against active trades from DB (CPU-heavy)
  - Complex status resolution + per-trade verification
  - Writes ORDER_DICT + VERIFIED flags to OrderSHM
  - Publishes "FILLS_UPDATED" via ZeroMQ → run_dev wakes up
  - Receives verification job submissions from run_dev via PULL socket
"""
import asyncio
import json
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

import zmq
import zmq.asyncio

import config
import cred
from database.db_manager    import Database
from utils.logger           import logger
from core.shared_memory     import OrderSHM
from core.resilient_task    import resilient_task

TOKEN_DB = os.path.abspath("shared_tokens.db")

# ── Module-level state ────────────────────────────────────────────────────────
_order_shm:   OrderSHM          = None
_db:          Database          = None
_executor:    ThreadPoolExecutor = None
_xt:          object            = None   # XTSConnect interactive instance

# In-memory working dicts — written to SHM after each cycle
_order_dict:  Dict[str, dict] = {}
_verified:    Dict[str, dict] = {}   # batch_name -> result_dict
_pending:     Dict[str, dict] = {}   # batch_name → {order_ids, trade_uid, attempts}


# ── Token loader ──────────────────────────────────────────────────────────────

def _load_token() -> Optional[Dict]:
    try:
        conn   = sqlite3.connect(TOKEN_DB)
        cursor = conn.cursor()
        # --- FIX: Ensure the table exists before trying to read from it ---
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT, timestamp DATETIME)"
        )
        cursor.execute("SELECT value FROM tokens WHERE key = 'xts_interactive_token'")
        row    = cursor.fetchone()
        conn.close()
        return json.loads(row[0]) if row else None
    except Exception as e:
        logger.error(f"❌ Reconciler: token load failed: {e}")
        return None


# ── XTS instance builder ──────────────────────────────────────────────────────

def _build_xt() -> Optional[object]:
    token_data = _load_token()
    if not token_data:
        logger.error("Reconciler: no login token — retrying next cycle")
        return None
    try:
        from Connect import XTSConnect
        xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WebAPI")
        xt._set_common_variables(
            token_data['token'],
            token_data.get('userID'),
            token_data.get('isInvestorClient', False),
        )
        xt.isInvestorClient = False
        logger.info("✅ Reconciler: XTS Interactive instance ready")
        return xt
    except Exception as e:
        logger.error(f"❌ Reconciler: XTS init failed: {e}")
        return None


# ── Order book fetcher ────────────────────────────────────────────────────────

def _fetch_order_book_sync() -> List[dict]:
    """Blocking XTS call — run inside executor."""
    global _xt
    if not _xt:
        _xt = _build_xt()
    if not _xt:
        return []
    try:
        resp = _xt.get_order_book(clientID=cred.clientID)
        if not resp or resp.get('type') != 'success':
            # Safely access 'description' only if resp is not None
            description = resp.get('description') if resp else 'no response'
            logger.warning(f"Reconciler: order book fetch failed — {description}")
            _xt = None   # force re-init next cycle
            return []
        return resp.get('result') or []
    except Exception as e:
        logger.error(f"❌ Reconciler: order book exception: {e}")
        _xt = None
        return []


# ── Order book parser (CPU-heavy) ─────────────────────────────────────────────

def _parse_order_book(raw_orders: List[dict]) -> Dict[str, dict]:
    """
    Parse raw XTS order list → clean ORDER_DICT.
    Runs in executor (CPU work).
    """
    result = {}
    for o in raw_orders:
        oid = str(o.get('AppOrderID') or o.get('app_order_id') or '')
        if not oid:
            continue

        status = str(
            o.get('OrderStatus') or o.get('order_status') or ''
        ).upper()

        result[oid] = {
            'status':          status,
            'avg_price':       float(o.get('OrderAverageTradedPrice') or o.get('order_avg_price') or 0),
            'qty':             int(o.get('CumulativeQuantity') or o.get('cumulative_quantity') or 0),
            'pending_qty':     int(o.get('LeavesQuantity') or o.get('leaves_quantity') or 0),
            'order_unique_id': str(o.get('OrderUniqueIdentifier') or o.get('order_unique_id') or ''),
            'symbol':          str(o.get('TradingSymbol') or o.get('trading_symbol') or ''),
            'side':            str(o.get('OrderSide') or o.get('order_side') or ''),
            'raw':             o,
        }
    return result


# ── Per-trade verifier (parallel) ─────────────────────────────────────────────

def _verify_trade_sync(
    batch_name: str,
    batch:      dict,
    order_dict: Dict[str, dict],
) -> Optional[dict]:
    """
    Verify a single trade batch against the parsed order dict.
    Returns result dict if complete, None if still pending.
    Runs in executor (CPU work).
    """
    order_ids = batch['order_ids']
    trade_uid = batch['trade_uid']
    attempts  = batch['attempts']

    filled       = []
    failed       = []
    still_pending = []

    for oid in order_ids:
        entry = order_dict.get(str(oid))
        if not entry:
            still_pending.append(oid)
            continue
 
        status = entry['status']
        if status in ('FILLED', 'COMPLETE', 'TRADED', 'EXECUTED', 'FULLY EXECUTED'):
            filled.append(oid)
        elif status in ('CANCELLED', 'REJECTED', 'CANCELED'): 
            failed.append({'order_id': oid, 'status': status})
        else:
            still_pending.append(oid)   # OPEN, TRANSIT, NEWREQUEST etc.

    all_done   = len(still_pending) == 0
    timed_out  = attempts >= 8   # 8 × 2s = 16s max wait

    if not (all_done or timed_out):
        return None   # still waiting

    if timed_out and still_pending:
        logger.warning(
            f"⚠ [{batch_name}] timed out — {len(still_pending)} orders unresolved after {attempts} attempts. Attempting to cancel."
        )
        
        # --- NEW: Active cancellation logic for timed-out orders ---
        global _xt
        if not _xt: _xt = _build_xt()

        for oid in still_pending:
            entry = order_dict.get(str(oid))
            if not entry:
                failed.append({'order_id': oid, 'status': 'TIMEOUT_NOT_FOUND', 'reason': 'Order disappeared from book during timeout.'})
                continue

            status = entry.get('status')
            # Only try to cancel orders that are in a cancellable state
            if status in ('OPEN', 'NEW', 'REPLACED', 'PENDINGNEW', 'PARTIALLYFILLED'):
                logger.warning(f"[{batch_name}] Order {oid} timed out with status {status}. Attempting cancel.")
                broker_order = entry.get('raw', {})
                original_uid = broker_order.get('OrderUniqueIdentifier')

                if not original_uid:
                    logger.error(f"[{batch_name}] Cannot cancel {oid}: OrderUniqueIdentifier missing.")
                    failed.append({'order_id': oid, 'status': 'CANCEL_FAILED', 'reason': 'Missing UID'})
                elif not _xt:
                    logger.error(f"[{batch_name}] Cannot cancel {oid}: XTS instance not available.")
                    failed.append({'order_id': oid, 'status': 'CANCEL_FAILED', 'reason': 'XTS instance unavailable'})
                else:
                    try:
                        cancel_response = _xt.cancel_order(appOrderID=oid, orderUniqueIdentifier=original_uid, clientID=cred.clientID)
                        if cancel_response and cancel_response.get('type') == 'success':
                            logger.info(f"[{batch_name}] Cancel successful for {oid}. Marking for re-execution.")
                            failed.append({'order_id': oid, 'status': 'REEXECUTE_NEEDED', 'reason': f'Cancelled due to timeout with status {status}.'})
                        else:
                            error_msg = cancel_response.get('description', 'Cancellation failed')
                            if "not found in OpenOrder List" in error_msg:
                                logger.warning(f"[{batch_name}] Cancel for {oid} failed as it was not found. It may have just filled. Will re-verify.")
                                failed.append({'order_id': oid, 'status': 'TIMEOUT', 'reason': 'Cancel failed (not found), may have filled.'})
                            else:
                                logger.error(f"[{batch_name}] Cancel failed for {oid}: {error_msg}")
                                failed.append({'order_id': oid, 'status': 'CANCEL_FAILED', 'reason': error_msg})
                    except Exception as cancel_e:
                        logger.error(f"[{batch_name}] Exception during cancel for {oid}: {cancel_e}", exc_info=True)
                        failed.append({'order_id': oid, 'status': 'CANCEL_FAILED', 'reason': str(cancel_e)})
            else:
                logger.warning(f"[{batch_name}] Order {oid} timed out with non-cancellable pending status: {status}")
                failed.append({'order_id': oid, 'status': 'TIMEOUT', 'reason': f'Timed out with status {status}'})

    success = len(failed) == 0 and len(filled) == len(order_ids)
    logger.info(
        f"{'✅' if success else '⚠'} [{batch_name}] "
        f"{len(filled)}/{len(order_ids)} filled | "
        f"{len(failed)} failed | trade={trade_uid}"
    )
    return {
        'batch_name': batch_name,
        'trade_uid':  trade_uid,
        'filled':     filled,
        'failed':     failed,
        'success':    success,
        'attempts':   attempts,
    }


# ── Main reconciliation loop ──────────────────────────────────────────────────

async def reconciliation_loop(pub_socket: zmq.asyncio.Socket):
    global _order_dict, _verified

    logger.info("🔄 Reconciliation loop started (2s interval)")
    last_hash    = None
    loop         = asyncio.get_event_loop()

    while True:
        try:
            await asyncio.sleep(2.0)

            # ── 1. Fetch order book in thread ─────────────────────────────────
            raw_orders = await asyncio.wait_for(
                loop.run_in_executor(_executor, _fetch_order_book_sync),
                timeout=10.0
            )
            if not raw_orders:
                continue

            # ── 2. Skip if unchanged ──────────────────────────────────────────
            current_hash = hash(json.dumps(
                [o.get('AppOrderID') for o in raw_orders], sort_keys=True
            ))
            changed = current_hash != last_hash
            last_hash = current_hash

            verification_completed_this_cycle = False

            # ── 3. Parse order book (CPU) in thread ───────────────────────────
            new_order_dict = await loop.run_in_executor(
                _executor, _parse_order_book, raw_orders
            )
            _order_dict = new_order_dict

            # ── 4. Increment pending attempts ─────────────────────────────────
            for b in _pending.values():
                b['attempts'] += 1

            # ── 5. Verify all pending trades in parallel ──────────────────────
            if _pending:
                verify_tasks = [
                    loop.run_in_executor(
                        _executor,
                        _verify_trade_sync,
                        batch_name,
                        batch,
                        _order_dict,
                    )
                    for batch_name, batch in list(_pending.items())
                ]
                results = await asyncio.gather(*verify_tasks, return_exceptions=True)

                completed = []
                for batch_name, result in zip(list(_pending.keys()), results):
                    if isinstance(result, Exception):
                        logger.error(f"❌ Verify error for {batch_name}: {result}")
                        continue
                    if result is None:
                        continue   # still pending
                    verification_completed_this_cycle = True
                    trade_uid = result['trade_uid']
                    # --- FIX: Store the full result dict keyed by batch_name ---
                    _verified[batch_name] = result
                    completed.append(batch_name)

                    # Update DB
                    if _db and result['filled']:
                        for oid in result['filled']:
                            try:
                                order_entry = _order_dict.get(str(oid), {})
                                await loop.run_in_executor(
                                    _executor,
                                    _db.update_order_status,
                                    oid,
                                    order_entry.get('status', 'FILLED'),
                                    order_entry.get('raw', {}),
                                )
                            except Exception as e:
                                logger.warning(f"DB update failed for {oid}: {e}")

                for b in completed:
                    _pending.pop(b, None)

            # ── 6. Write to OrderSHM ──────────────────────────────────────────
            if changed or verification_completed_this_cycle:
                _order_shm.write(_order_dict, _verified)

                # Signal run_dev
                await pub_socket.send_string("FILLS_UPDATED")
                logger.debug(
                    f"📤 SHM updated: {len(_order_dict)} orders, "
                    f"{len(_verified)} verified, "
                    f"{len(_pending)} pending"
                )

        except asyncio.TimeoutError:
            logger.warning("⚠ Reconciler: order book fetch timed out")
        except asyncio.CancelledError:
            logger.info("🛑 Reconciliation loop cancelled")
            break
        except Exception as e:
            logger.exception(f"❌ Reconciler loop error: {e}")
            await asyncio.sleep(5.0)


# ── Job receiver (from run_dev) ───────────────────────────────────────────────

async def job_receiver_loop(pull_socket: zmq.asyncio.Socket):
    """
    Receives verification job submissions from run_dev.
    run_dev sends: {batch_name, order_ids, trade_uid}
    """
    logger.info(
        f"📬 Job receiver listening on port "
        f"{getattr(config, 'ZMQ_VERIFIER_PULL_PORT', 5565)}"
    )
    while True:
        try:
            req = await pull_socket.recv_json()
            batch_name = req.get('batch_name')
            order_ids  = req.get('order_ids', [])
            trade_uid  = req.get('trade_uid')

            if not batch_name or not order_ids or not trade_uid:
                logger.warning(f"⚠ Job receiver: invalid request: {req}")
                continue

            _pending[batch_name] = {
                'order_ids':    order_ids,
                'trade_uid':    trade_uid,
                'submitted_at': time.time(),
                'attempts':     0,
            }
            logger.info(
                f"📬 Job queued: {batch_name} | "
                f"{len(order_ids)} orders | trade={trade_uid}"
            )
        except asyncio.CancelledError:
            logger.info("📬 Job receiver cancelled")
            break
        except Exception as e:
            logger.error(f"❌ Job receiver error: {e}")


# ── Health logger ─────────────────────────────────────────────────────────────

async def health_loop():
    while True:
        logger.info(
            f"💚 order_reconciler alive | "
            f"orders={len(_order_dict)} | "
            f"verified={len(_verified)} | "
            f"pending={len(_pending)}"
        )
        await asyncio.sleep(30)


async def health_check_handler(rep_socket: zmq.asyncio.Socket):
    """Handles ZMQ health check requests from the launcher."""
    health_port = getattr(config, 'ZMQ_RECONCILER_REQ_PORT', 5568)
    logger.info(f"🩺 Health check responder listening on tcp://*:{health_port}")
    while True:
        request = await rep_socket.recv_json()
        if request.get("command") == "health_check":
            await rep_socket.send_json({"success": True, "service": "reconciler"})
        else:
            await rep_socket.send_json({"success": False, "error": "Unknown command"})


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    global _order_shm, _db, _executor

    logger.debug("=" * 80)
    logger.info("🚀 STARTING ORDER RECONCILIATION SERVICE (PROCESS 3)")
    logger.debug("=" * 80)

    # ── Init resources ────────────────────────────────────────────────────────
    _db       = Database()
    _executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="reconciler")
    _order_shm = OrderSHM(create=True)
    logger.info("✅ DB, executor, OrderSHM initialized")

    # ── ZMQ setup ─────────────────────────────────────────────────────────────
    ctx = zmq.asyncio.Context()

    pub_socket  = ctx.socket(zmq.PUB)
    pub_port    = getattr(config, 'ZMQ_FILLS_PUB_PORT', 5564)
    pub_socket.bind(f"tcp://*:{pub_port}")
    logger.info(f"📢 ZMQ PUB bound to tcp://*:{pub_port}")

    pull_socket = ctx.socket(zmq.PULL)
    pull_port   = getattr(config, 'ZMQ_VERIFIER_PULL_PORT', 5565)
    pull_socket.bind(f"tcp://*:{pull_port}")
    logger.info(f"📬 ZMQ PULL bound to tcp://*:{pull_port}")

    health_rep_socket = ctx.socket(zmq.REP)
    health_port = getattr(config, 'ZMQ_RECONCILER_REQ_PORT', 5568)
    health_rep_socket.bind(f"tcp://*:{health_port}")

    logger.debug("=" * 80)
    logger.info("✅ ORDER RECONCILER READY")
    logger.info(f"   PUB  port (fills signal): {pub_port}")
    logger.info(f"   PULL port (job inbox):     {pull_port}")
    logger.info(f"   HEALTH REQ port:           {health_port}")
    logger.debug("=" * 80)

    # ── Run all loops supervised ──────────────────────────────────────────────
    try:
        await asyncio.gather(
            resilient_task("reconciliation_loop", reconciliation_loop, pub_socket),
            resilient_task("job_receiver",        job_receiver_loop,   pull_socket),
            resilient_task("health_loop",         health_loop),
            resilient_task("health_check_handler", health_check_handler, health_rep_socket),
        )
    except asyncio.CancelledError:
        logger.info("🛑 Main gather cancelled")
    finally:
        logger.info("🛑 Shutting down ORDER RECONCILER...")
        _executor.shutdown(wait=False)
        try:
            pub_socket.close(linger=0)
            pull_socket.close(linger=0)
            health_rep_socket.close(linger=0)
            ctx.term()
        except Exception:
            pass
        if _order_shm:
            _order_shm.close(unlink=True)
        if _db:
            try:
                _db.close()
            except Exception:
                pass
        logger.info("✅ ORDER RECONCILER shutdown complete")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Order Reconciler stopped by user")
