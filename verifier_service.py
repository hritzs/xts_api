"""
verifier_service.py — Order Verification Process (ZMQ-based)

Runs as a SEPARATE SUBPROCESS spawned by main.py.

Responsibilities:
- PULL from ZMQ_VERIFIER_PULL_PORT for verification jobs
- REQ to order_book_service for order book data
- PUSH to snapshot_service on ZMQ_SNAPSHOT_PULL_PORT on completion

FIXES:
- Sleep moved to BOTTOM of loop (poll immediately when work pending)
- Persistent ZMQ REQ socket (no recreate per poll)
- attempts only incremented on valid order book fetch
- Reduced poll interval to 0.3s when active (was 2.0s)
- Max attempts raised to 15 (= 15 × 0.3s = 4.5s max wait)
"""
import asyncio
import time
import sys
import os
import zmq
import zmq.asyncio
from typing import List, Dict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from utils.logger import logger
from utils.helpers import get_ist_now
from database.db_manager import Database
from models.state import state

_pending: Dict[str, dict] = {}
_results: Dict[str, dict] = {}

MAX_ATTEMPTS    = 15    # 15 × 0.3s = 4.5s max verification window
ACTIVE_INTERVAL = 0.3   # poll every 300ms when work is pending
IDLE_INTERVAL   = 0.5   # poll every 500ms when nothing pending


async def _fetch_order_book(req_socket: zmq.asyncio.Socket) -> List[dict]:
    """Fetch cached order book from order_book_service via persistent socket."""
    try:
        await req_socket.send_json({"command": "get_order_book"})
        poller = zmq.asyncio.Poller()
        poller.register(req_socket, zmq.POLLIN)
        if await poller.poll(2000):
            response = await req_socket.recv_json()
            return response.get("order_book", [])
        else:
            logger.warning("⚠️ Order book request timed out.")
            return []
    except Exception as e:
        logger.warning(f"Order book fetch failed via ZMQ: {e}")
        return []


async def _notify_snapshot_service(
    push_socket: zmq.asyncio.Socket,
    trade_uid: str,
    batch_name: str,
    result: dict
):
    try:
        await push_socket.send_json({
            "command": "verification_complete",
            "data": {
                "trade_uid":      trade_uid,
                "batch_name":     batch_name,
                "verified_count": len(result.get("verified", [])),
                "failed_count":   len(result.get("failed", [])),
            }
        })
    except Exception as e:
        logger.error(f"Failed to notify snapshot service: {e}")


async def _verification_loop(
    snapshot_push_socket: zmq.asyncio.Socket,
    orderbook_req_socket: zmq.asyncio.Socket
):
    """
    Poll order book and verify pending batches.
    - Sleeps ONLY when idle or after a poll cycle
    - Reuses persistent ZMQ socket
    - Only counts attempt on successful order book fetch
    """
    logger.info(f"🔍 Verifier loop started (active={ACTIVE_INTERVAL}s, idle={IDLE_INTERVAL}s)")

    while True:
        try:
            # ── Idle wait when nothing to do ─────────────────────────────
            if not _pending:
                await asyncio.sleep(IDLE_INTERVAL)
                continue

            # ── Fetch order book (reuse socket) ──────────────────────────
            order_book = await _fetch_order_book(orderbook_req_socket)

            if not order_book:
                # Don't increment attempts — bad fetch shouldn't burn budget
                await asyncio.sleep(ACTIVE_INTERVAL)
                continue

            # ── Build lookup map ─────────────────────────────────────────
            book_map: Dict[str, dict] = {}
            for o in order_book:
                oid = str(o.get("AppOrderID") or o.get("app_order_id") or "")
                if oid:
                    book_map[oid] = o

            completed_batches = []

            for batch_name, batch in list(_pending.items()):
                batch["attempts"] += 1   # only incremented on valid book fetch
                order_ids = batch["order_ids"]
                trade_uid = batch["trade_uid"]

                verified     = []
                failed       = []
                still_pending = []

                for oid in order_ids:
                    order = book_map.get(str(oid))
                    if not order:
                        still_pending.append(oid)
                        continue

                    status = str(
                        order.get("OrderStatus") or order.get("order_status") or ""
                    ).upper()

                    if status in ["FILLED", "COMPLETE", "TRADED", "EXECUTED"]:
                        verified.append({
                            "order_id":   oid,
                            "AppOrderID": oid,
                            "status":     status,
                            "order":      order,
                        })
                        if state.db:
                            try:
                                state.db.update_order_status(oid, status, order)
                            except Exception as db_err:
                                logger.warning(f"DB update failed for {oid}: {db_err}")

                    elif status in ["CANCELLED", "REJECTED", "CANCELED"]:
                        failed.append({
                            "order_id": oid,
                            "status":   status,
                            "reason":   order.get("CancelRejectReason", "Unknown"),
                        })
                    else:
                        # OPEN, PARTIALLYFILLED, PENDINGNEW etc — keep waiting
                        still_pending.append(oid)

                all_resolved = len(still_pending) == 0
                timed_out    = batch["attempts"] >= MAX_ATTEMPTS

                if all_resolved or timed_out:
                    if timed_out and still_pending:
                        elapsed = time.time() - batch["submitted_at"]
                        logger.warning(
                            f"⚠️ [{batch_name}] Timed out after {batch['attempts']} attempts "
                            f"({elapsed:.1f}s) — {len(still_pending)} unresolved orders"
                        )
                        failed += [
                            {
                                "order_id": oid,
                                "status":   "TIMEOUT",
                                "reason":   f"Not found after {MAX_ATTEMPTS} attempts"
                            }
                            for oid in still_pending
                        ]

                    elapsed = time.time() - batch["submitted_at"]
                    result = {
                        "batch_name":  batch_name,
                        "trade_uid":   trade_uid,
                        "timestamp":   get_ist_now().isoformat(),
                        "verified":    verified,
                        "failed":      failed,
                        "total":       len(order_ids),
                        "status":      "complete",
                        "elapsed_sec": round(elapsed, 3),
                    }
                    _results[batch_name] = result
                    completed_batches.append(batch_name)

                    logger.info(
                        f"✅ [{batch_name}] Done in {elapsed:.2f}s | "
                        f"{len(verified)}/{len(order_ids)} filled | "
                        f"{len(failed)} failed | trade={trade_uid}"
                    )
                    await _notify_snapshot_service(
                        snapshot_push_socket, trade_uid, batch_name, result
                    )
                else:
                    logger.debug(
                        f"🔄 [{batch_name}] Attempt {batch['attempts']}/{MAX_ATTEMPTS} | "
                        f"{len(verified)} filled, {len(still_pending)} pending"
                    )

            for b in completed_batches:
                _pending.pop(b, None)

            # Prune old results
            if len(_results) > 500:
                oldest = sorted(_results.keys())[:len(_results) - 500]
                for k in oldest:
                    _results.pop(k, None)

            # ── Short sleep before next poll ──────────────────────────────
            await asyncio.sleep(ACTIVE_INTERVAL)

        except asyncio.CancelledError:
            logger.info("🔍 Verifier loop shutting down.")
            break
        except Exception as e:
            logger.exception(f"❌ Verifier loop error: {e}")
            await asyncio.sleep(2.0)


async def job_receiver(pull_socket: zmq.asyncio.Socket):
    """Pulls verification jobs from ZMQ and queues them in _pending."""
    logger.info(f"📬 Verifier job receiver listening on tcp://*:{config.ZMQ_VERIFIER_PULL_PORT}")
    while True:
        try:
            req = await pull_socket.recv_json()
            _pending[req['batch_name']] = {
                "order_ids":    req['order_ids'],
                "trade_uid":    req['trade_uid'],
                "submitted_at": time.time(),
                "attempts":     0,
            }
            logger.info(
                f"📬 Queued: {req['batch_name']} "
                f"({len(req['order_ids'])} orders, trade={req['trade_uid']})"
            )
        except Exception as e:
            logger.error(f"Error in job receiver: {e}")


async def main():
    state.db = Database()

    ctx = zmq.asyncio.Context()

    pull_socket = ctx.socket(zmq.PULL)
    pull_socket.bind(f"tcp://*:{config.ZMQ_VERIFIER_PULL_PORT}")

    snapshot_push_socket = ctx.socket(zmq.PUSH)
    snapshot_push_socket.connect(f"tcp://localhost:{config.ZMQ_SNAPSHOT_PULL_PORT}")

    # ── Persistent REQ socket — reused every poll cycle ──────────────────
    orderbook_req_socket = ctx.socket(zmq.REQ)
    orderbook_req_socket.connect(f"tcp://localhost:{config.ZMQ_ORDERBOOK_REQ_PORT}")

    receiver_task = asyncio.create_task(job_receiver(pull_socket))
    verifier_task = asyncio.create_task(
        _verification_loop(snapshot_push_socket, orderbook_req_socket)
    )

    logger.info("🔍 Verifier Service is running.")
    await asyncio.gather(receiver_task, verifier_task)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Verifier Service stopped by user.")
