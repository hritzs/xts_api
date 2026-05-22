"""
snapshot_service.py — Full-Compute Snapshot Process (Port 8003)

Runs as a SEPARATE SUBPROCESS.

Responsibilities:
  1. Every 1s: fetch active trades from DB
  2. For each trade: fetch filled orders, bulk LTP, compute PnL/Greeks/IV
  3. Broadcast pnl_batch_update  → table rows in UI
  4. Broadcast straddle_update   → detail panel
  5. PULL from ZMQ_SNAPSHOT_FORCE_PULL_PORT for on-demand recompute
  6. PULL from ZMQ_SNAPSHOT_PULL_PORT for verification completion notifications
  7. GET  /api/snapshots (for debugging)
  8. WS   /ws/snapshots (for UI clients)
"""
import asyncio
import math
import time
import sys
import os
import zmq
import zmq.asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Set

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from utils.logger import logger
from utils.greeks import calculate_all_greeks, calculate_greeks_from_iv
from utils.helpers import calculate_dte, get_ist_now
from market_data import SYMBOL_CONFIG, data_client as md_client
from database.db_manager import Database
from models.state import state
from core.shared_memory import ChainSHM

# Module-level declarations for ZMQ sockets and queue to ensure they are always defined
# and accessible by all functions in this module.
_verify_pull_socket: Optional[zmq.asyncio.Socket] = None
_force_pull_socket: Optional[zmq.asyncio.Socket] = None
_force_queue: Optional[asyncio.Queue] = None


async def _pre_warm_chain_task():
    """Background task to pre-warm the NIFTY chain without blocking startup."""
    logger.info("🔥 Pre-warming NIFTY option chain in background...")
    chain = None
    for i in range(5): # Retry up to 5 times to handle startup race conditions
        chain = await _fetch_option_chain("NIFTY")
        if chain:
            break
        logger.warning(f"Pre-warm failed (attempt {i+1}/5). Retrying in 2s...")
        await asyncio.sleep(2)

    if chain:
        logger.info(f"✅ NIFTY chain ready — fut_ltp={chain.get('fut_ltp')}")
    else:
        logger.error("❌ FAILED to pre-warm NIFTY chain after multiple attempts. Service may be unstable.")

# ── Lifespan (defined BEFORE app so we can pass it in) ───────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("="*80)
    logger.info("🚀 STARTING SNAPSHOT SERVICE")
    logger.info("="*80)

    state.db = Database()
    logger.info("✅ Database initialized")

    try:
        from utils.shared_data import SharedDataManager
        state.shared_data   = SharedDataManager(create=False)
        state.prices        = state.shared_data.prices_array # This uses named SHM and is OK
        logger.info("✅ Attached to shared memory for prices.")
        # Initialize a local dict for option chains. The proxy from main.py is not accessible.
        
        # --- NEW: Attach to ChainSHM for non-blocking chain reads ---
        state.chain_shms = {}
        for sym in SYMBOL_CONFIG.keys():
            try:
                state.chain_shms[sym] = ChainSHM(sym, create=False)
                logger.info(f"✅ Attached to ChainSHM for {sym}")
            except Exception:
                pass # Might not exist yet, will try in loop

        state.option_chains = {}
        logger.info("✅ Initialized local cache for option chains.")
    except Exception as e:
        logger.warning(f"⚠️  Shared memory attach failed ({e}) — using local fallback")
        if not hasattr(state, "option_chains"):
            state.option_chains = {}


    # ── ZMQ Sockets & Listeners ───────────────────────────────────────────────
    ctx = zmq.asyncio.Context()
    
    # PULL socket for verification completion notifications
    global _verify_pull_socket, _force_pull_socket, _force_queue

    _verify_pull_socket = ctx.socket(zmq.PULL)
    _verify_pull_socket.bind(f"tcp://*:{config.ZMQ_SNAPSHOT_PULL_PORT}")
    
    # PULL socket for force-snapshot requests
    _force_pull_socket = ctx.socket(zmq.PULL)
    _force_pull_socket.bind(f"tcp://*:{config.ZMQ_SNAPSHOT_FORCE_PULL_PORT}")

    # Queue for force snapshot requests
    _force_queue = asyncio.Queue()

    # ── Background Tasks ──────────────────────────────────────────────────────
    bg_tasks = [
        asyncio.create_task(_pre_warm_chain_task()), # Run pre-warm in background
        asyncio.create_task(_snapshot_compute_loop()), # This task will now get _force_queue from global
        asyncio.create_task(_verification_listener(_verify_pull_socket)), # Use global variable
        asyncio.create_task(_force_snapshot_listener(_force_pull_socket)), # Use global variable
    ]

    logger.info("="*80)
    logger.info(f"✅ SNAPSHOT SERVICE READY on port {getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}")
    logger.info(f"   WS:  ws://localhost:{getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}/ws/snapshots")
    logger.info("="*80)

    yield   # ← service runs here

    logger.info("🛑 Shutting down Snapshot Service...")
    for task in bg_tasks:
        task.cancel()
    try:
        await asyncio.gather(*bg_tasks, return_exceptions=True)
    except asyncio.CancelledError: # Catching CancelledError here is fine, it's expected during shutdown
        pass
    
    if _verify_pull_socket: # Check if initialized before closing
        _verify_pull_socket.close()
    if _force_pull_socket: # Check if initialized before closing
        _force_pull_socket.close()
    ctx.term()
    logger.info("✅ Snapshot Service shutdown complete")


# ── App created AFTER lifespan, BEFORE any route decorators ──────────────────

app = FastAPI(title="Snapshot Service", version="3.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"]
)


# ── Service-level state ───────────────────────────────────────────────────────
_clients:          List[WebSocket]  = []
_latest_snapshots: Dict[str, dict]  = {}
_total_cycles:     int              = 0
_last_push_time:   float            = 0.0
_last_log_time:    float            = 0.0
_force_queue:      asyncio.Queue    = None

# Debounce: suppress repeated force-snapshot requests for same trade_uid
# within this window (prevents price-tick flood from saturating the loop)
_FORCE_DEBOUNCE_S: float = 0.5
_force_last_fired: Dict[str, float] = {}

# ── IV normalisation ──────────────────────────────────────────────────────────

def _normalise_iv(v: float) -> float:
    """Convert decimal IV → % exactly once. Values >= 2.0 assumed already in %."""
    if v <= 0:
        return 0.0
    return round(v * 100.0, 4) if v < 2.0 else round(v, 4)


# ── WebSocket helpers ─────────────────────────────────────────────────────────

async def _send_safe(ws: WebSocket, msg: dict) -> bool:
    try:
        await asyncio.wait_for(ws.send_json(msg), timeout=2.0)
        return True
    except Exception:
        return False


async def _broadcast(msg: dict) -> List[WebSocket]:
    dead = []
    for ws in list(_clients):
        if not await _send_safe(ws, msg):
            dead.append(ws)
    return dead


def _cleanup_dead(dead: List[WebSocket]):
    for c in set(dead):
        if c in _clients:
            _clients.remove(c)


# ── WebSocket endpoint ────────────────────────────────────────────────────────

@app.websocket("/ws/snapshots")
async def ws_endpoint(websocket: WebSocket):
    await websocket.accept()
    _clients.append(websocket)
    logger.info(f"✅ Snapshot WS client connected. Total: {len(_clients)}")
    # Push current state immediately on connect so UI doesn't wait 1s
    if _latest_snapshots:
        await _send_safe(websocket, {
            "type": "pnl_batch_update",
            "data": list(_latest_snapshots.values())
        })
    try:
        while True:
            await websocket.receive_text()
    except (WebSocketDisconnect, Exception):
        pass
    finally:
        if websocket in _clients:
            _clients.remove(websocket)
        logger.info(f"🔌 Snapshot WS disconnected. Total: {len(_clients)}")


# ── REST endpoints ────────────────────────────────────────────────────────────

async def _queue_force_snapshot(trade_uid: str):
    """Helper to queue a force snapshot request, handling debounce."""
    global _force_queue, _force_last_fired
    now = time.time()
    last = _force_last_fired.get(trade_uid, 0.0)

    if now - last < _FORCE_DEBOUNCE_S:
        return # Suppress

    _force_last_fired[trade_uid] = now
    if _force_queue:
        await _force_queue.put(trade_uid)

async def _verification_listener(pull_socket: zmq.asyncio.Socket):
    """Listens for verification completion messages from main app."""
    logger.info(f"📬 Verification listener running on PULL socket {config.ZMQ_SNAPSHOT_PULL_PORT}")
    while True:
        msg = await pull_socket.recv_json()
        if msg.get("command") == "verification_complete":
            trade_uid = msg.get("data", {}).get("trade_uid")
            if trade_uid and _force_queue:
                _force_last_fired.pop(trade_uid, None) # Bypass debounce
                await _force_queue.put(trade_uid)

async def _force_snapshot_listener(pull_socket: zmq.asyncio.Socket):
    """Listens for force snapshot requests from other services (e.g., main app)."""
    logger.info(f"📬 Force snapshot listener running on PULL socket {config.ZMQ_SNAPSHOT_FORCE_PULL_PORT}")
    while True:
        msg = await pull_socket.recv_json()
        if msg.get("command") == "force_snapshot":
            data = msg.get("data", {})
            trade_uid = data.get("trade_uid")
            
            if trade_uid:
                # --- FIX: Extract and cache trade_data if provided in the payload ---
                # This allows the sender (e.g., square_off) to provide the absolute latest state,
                # bypassing DB lag and ensuring the UI shows the correct status/PnL immediately.
                trade_data = data.get("trade_data")
                if trade_data:
                    if not hasattr(state, 'trade_data_cache'):
                        state.trade_data_cache = {}
                    state.trade_data_cache[trade_uid] = {'data': trade_data, 'timestamp': time.time()}
                    logger.info(f"📬 Received force_snapshot with fresh trade_data for {trade_uid}. Cached.")
                
                await _queue_force_snapshot(trade_uid)

@app.get("/api/snapshots")
async def get_all_snapshots():
    return {"snapshots": list(_latest_snapshots.values())}


@app.get("/api/snapshots/{trade_uid}")
async def get_snapshot(trade_uid: str):
    snap = _latest_snapshots.get(trade_uid)
    if not snap:
        raise HTTPException(404, f"Snapshot not found for {trade_uid}")
    return snap


@app.get("/health")
@app.get("/api/health")
async def health():
    return {
        "status":         "ok",
        "service":        "Snapshot",
        "clients":        len(_clients),
        "total_cycles":   _total_cycles,
        "cached_trades":  len(_latest_snapshots),
        "last_push_age_s": (
            round(time.time() - _last_push_time, 1) if _last_push_time else None
        ),
    }


# ── Chain fetcher with active retry ──────────────────────────────────────────

async def _fetch_option_chain(symbol: str) -> Optional[dict]:
    """
    Fetch chain from shared memory (ChainSHM).
    This is a non-blocking memory read, avoiding HTTP/ZMQ latency.
    """
    # 1. Try to get/init the SHM reader
    shm = getattr(state, 'chain_shms', {}).get(symbol)
    if not shm:
        try:
            state.chain_shms[symbol] = ChainSHM(symbol, create=False)
            shm = state.chain_shms[symbol]
            logger.debug(f"Dynamically attached to ChainSHM for {symbol}")
        except Exception:
            pass # Still not available

    # 2. Read from SHM if available
    if shm:
        try:
            chain_data = shm.read()
            if chain_data and chain_data.get("fut_ltp"):
                # Update local cache
                if not hasattr(state, "option_chains"): state.option_chains = {}
                state.option_chains[symbol] = chain_data
                return chain_data
        except Exception as e:
            logger.error(f"Failed to read ChainSHM for {symbol}: {e}")

    # 3. Fallback: Check local cache (in case SHM failed but we have old data)
    cached = state.option_chains.get(symbol)
    if cached and cached.get("fut_ltp"):
        return cached

    # 4. Last Resort: HTTP Call (Only if absolutely necessary, e.g. startup)
    try:
        data = await md_client.get_option_chain_from_service(symbol)
        if data and data.get("fut_ltp"):
            # logger.info(f"✅ [Snapshotter] Chain loaded via HTTP for {symbol}")
            if hasattr(state, "option_chains"):
                state.option_chains[symbol] = data
            return data
    except Exception as e:
        pass

    # logger.warning(f"[Snapshotter] Chain not ready for {symbol} — skipping cycle")
    return None


def _order_belongs_to_trade(order: dict, trade_uid: str) -> bool:
    """
    Checks all known UID field names in priority order.
    Handles DB format: order_unique_id = "BUI_ny050326123100a_CE_LOT1"
    Accounts for XTS truncating OrderUniqueIdentifier to 20 chars.
    """
    base_uid = trade_uid[:-1] if len(trade_uid) > 1 else trade_uid
    for field in (
        "order_unique_id",        # ← DB snake_case (confirmed present)
        "OrderUniqueIdentifier",  # broker PascalCase (confirmed None, but safe fallback)
        "UniqueIdentifier",
        "unique_identifier",
    ):
        val = order.get(field) or ""
        if val:
            if trade_uid in val:
                return True
            if base_uid in val:
                return True
    return False


# ── Core snapshot computation ─────────────────────────────────────────────────

async def _compute_snapshot(trade_uid: str) -> Optional[dict]:
    """
    Full snapshot computation for one trade.
    Reads from DB + shared price memory, computes everything here.
    """
    try:
        db: Database = state.db
        loop = asyncio.get_event_loop()

        # ── Trade data ────────────────────────────────────────────────────────
        # --- FIX: Check for a short-term cached version of the trade data first ---
        # This mitigates race conditions where this service reads the DB before a
        # write from another process (e.g., partial_square_off) is visible.
        cached_trade_data = None
        if hasattr(state, 'trade_data_cache') and state.trade_data_cache:
            cached_entry = state.trade_data_cache.get(trade_uid)
            # Use cache if it's less than 5 seconds old
            if cached_entry and (time.time() - cached_entry.get('timestamp', 0)) < 5.0:
                cached_trade_data = cached_entry.get('data')
                logger.info(f"[{trade_uid}] Using recently cached trade data for snapshot.")

        full_trade = cached_trade_data or await loop.run_in_executor(None, db.get_straddle_by_id, trade_uid)

        if not full_trade:
            # If trade is not found, it might have been deleted or is no longer active.
            # We should remove it from our local caches to prevent stale data.
            if _latest_snapshots.pop(trade_uid, None):
                logger.info(f"[{trade_uid}] Removed from snapshot cache as it's no longer in DB.")
            if hasattr(state, "trade_snapshots") and getattr(state, 'trade_snapshots', None):
                state.trade_snapshots.pop(trade_uid, None)
            return None

        # --- NEW ROBUST CHECK ---
        # A trade is only ready for a full snapshot if it has been built,
        # meaning it has a status other than PENDING and has valid tokens.
        status = full_trade.get("status")
        ce_token_val = full_trade.get("ce_token")
        pe_token_val = full_trade.get("pe_token")

        is_pending = (status == "PENDING")
        has_no_tokens = not ce_token_val or not pe_token_val

        if is_pending or has_no_tokens:
            # Only log if it's an unexpected state (e.g. ACTIVE but no tokens)
            if not is_pending and has_no_tokens:
                logger.warning(f"Snapshot for {trade_uid} aborted: Status is '{status}' but tokens are missing.")
            return None

        if "straddle_id" not in full_trade:
            full_trade["straddle_id"] = trade_uid

        symbol   = full_trade.get("symbol", "NIFTY").upper()
        lot_size = full_trade.get("lot_size") or 0

        # ── Option chain ──────────────────────────────────────────────────────
        option_chain = await _fetch_option_chain(symbol)
        if not option_chain or not option_chain.get("fut_ltp"):
            return None   # warning already logged in _fetch_option_chain

        if not lot_size or lot_size <= 0:
            lot_size = option_chain.get("lot_size") or 65

        base_sym = next(
            (k for k in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True)
             if k in symbol), None
        )
        segment = (
            SYMBOL_CONFIG.get(base_sym, {}).get("segment", 2)
            if base_sym else full_trade.get("exchange_segment", 2)
        )

        # ── Robust token resolution ───────────────────────────────────────────
        def _resolve_token(token_val, symbol_val, otype):
            if not token_val:
                return None
            try:
                return int(token_val)
            except (ValueError, TypeError):
                logger.warning(f"⚠️ Snapshot: {otype}_token for {trade_uid} is not an integer ('{token_val}'). Attempting lookup by symbol.")
                # Use the symbol from the trade data if available, otherwise use the invalid token value itself
                lookup_symbol = full_trade.get(f"{otype.lower()}_symbol") or token_val
                for row in option_chain.get("chain", []):
                    if row.get(f"{otype.lower()}_symbol") == lookup_symbol:
                        resolved_tok = row.get(f"{otype.lower()}_token")
                        if resolved_tok:
                            logger.info(f"✅ Resolved {otype} token for '{lookup_symbol}' to {resolved_tok}")
                            return int(resolved_tok)
                
                logger.error(f"❌ Snapshot: Failed to resolve {otype} token for '{lookup_symbol}' from option chain. Using 0.")
                return 0 # Ensure it's an integer

        ce_tok = _resolve_token(full_trade.get("ce_token"), full_trade.get("ce_symbol"), "CE")
        pe_tok = _resolve_token(full_trade.get("pe_token"), full_trade.get("pe_symbol"), "PE")

        if ce_tok is None or pe_tok is None:
            logger.error(f"❌ Snapshot: Could not resolve original CE/PE tokens for {trade_uid}. Aborting snapshot.")
            return None

        # ── Live synthetic spot (Put-Call Parity) ─────────────────────────────
        cash_spot = option_chain.get("fut_ltp", 0.0) # This is the cash index price
        synthetic_spot = cash_spot # Default to cash price
        try:
            atm_s = option_chain.get("atm")
            gap = option_chain.get("gap", 50) # Get gap for sanity check
            atm_row = next(
                (r for r in option_chain.get("chain", []) if r.get("strike") == atm_s),
                None
            )
            if atm_row and atm_row.get("ce_token") and atm_row.get("pe_token"):
                ce_p = state.get_price(int(atm_row["ce_token"])) or atm_row.get("ce_ltp", 0.0)
                pe_p = state.get_price(int(atm_row["pe_token"])) or atm_row.get("pe_ltp", 0.0)
                
                if ce_p > 0 and pe_p > 0:
                    # Simplified Put-Call Parity for futures/indices: S = K + C - P
                    K = float(atm_s)
                    C = float(ce_p)
                    P = float(pe_p)
                    
                    calculated_synthetic = K + C - P
                    
                    # Sanity check: ensure the calculated spot is reasonably close to the cash index price
                    if cash_spot > 0 and abs(calculated_synthetic - cash_spot) / cash_spot < 0.01: # within 1%
                        synthetic_spot = calculated_synthetic
                        logger.debug(f"[{trade_uid}] Using Put-Call Parity spot: {synthetic_spot:.2f} (Cash Index: {cash_spot:.2f})")
                    else:
                        logger.debug(f"[{trade_uid}] Put-Call Parity spot ({calculated_synthetic:.2f}) differs >1% from cash index ({cash_spot:.2f}). Using cash index for synthetic.")
        except Exception as e:
            logger.warning(f"[{trade_uid}] Error calculating synthetic spot: {e}. Using cash index LTP.")
            synthetic_spot = cash_spot

        # ── Filled orders ─────────────────────────────────────────────────────
        all_orders = await loop.run_in_executor(
            None, db.get_orders_by_trade_id, trade_uid
        )
        # REPLACE WITH — single combined filter using correct field priority:
        filled_orders = [
            o for o in all_orders
            if str(o.get("order_status", "") or o.get("OrderStatus", "")).upper()
            in ["FILLED", "COMPLETE", "TRADED", "EXECUTED"]
            and _order_belongs_to_trade(o, trade_uid)
        ]
        # ── Aggregate per token ───────────────────────────────────────────────
        all_tokens: Set[int] = set()
        agg: Dict[int, dict] = {}
        for o in filled_orders:
            tv = o.get("exchange_instrument_id") or o.get("ExchangeInstrumentID")
            if not tv:
                continue
            token = int(tv)
            qty   = int(o.get("cumulative_quantity") or o.get("CumulativeQuantity", 0))
            price = float(o.get("order_avg_price") or o.get("OrderAverageTradedPrice", 0))
            side  = str(o.get("order_side") or o.get("OrderSide", "")).upper()
            all_tokens.add(token)
            agg.setdefault(token, {
                "buy_qty": 0, "buy_value": 0.0,
                "sell_qty": 0, "sell_value": 0.0
            })
            if side == "BUY":
                agg[token]["buy_qty"]   += qty
                agg[token]["buy_value"] += qty * price
            elif side == "SELL":
                agg[token]["sell_qty"]   += qty
                agg[token]["sell_value"] += qty * price

        # ── BULK LTP (Optimized: Read from SHM, do not call HTTP) ──
        price_map: Dict[int, float] = {}
        if all_tokens:
            # Direct SHM read is 100x faster than HTTP
            for t in all_tokens:
                p = state.get_price(t)
                if p and p > 0:
                    price_map[t] = p

        # ── PnL pool ──────────────────────────────────────────────────────────
        total_pnl_pool = 0.0
        for tok, a in agg.items():
            net  = a["sell_qty"] - a["buy_qty"]
            ltp  = price_map.get(tok, 0.0)
            total_pnl_pool += (
                (a["sell_value"] - a["buy_value"]) - (net * ltp if ltp > 0 else 0)
            )

        total_realized   = full_trade.get("realized_pnl", 0.0)
        total_unrealized = total_pnl_pool - total_realized
        total_pnl        = total_realized + total_unrealized

        # ── token → (strike, otype) + IV lookup from chain ───────────────────
        chain_rows  = option_chain.get("chain", [])
        tok_to_leg: Dict[int, tuple]  = {}
        iv_lookup:  Dict[int, float]  = {}
        for row in chain_rows:
            for side_key, otype in [("ce_token", "CE"), ("pe_token", "PE")]:
                tok = row.get(side_key)
                if tok:
                    tok = int(tok)
                    tok_to_leg[tok] = (row["strike"], otype)
                    iv_lookup[tok]  = float(row.get(f"{side_key[:2]}_iv") or 0)

        dte = option_chain.get("dte", 0)

        # ── Per-token: Greeks + live_positions ────────────────────────────────
        live_positions: List[dict] = []
        net_delta = net_gamma = net_theta = net_vega = 0.0
        pnl_by_token: Dict[int, float] = {}

        for tok in all_tokens:
            a        = agg[tok]
            net_open = a["sell_qty"] - a["buy_qty"]
            ltp      = price_map.get(tok, 0.0)
            tok_pnl  = (a["sell_value"] - a["buy_value"]) - (net_open * ltp)
            pnl_by_token[tok] = tok_pnl

            if net_open == 0:
                continue

            leg = tok_to_leg.get(tok)
            if not leg:
                continue
            strike, otype = leg

            action = "SELL" if net_open > 0 else "BUY"
            if net_open > 0:
                ep      = a["sell_value"] / a["sell_qty"] if a["sell_qty"] > 0 else 0.0
                leg_pnl = (ep - ltp) * net_open if ltp > 0 else 0.0
            else:
                ep      = a["buy_value"] / a["buy_qty"] if a["buy_qty"] > 0 else 0.0
                leg_pnl = (ltp - ep) * abs(net_open) if ltp > 0 else 0.0

            # Greeks — _normalise_iv applied EXACTLY ONCE
            g: dict = {}
            if ltp > 0:
                g = calculate_all_greeks(
                    otype.lower(), strike, synthetic_spot, dte, ltp, 0.0
                )
            raw_iv = g.get("iv", 0)
            if raw_iv > 0:
                g["iv"] = _normalise_iv(raw_iv)
            elif iv_lookup.get(tok, 0) > 0:
                civ = iv_lookup[tok]
                g   = calculate_greeks_from_iv(
                    otype.lower(), strike, synthetic_spot, dte, civ / 100.0, 0.0
                )
                g["iv"] = civ
            else:
                g = {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0, "iv": 0.0}

            sign = 1 if action == "BUY" else -1
            nd = g["delta"] * abs(net_open) * sign
            ng = g["gamma"] * abs(net_open) * sign
            nt = g["theta"] * abs(net_open) * sign
            nv = g["vega"]  * abs(net_open) * sign
            net_delta += nd
            net_gamma += ng
            net_theta += nt
            net_vega  += nv

            live_positions.append({
                "token":       tok,
                "strike":      strike,
                "option_type": otype,
                "quantity":    abs(net_open),
                "action":      action,
                "entry_price": ep,
                "ltp":         ltp,
                "pnl":         leg_pnl,
                "iv":          g.get("iv", 0.0),
                "delta":       nd,
                "gamma":       ng,
                "theta":       nt,
                "vega":        nv,
            })

        # Fallback: no filled orders → use DB entry prices
        if not filled_orders:
            # Use the robustly resolved tokens from the top of the function.
            for tok, otype, qty_f, ep_f in [
                (ce_tok, "CE", "ce_quantity", "ce_entry_price"),
                (pe_tok, "PE", "pe_quantity", "pe_entry_price"),
            ]:
                if not tok:
                    continue
                qty   = full_trade.get(qty_f, 0)
                ep    = full_trade.get(ep_f, 0.0)
                ltp_f = state.get_price(tok) or 0.0
                pnl_f = (ep - ltp_f) * qty if ep > 0 and ltp_f > 0 else 0.0
                total_unrealized += pnl_f
                total_pnl        += pnl_f
                g_row = next(
                    (r for r in chain_rows
                     if r.get(f"{otype.lower()}_token") == tok), {}
                )
                sp = otype.lower()
                live_positions.append({
                    "token":       tok,
                    "strike":      full_trade.get("strike"),
                    "option_type": otype,
                    "quantity":    qty,
                    "action":      "SELL",
                    "entry_price": ep,
                    "ltp":         ltp_f,
                    "pnl":         pnl_f,
                    "iv":          float(g_row.get(f"{sp}_iv")    or 0),
                    "delta":       float(g_row.get(f"{sp}_delta") or 0),
                    "gamma":       float(g_row.get(f"{sp}_gamma") or 0),
                    "theta":       float(g_row.get(f"{sp}_theta") or 0),
                    "vega":        float(g_row.get(f"{sp}_vega")  or 0),
                })

        # ── Live ATM IV ───────────────────────────────────────────────────────
        gap        = option_chain.get("gap") or 50
        atm_strike = int(round(synthetic_spot / gap) * gap)
        atm_row    = next(
            (r for r in chain_rows if r.get("strike") == atm_strike), None
        )
        live_atm_iv = 0.0
        if atm_row:
            vals = [
                float(atm_row.get(x) or 0)
                for x in ["ce_iv", "pe_iv"]
                if float(atm_row.get(x) or 0) > 0
            ]
            live_atm_iv = sum(vals) / len(vals) if vals else 0.0

        avg_iv = live_atm_iv if live_atm_iv > 0 else (
            sum(p.get("iv", 0) for p in live_positions) / len(live_positions)
            if live_positions else 0.0
        )

        # ── Hedge / Roll params ───────────────────────────────────────────────
        pts_out        = abs(net_delta) / abs(net_gamma) if abs(net_gamma) > 1e-6 else 0.0
        cfg            = full_trade.get("config") or {}
        points_allowed = float("inf")
        roll_trigger   = 0.0
        atm_straddle   = 0.0
        try:
            if atm_row:
                ce_p = (
                    state.get_price(int(atm_row.get("ce_token") or 0))
                    or float(atm_row.get("ce_ltp") or 0)
                )
                pe_p = (
                    state.get_price(int(atm_row.get("pe_token") or 0))
                    or float(atm_row.get("pe_ltp") or 0)
                )
                atm_straddle = ce_p + pe_p
            hd     = cfg.get("hedge_div", 57)
            sd     = cfg.get("straddle_div", 4)
            try:
                rd = float(cfg.get("roll_straddle_div", 0.2))
                if rd == 2.0: rd = 0.2
            except (ValueError, TypeError):
                rd = 0.2
            iv_dec = avg_iv / 100.0
            sb     = atm_straddle / sd if sd > 0 and atm_straddle > 0 else float("inf")
            ib     = (synthetic_spot * iv_dec) / hd if hd > 0 and iv_dec > 0 else float("inf")
            points_allowed = min(sb, ib)
            roll_trigger   = atm_straddle / rd if rd > 0 and atm_straddle > 0 else 0.0
        except Exception:
            pass

        # ── PnL per straddle unit (CONTRACT PAIRS not lots) ───────────────────
        net_ce = sum(
            p["quantity"] if p["action"] == "SELL" else -p["quantity"]
            for p in live_positions if p["option_type"] == "CE"
        )
        net_pe = sum(
            p["quantity"] if p["action"] == "SELL" else -p["quantity"]
            for p in live_positions if p["option_type"] == "PE"
        )
        units = math.ceil((abs(net_ce) + abs(net_pe)) / 2.0)
        if units < 1:
            units = math.ceil(
                (full_trade.get("ce_quantity", 0) + full_trade.get("pe_quantity", 0))
                / 2.0
            )
        pnl_per_straddle = total_unrealized / units if units > 0 else 0.0

        # ── SL threshold ──────────────────────────────────────────────────────
        sl_bps       = cfg.get("sl_bps", 14)
        sl_pts       = (synthetic_spot * sl_bps) / 10000 if synthetic_spot > 0 else 0.0
        sl_threshold = -1 * sl_pts * units

        # ── Per-leg summary ───────────────────────────────────────────────────
        ce_tok_int = ce_tok # Already resolved to an integer
        pe_tok_int = pe_tok # Already resolved to an integer
        ce_pnl = pnl_by_token.get(ce_tok_int, 0.0)
        pe_pnl = pnl_by_token.get(pe_tok_int, 0.0)
        ce_iv = ce_delta = pe_iv = pe_delta = 0.0
        for p in live_positions:
            if p["token"] == ce_tok_int:
                ce_iv, ce_delta = p["iv"], p["delta"]
            elif p["token"] == pe_tok_int:
                pe_iv, pe_delta = p["iv"], p["delta"]

        days_to_expiry = -1
        expiry_val = full_trade.get("expiry")
        try:
            if expiry_val and expiry_val != "N/A":
                days_to_expiry = calculate_dte(expiry_val)
        except Exception: # Catch any error from calculate_dte, like an invalid format
            pass

        # ── Assemble final snapshot ───────────────────────────────────────────
        snap = {
            "trade_uid":          trade_uid,
            "timestamp":          get_ist_now().isoformat(),
            "symbol":             full_trade.get("symbol"),
            "strike":             full_trade.get("strike"),
            "status":             full_trade.get("status"),
            "ce_quantity":        full_trade.get("ce_quantity", 0),
            "pe_quantity":        full_trade.get("pe_quantity", 0),
            "entry_spot":         full_trade.get("entry_spot"),
            "lot_size":           lot_size,
            "total_pnl":          total_pnl,
            "realized_pnl":       total_realized,
            "unrealized_pnl":     total_unrealized,
            "pnl_per_straddle":   pnl_per_straddle,
            "net_delta":          net_delta,
            "net_gamma":          net_gamma,
            "net_theta":          net_theta,
            "net_vega":           net_vega,
            "avg_iv":             avg_iv,
            "ce_pnl":             ce_pnl,
            "pe_pnl":             pe_pnl,
            "ce_iv":              ce_iv,
            "pe_iv":              pe_iv,
            "ce_delta":           ce_delta,
            "pe_delta":           pe_delta,
            "pts_out":            pts_out,
            "points_allowed":     None if points_allowed == float("inf") else points_allowed,
            "roll_trigger_price": roll_trigger,
            "sl_threshold":       sl_threshold,
            "sl_points":          sl_pts,
            "spot_price":         cash_spot,
            "synthetic_spot":     synthetic_spot,
            "days_to_expiry":     days_to_expiry,
            "live_positions":     live_positions,
            # Cell-level update maps for JS
            "position_ltps": {str(p["token"]): p["ltp"]          for p in live_positions},
            "position_pnls": {str(p["token"]): p["pnl"]          for p in live_positions},
            "position_ivs":  {str(p["token"]): p.get("iv", 0.0)  for p in live_positions},
        }

        # --- Detailed Logging ---
        pts_allowed_str = f"{snap['points_allowed']:.2f}" if snap['points_allowed'] is not None else "∞"
        
        pos_log = ""
        for p in sorted(live_positions, key=lambda x: x['option_type'], reverse=True):
            pos_log += (
                f"\n     - {p['action']} {p['quantity']} {symbol} {p['strike']} {p['option_type']} "
                f"| Entry: {p['entry_price']:.2f} | LTP: {p['ltp']:.2f} | PnL: ₹{p['pnl']:.2f}"
            )
        
        log_msg = (
            f"📸 Snapshot for {trade_uid}:\n"
            f"   - PnL: ₹{total_pnl:.2f} (R: ₹{total_realized:.2f}, U: ₹{total_unrealized:.2f}) | PnL/Straddle: ₹{pnl_per_straddle:.2f} | Spot: ₹{cash_spot:.2f} | Syn.Fut: ₹{synthetic_spot:.2f}\n"
            f"   - Greeks (Δ|Γ|Θ|V): {net_delta:.2f} | {net_gamma:.4f} | {net_theta:.2f} | {net_vega:.2f}\n"
            f"   - Hedge (Pts Out|Allowed): {pts_out:.2f} | {pts_allowed_str}\n"
            f"   - Roll (Trigger Price): ₹{roll_trigger:.2f}\n"
            f"   - SL (Threshold|Points): ₹{sl_threshold:.2f} | {sl_pts:.2f}\n"
            f"   - IV (Avg|CE|PE): {avg_iv:.2f}% | {ce_iv:.2f}% | {pe_iv:.2f}%\n"
            f"   - DTE: {days_to_expiry}\n"
            f"   - Positions:{pos_log}"
        )
        logger.info(log_msg)
        return snap

    except Exception as e:
        logger.exception(f"❌ Snapshot compute failed for {trade_uid}: {e}")
        return None


# ── Broadcast helpers ─────────────────────────────────────────────────────────

async def _broadcast_snapshot(snap: dict):
    """Broadcast one trade snapshot as BOTH message types."""
    dead: List[WebSocket] = []
    dead += await _broadcast({"type": "pnl_batch_update", "data": [snap]})
    dead += await _broadcast({"type": "straddle_update",  "data": snap})
    _cleanup_dead(dead)


# ── Main compute loop ─────────────────────────────────────────────────────────

async def _snapshot_compute_loop():
    """Every 1 second: compute + broadcast snapshots for all active trades."""
    global _total_cycles, _last_push_time, _last_log_time, _force_queue
    logger.info("📸 Snapshot compute loop started (1s interval)")

    while True:
        try:
            # ── Priority: drain force-snapshot queue first ─────────────────
            force_uids: set = set()
            while not _force_queue.empty():
                uid = _force_queue.get_nowait()
                force_uids.add(uid)

            for uid in force_uids:
                snap = await _compute_snapshot(uid)
                if snap:
                    _latest_snapshots[uid] = snap
                    if hasattr(state, "trade_snapshots"):
                        state.trade_snapshots[uid] = snap
                    await _broadcast_snapshot(snap)

            # ── Regular cycle: all active trades ──────────────────────────
            if state.db:
                active = state.db.get_active_straddles() or []
                for trade in active:
                    uid = trade.get("trade_uid")
                    if not uid or uid in force_uids:
                        continue    # already handled above
                    snap = await _compute_snapshot(uid)
                    if snap:
                        _latest_snapshots[uid] = snap
                        if hasattr(state, "trade_snapshots"):
                            state.trade_snapshots[uid] = snap
                        await _broadcast_snapshot(snap)

            _total_cycles  += 1
            _last_push_time = time.time()

            now = time.time()
            if now - _last_log_time > 30:
                logger.info(
                    f"📸 Snapshotter: cycle #{_total_cycles}, "
                    f"{len(_latest_snapshots)} trades, "
                    f"{len(_clients)} WS clients"
                )
                _last_log_time = now

            await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logger.info("📸 Snapshot loop shutting down.")
            break
        except Exception as e:
            logger.exception(f"❌ Snapshot loop error: {e}")
            await asyncio.sleep(5)



if __name__ == "__main__":
    import uvicorn
    port = getattr(config, "SNAPSHOT_SERVICE_PORT", 8003)
    logger.info(f"🚀 Starting Snapshot Service on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
