"""
Central in-process state — single source of truth for run_dev.py
All tasks read/write these directly. Zero IPC, zero serialization.
"""
import asyncio
from typing import Dict, Any, Optional

# ── Per-trade state ────────────────────────────────────────────────────
# Written by: master_tick (Greeks), monitors (actions), verifier (fills)
# Read by:    all tasks
TRADE_STATES: Dict[str, dict] = {}
# Each entry:
# {
#   "status":          "ACTIVE" | "BUILDING" | "SQUAREDOFF",
#   "lock":            asyncio.Lock(),   # per-trade, prevents race conditions
#   "ce_ltp":          float,
#   "pe_ltp":          float,
#   "net_delta":       float,
#   "net_gamma":       float,
#   "net_theta":       float,
#   "net_vega":        float,
#   "pnl":             float,
#   "greeks":          dict,
#   "verified":        bool,            # written by reconciler
#   "pending_action":  None | str,      # SL/hedge queued during build
#   "spot":            float,
#   "avg_iv":          float,
# }

# ── Order tracking ─────────────────────────────────────────────────────
# Written by: order_reconciler process via SHM
# Read by:    verifier task, monitors
ORDER_DICT: Dict[str, dict] = {}
# Each entry: {order_id: {status, avg_price, qty, trade_uid, ...}}

VERIFIED: Dict[str, bool] = {}
# {trade_uid: True/False}

# ── Fill events ────────────────────────────────────────────────────────
# asyncio.Event per trade_uid — set by verifier when fills confirmed
FILL_EVENTS: Dict[str, asyncio.Event] = {}

def get_or_create_fill_event(trade_uid: str) -> asyncio.Event:
    if trade_uid not in FILL_EVENTS:
        FILL_EVENTS[trade_uid] = asyncio.Event()
    return FILL_EVENTS[trade_uid]

def get_trade_lock(trade_uid: str) -> asyncio.Lock:
    if trade_uid not in TRADE_STATES:
        TRADE_STATES[trade_uid] = {"lock": asyncio.Lock(), "status": "UNKNOWN"}
    if "lock" not in TRADE_STATES[trade_uid]:
        TRADE_STATES[trade_uid]["lock"] = asyncio.Lock()
    return TRADE_STATES[trade_uid]["lock"]

def init_trade(trade_uid: str, initial: dict = None):
    if trade_uid not in TRADE_STATES:
        TRADE_STATES[trade_uid] = { "lock": asyncio.Lock(), "status": "BUILDING", "verified": False, "pending_action": None, "ce_ltp": 0.0, "pe_ltp": 0.0, "net_delta": 0.0, "net_gamma": 0.0, "net_theta": 0.0, "net_vega": 0.0, "pnl": 0.0, "spot": 0.0, "avg_iv": 0.0, "greeks": {}, }
    if initial:
        TRADE_STATES[trade_uid].update(initial)
    FILL_EVENTS[trade_uid] = asyncio.Event()
    VERIFIED[trade_uid] = False