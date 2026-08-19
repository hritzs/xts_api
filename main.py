"""
Main FastAPI Application - Event-Driven Trading System
Version: 2.0.0
Features: Event Bus, Delta-Neutral, Config-Based Automation
"""
import asyncio
import multiprocessing
import os
import sys
import socket
import zmq
import zmq.asyncio
import sqlite3
import json
from datetime import datetime
import subprocess
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Local imports
from utils.logger import logger
from models.state import state
from database.db_manager import Database
from market_data import subscribe_active_straddles, market_data_service_listener # Import ZMQ client functions
from trading.data_client import set_http_client_instance
from trading.order_manager import set_interactive_instance
from trading.order_executor import set_order_executor
from trading.event_bus import EventBus, set_event_bus
from trading.trade_manager import register_event_handlers # Keep this
from trading.builder import manual_sync_trade_orders
from trading.trade_process import trade_process_worker_entry
from utils.shared_data import SharedDataManager
# Find this line in main.py (roughly at the top):
from background.tasks import (
    create_trade_snapshots_loop,
    set_websocket_clients,
    broadcast_message,
    broadcast_log,
    websocket_keepalive_loop,
    monitor_xts_socket_status,
    cleanup_old_data,
    snapshot_bridge_loop,
    capture_918_synthetic_price_loop,
    reconciliation_listener,
    marketdata_bridge_loop,          # ← ADD THIS LINE
)

from api.routes import router as api_router
from api.websocket import websocket_endpoint, websocket_clients
import config
import cred

# XTS imports
from Connect import XTSConnect

# --- Pydantic Models for API ---

def is_port_in_use(port: int) -> bool:
    """Check if a port is already in use on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

async def restore_active_trades():
    """
    On startup, find all active trades and restart their monitoring.
    """
    logger.debug("=" * 100)
    logger.info("🔄 RESTORING ACTIVE TRADES...")
    logger.debug("=" * 100)

    try:
        from trading.trade_process import trade_process_worker_entry

        all_trades_today = state.db.get_todays_straddles()
        if not all_trades_today:
            logger.info("✅ No active trades to restore.")
            return

        resumable_statuses = [
            'ACTIVE', 'PARTIAL', 'SQUARING-OFF', 'PARTIAL-SQF',
            'HEDGING', 'ROLLING', 'BUILDING'
        ]
        trades_to_restore = [t for t in all_trades_today if t.get('status') in resumable_statuses]

        if not trades_to_restore:
            logger.info("✅ No active or resumable trades found.")
            return

        logger.info(f"📊 Found {len(trades_to_restore)} trades to restore (active or interrupted).")

        for trade in trades_to_restore:
            trade_uid = trade.get('trade_uid')
            status = trade.get('status')
            if not trade_uid:
                continue

            logger.info(f"   -> Spawning process for {trade_uid} (Status: {status})...")

            command_q = multiprocessing.Queue()

            process = multiprocessing.Process(
                target=trade_process_worker_entry,
                args=(trade_uid, trade, command_q, state.trade_data_cache, [])
            )
            process.start()

            state.trade_processes[trade_uid] = {
                'pid': process.pid,
                'status': status,
            }
            state.local_process_refs[trade_uid] = process
            state.local_command_queues[trade_uid] = command_q

    except Exception as e:
        logger.error(f"❌ Failed to restore active trades: {e}", exc_info=True)

async def persist_interactive_token(token: str, user_id: str, is_investor: bool):
    """Save interactive login token to shared DB for services."""
    try:
        db_path = os.path.abspath("shared_tokens.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT, timestamp DATETIME)")
        
        data = json.dumps({'token': token, 'userID': user_id, 'isInvestorClient': is_investor})
        cursor.execute("INSERT OR REPLACE INTO tokens (key, value, timestamp) VALUES (?, ?, ?)",
                       ('xts_interactive_token', data, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        logger.info("💾 Interactive login token persisted to shared DB")
    except Exception as e:
        logger.error(f"❌ Failed to persist token: {e}")

async def _post_startup_tasks():
    """
    Tasks to run in the background AFTER the main server has started.
    This prevents long-running initializations from blocking the health check.
    """
    try:
        # Give microservices a moment to be ready before subscribing.
        # Increased to 5s to allow marketdata_service to build initial chains.
        logger.info("   ⏳ Waiting 2s for services to stabilize before restoring trades...")
        await asyncio.sleep(2)

        # --- Pre-build all configured option chains ---
        from trading.data_client import get_option_chain_from_service
        from trading.chain_provider import SYMBOL_CONFIG
        logger.info("   Pre-building all configured option chains (NIFTY, SENSEX, etc.)...")
        build_tasks = []
        for symbol in SYMBOL_CONFIG.keys():
            logger.info(f"      -> Triggering build for {symbol}")
            build_tasks.append(get_option_chain_from_service(symbol))
        await asyncio.gather(*build_tasks, return_exceptions=True)

        # Subscribe to active straddle tokens
        await subscribe_active_straddles()

        # Restore trade monitors for any in-flight trades surviving a restart
        await restore_active_trades()
    except Exception as e:
        logger.error(f"❌ Error in post-startup tasks: {e}", exc_info=True)

# ══════════════════════════════════════════════════════════════════════════════
# APPLICATION LIFESPAN (STARTUP & SHUTDOWN)
# ══════════════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager
    Handles:
    - Startup: DB, XTS login, Socket.IO, Event Bus, Background tasks
    - Shutdown: Graceful cleanup of all resources
    """
    global xt_i, event_bus, background_tasks
    background_tasks = []

    background_tasks = []

    logger.info("="*100)
    logger.info("🚀 STARTING LIVE STRADDLE TRADING DASHBOARD")
    logger.info("="*100)

    try:
        # ══════════════════════════════════════════════════════════════════
        # STEP 1: DATABASE INITIALIZATION
        # ══════════════════════════════════════════════════════════════════
        logger.info("📊 Step 1/8: Initializing database...")
        state.db = Database()
        logger.info("✅ Database ready")

        # ══════════════════════════════════════════════════════════════════
        # STEP 2: SHARED DATA & PROCESS MANAGEMENT
        # ══════════════════════════════════════════════════════════════════
        logger.info("🧠 Step 2/8: Initializing Shared Data and Process Manager...")
        try:
            logger.info(f"[SHARED DATA CREATOR] file={__file__}")
            state.shared_data = SharedDataManager(create=True)
            logger.info("[SHARED DATA INITIALIZED] SharedDataManager(create=True) completed successfully")
            
            state.trade_processes = multiprocessing.Manager().dict()
            state.local_process_refs = {}
            state.local_command_queues = {}
            state.prices = state.shared_data.prices_array
            state.shared_data.order_book_cache = state.shared_data.manager.dict()
            state.shared_data.verified_trades = state.shared_data.manager.dict()
            state.option_chains = state.shared_data.option_chains_proxy
            state.trade_data_cache = state.shared_data.trade_data_cache_proxy
            state.cancellation_flags = {}
            if not hasattr(state, 'trade_snapshots'):
                state.trade_snapshots = {}
            if not hasattr(state, 'temp_order_cache'):
                state.temp_order_cache = {}
            logger.info("✅ Shared data structures ready")
        except Exception as e:
            logger.exception(f"❌ Shared data manager initialization failed: {e}")
            raise

        logger.info("🔐 Step 3/8: Logging into XTS Interactive API...")

        xt_i = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        response_i = xt_i.interactive_login()

        if response_i.get('type') != 'success':
            raise Exception(
                f"Interactive login failed: {response_i.get('description', 'Unknown error')}"
            )

        await persist_interactive_token(
            response_i['result']['token'],
            response_i['result']['userID'],
            response_i['result']['isInvestorClient']
        )

        # Force isInvestorClient=False so clientID is always passed for pro accounts
        xt_i.isInvestorClient = False
        logger.info("   Forcing isInvestorClient=False for Pro account order placement.")

        state.xt_i = xt_i
        logger.info("✅ Interactive API logged in")

        # ══════════════════════════════════════════════════════════════════
        # STEP 4: START MICROSERVICES
        # ══════════════════════════════════════════════════════════════════
        logger.info("⚙️ Step 4/8: Microservice Management")
        logger.info("   Services are now managed by start_all.py.")
        logger.info("   This process assumes they are already running.")
        state.services = {}
        logger.info("✅ Microservice check complete.")

        # ══════════════════════════════════════════════════════════════════
        # STEP 5: EVENT BUS INITIALIZATION
        # ══════════════════════════════════════════════════════════════════
        logger.info("🚌 Step 5/8: Initializing Event Bus...")

        event_bus = EventBus()
        set_event_bus(event_bus)
        register_event_handlers()

        event_bus_task = asyncio.create_task(event_bus.process_events())
        background_tasks.append(event_bus_task)

        logger.info("✅ Event Bus started (Priority: HEDGE > SL > SQUARE_OFF > ROLL)")

        # ══════════════════════════════════════════════════════════════════
        # STEP 6: SET GLOBAL INSTANCES
        # ══════════════════════════════════════════════════════════════════
        logger.info("🔧 Step 6/8: Setting global instances...")

        set_interactive_instance(xt_i)
        logger.info("   ✅ Order manager instance set")

        host = getattr(config, 'HOST', '127.0.0.1')
        connect_host = host if host != '0.0.0.0' else '127.0.0.1'
        set_http_client_instance(connect_host, config.MARKET_DATA_PORT)
        logger.info("   ✅ Trading Data Client initialized")

        client_id = getattr(cred, 'clientID', None)
        set_order_executor(
            xt_interactive=xt_i,
            max_concurrent=20,
            client_id=client_id
        )
        logger.info(f"   ✅ Order executor initialized for clientID: '{client_id}' (max_concurrent=20)")

        set_websocket_clients(websocket_clients)
        logger.info("   ✅ WebSocket clients set")

        logger.info("✅ All global instances configured")

        # ══════════════════════════════════════════════════════════════════
        # STEP 7: START BACKGROUND TASKS
        # ══════════════════════════════════════════════════════════════════
        logger.info("🔄 Step 7/8: Starting background tasks...")

        # Market Data Service listener (WebSocket → shared prices array)
        background_tasks.append(
            asyncio.create_task(market_data_service_listener())
        )
        logger.info("   ✅ Market Data Service listener started (WebSocket)")

        # Snapshot loop stub — actual computation is in snapshot_service.py (port 8003)
        background_tasks.append(
            asyncio.create_task(create_trade_snapshots_loop())
        )
        logger.info("   ✅ Snapshot loop delegated to snapshot_service (port 8003)")
        
        background_tasks.append(
            asyncio.create_task(snapshot_bridge_loop())
        )
        logger.info("   ✅ Snapshot bridge started (port 8003 → 5000)")

        background_tasks.append(
            asyncio.create_task(marketdata_bridge_loop())
        )
        logger.info("   ✅ Marketdata bridge started (ZMQ PUB port → 5000, topics: price_update + chain_header_update)")
        # XTS socket status monitor
        background_tasks.append(
            asyncio.create_task(monitor_xts_socket_status())
        )
        logger.info("   ✅ XTS socket status monitor started")

        # WebSocket keep-alive
        background_tasks.append(
            asyncio.create_task(websocket_keepalive_loop())
        )
        logger.info("   ✅ WebSocket keep-alive started")

        # DB cleanup (runs every 6 hours)
        background_tasks.append(
            asyncio.create_task(cleanup_old_data())
        )
        logger.info("   ✅ DB cleanup task started (6h interval)")

        # ZMQ/SHM listener for reconciler updates
        background_tasks.append(
            asyncio.create_task(reconciliation_listener())
        )
        logger.info("   ✅ Reconciliation listener started (ZMQ/SHM)")

        # 9:18 Price Capture
        background_tasks.append(
            asyncio.create_task(capture_918_synthetic_price_loop())
        )
        logger.info("   ✅ 9:18 Synthetic Price Capture task started")

        logger.info("✅ Background tasks running")

        # ══════════════════════════════════════════════════════════════════
        # STEP 8: SUBSCRIBE & RESTORE ACTIVE TRADES
        # ══════════════════════════════════════════════════════════════════
        logger.info("📡 Step 8/8: Subscribing to active trades and restoring monitors...")
        # --- FIX: Defer these long-running tasks to the background ---
        # This allows the server to start and respond to health checks immediately.
        background_tasks.append(asyncio.create_task(_post_startup_tasks()))
        logger.info("   ✅ Trade restoration and subscription tasks scheduled to run in background.")

        # ══════════════════════════════════════════════════════════════════
        # STARTUP COMPLETE
        # ══════════════════════════════════════════════════════════════════
        logger.info("="*100)
        logger.info("✅ INITIALIZATION COMPLETE — SYSTEM READY")
        logger.info("="*100)
        logger.info(f"🌐 Dashboard URL:       http://localhost:{config.PORT}")
        logger.info(f"📊 Main WS:            ws://localhost:{config.PORT}/ws")
        logger.info(f"📈 Market Data Svc:    ZMQ PUB Port {config.ZMQ_MARKETDATA_PUB_PORT}")
        logger.info(f"📋 Order Reconciler:   ZMQ PUB Port {config.ZMQ_FILLS_PUB_PORT}")
        logger.info(f"� Database:           {state.db.db_name}")
        logger.info(f"⚡ Max Orders:          20 concurrent")
        logger.info(f"🚌 Event Bus:          Active (Priority-based)")
        logger.info(f"📈 Active Straddles:   {len(state.db.get_active_straddles())}")
        logger.info(f"💰 Cached Prices:      {len(state.prices)} instruments")
        logger.info("="*100)

    except Exception as e:
        logger.error("="*100)
        logger.error("❌ STARTUP FAILED")
        logger.error("="*100)
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        logger.error("="*100)
        raise

    # ── Running ──────────────────────────────────────────────────────────
    yield

    # ══════════════════════════════════════════════════════════════════════
    # SHUTDOWN SEQUENCE
    # ══════════════════════════════════════════════════════════════════════
    logger.info("="*100)
    logger.info("🛑 SHUTDOWN INITIATED")
    logger.info("="*100)

    try:
        # 1. Stop all trade worker processes gracefully
        logger.info("🛑 Stopping all trade worker processes...")
        for trade_uid, process_info in list(state.local_process_refs.items()):
            try:
                # Get the queue from the shared manager dict
                # Get the queue from the local command queue registry
                command_q = state.local_command_queues.get(trade_uid)
                if command_q:
                    command_q.put({'command': 'STOP'})

                process_info.join(timeout=10)
                if process_info.is_alive():
                    process_info.terminate()
                    logger.warning(f"   ⚠️  Force-terminated process for {trade_uid}")
                else:
                    logger.info(f"   ✅ Process for {trade_uid} stopped gracefully")
            except Exception as e:
                logger.warning(f"   ⚠️  Error stopping process for {trade_uid}: {e}")

        # 2. Stop microservices (snapshot, verifier, orderbook, marketdata)
        if hasattr(state, 'services'):
            logger.info("🛑 Stopping microservices...")
            for name, proc in state.services.items():
                try:
                    proc.terminate()
                    proc.wait(timeout=5)
                    logger.info(f"   ✅ {name} service stopped")
                except Exception as e:
                    logger.warning(f"   ⚠️  Error stopping {name} service: {e}")

        # 3. Stop Event Bus
        if event_bus:
            logger.info("🚌 Stopping Event Bus...")
            try:
                await event_bus.stop()
                logger.info("   ✅ Event Bus stopped")
            except Exception as e:
                logger.warning(f"   ⚠️  Event Bus stop error: {e}")

        # 4. Cancel all background tasks
        logger.info("🔄 Cancelling background tasks...")
        for task in background_tasks:
            if not task.done():
                task.cancel()
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        logger.info("   ✅ Background tasks stopped")

        # 5. Close shared memory
        if state.shared_data:
            logger.info("🧠 Closing shared data manager...")
            state.shared_data.close(unlink=True)
            state.shared_data = None
            logger.info("   ✅ Shared data manager closed")

        # 6. Close database
        if state.db:
            logger.info("💾 Closing database...")
            try:
                state.db.close()
                state.db = None
                logger.info("   ✅ Database closed")
            except Exception as e:
                logger.warning(f"   ⚠️  Database close error: {e}")

        logger.info("="*100)
        logger.info("✅ SHUTDOWN COMPLETE — GOODBYE!")
        logger.info("="*100)

    except Exception as e:
        logger.error(f"❌ Shutdown error: {e}")
        import traceback
        logger.error(traceback.format_exc())

# ══════════════════════════════════════════════════════════════════════════════
# FASTAPI APPLICATION
# ══════════════════════════════════════════════════════════════════════════════

app = FastAPI(
    title="Live Straddle Trading Dashboard",
    description="Real-time option trading with event-driven automation",
    version="2.0.0",
    lifespan=lifespan
)

# ══════════════════════════════════════════════════════════════════════════════
# MIDDLEWARE
# ══════════════════════════════════════════════════════════════════════════════

# Define allowed origins for the main application's frontend UI.
# Using a wildcard ("*") with `allow_credentials=True` is a security risk and is
# disallowed by browsers for WebSocket connections, which can lead to 403 errors.
allowed_origins_main = [
    f"http://localhost:{config.PORT}",
    f"http://127.0.0.1:{config.PORT}",
    "http://localhost",
    "http://127.0.0.1",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins_main,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════

# Include API routes
app.include_router(api_router, prefix="/api")

@app.post("/api/trade/sync/{trade_uid}", tags=["Trade Management"])
async def sync_trade_orders(trade_uid: str):
    """
    Manually synchronizes orders for a specific trade between the local DB and the broker's order book.
    This fixes discrepancies where an order exists at the broker but not in the local database.
    """
    logger.info(f"API call received to manually sync trade: {trade_uid}")
    try:
        result = await manual_sync_trade_orders(trade_uid)
        if not result or not result.get("success"):
            # Use the error from the result if available
            detail = result.get("error", "Sync failed with an unknown error")
            raise HTTPException(status_code=500, detail=detail)
        return result
    except Exception as e:
        logger.error(f"API sync failed for {trade_uid}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

# WebSocket endpoint
@app.websocket("/ws")
async def ws(websocket: WebSocket):
    """WebSocket endpoint for live updates"""
    await websocket_endpoint(websocket)

# Mount static files
try:
    app.mount("/static", StaticFiles(directory="static"), name="static")
    logger.info("✅ Static files mounted: /static")
except Exception as e:
    logger.warning(f"⚠️  Static files not mounted: {e}")

# Serve dashboard HTML
@app.get("/", response_class=HTMLResponse)
async def get_dashboard():
    """Serve main dashboard HTML"""
    try:
        with open("static/dashboard.html", "r", encoding='utf-8') as f:
            html_content = f.read()
        return HTMLResponse(content=html_content)
    except FileNotFoundError:
        logger.error("❌ Dashboard HTML not found")
        return HTMLResponse(
            content="""
            <html>
                <head><title>Dashboard Not Found</title></head>
                <body>
                    <h1>❌ Dashboard Not Found</h1>
                    <p>Please ensure static/dashboard.html exists</p>
                </body>
            </html>
            """,
            status_code=404
        )

# Health check endpoint
@app.get("/health")
async def health_check():
    """
    Health check endpoint
    
    Returns system status and metrics
    """
    try:
        active_straddles = len(state.db.get_active_straddles()) if state.db else 0
    except Exception:
        active_straddles = 0
    
    return {
        "status": "ok",
        "timestamp": asyncio.get_event_loop().time(),
        "socket_connected": state.socket_connected,
        "data_source": getattr(state, 'data_source', 'UNKNOWN'),
        "db_connected": state.db is not None,
        "cached_prices": len(state.prices),
        "subscribed_tokens": len(state.subscribed_tokens),
        "reconciler_shm_orders": len(state.shared_data.order_book_cache) if hasattr(state, 'shared_data') and state.shared_data.order_book_cache is not None else -1,
        "active_straddles": active_straddles,
        "event_bus": "active" if event_bus and event_bus.running else "inactive",
        "version": "3.0.0"
    }

# ══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn
    
    logger.info("="*100)
    logger.info("🚀 STARTING UVICORN SERVER")
    logger.info("="*100)
    logger.info(f"Host:        {config.HOST}")
    logger.info(f"Port:        {config.PORT}")
    logger.info(f"Log Level:   INFO")
    logger.info(f"Reload:      False")
    logger.info("="*100)
    
    try:
        uvicorn.run(
            app,
            host=config.HOST,
            port=config.PORT,
            log_level="info",
            access_log=True,
            timeout_keep_alive=30,
            ws_ping_interval=30,
            ws_ping_timeout=30
        )
    except KeyboardInterrupt:
        logger.info("🛑 Server stopped by user (Ctrl+C)")
    except Exception as e:
        logger.error(f"❌ Server error: {e}")
        import traceback
        logger.error(traceback.format_exc())
