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
from market_data.data_client import subscribe_active_straddles, market_data_service_listener, get_option_chain_from_service
from trading.data_client import set_http_client_instance
from trading.order_manager import set_interactive_instance
from trading.order_executor import set_order_executor
from trading.event_bus import EventBus, set_event_bus
from trading.trade_manager import register_event_handlers # Keep this
from trading.trade_process import trade_process_worker_entry
from utils.shared_data import SharedDataManager
from background.tasks import (
    # Removed process_market_data_queue, rest_polling_loop as they are no longer in background.tasks
    update_order_book_loop, set_websocket_clients,
    create_trade_snapshots_loop, websocket_keepalive_loop
)
from api.routes import router as api_router
from api.websocket import websocket_endpoint, websocket_clients
import config
import cred

# XTS imports
from Connect import XTSConnect


# --- Pydantic Models for API ---
class UpdateConfigRequest(BaseModel):
    sl_bps: float
    sl_start_time: str
    hedge_div: int
    straddle_div: int
    hedge_start_time: str
    roll_straddle_div: int
    roll_start_time: str
    exit_time: str


# ══════════════════════════════════════════════════════════════════════════════
# GLOBAL INSTANCES
# ══════════════════════════════════════════════════════════════════════════════

xt_i = None          # Interactive API (orders)
event_bus = None     # Event coordination bus
background_tasks = []  # Track background tasks for cleanup

def is_port_in_use(port: int) -> bool:
    """Check if a port is already in use on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0


async def restore_active_trades():
    """
    On startup, find all active trades and restart their monitoring.
    """
    logger.info("="*100)
    logger.info("🔄 RESTORING ACTIVE TRADES...")
    logger.info("="*100)
    
    try:
        # Defer import to avoid potential circular dependency issues at startup
        from trading.trade_manager import get_trade_manager
        from trading.square_off import square_off_by_trade_uid

        all_trades_today = state.db.get_todays_straddles()
        if not all_trades_today:
            logger.info("✅ No active trades to restore.")
            return

        # --- RESUMPTION LOGIC ---
        # Identify trades that are active or were interrupted mid-action.
        resumable_statuses = ['ACTIVE', 'PARTIAL', 'SQUARING-OFF', 'PARTIAL-SQF', 'HEDGING', 'ROLLING', 'BUILDING']
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
            
            # Spawn a process for the existing trade
            command_q = multiprocessing.Queue()
            snapshot_q = multiprocessing.Queue()
            
            process = multiprocessing.Process(
                target=trade_process_worker_entry,
                args=(trade_uid, trade.get('config', {}), command_q, snapshot_q, state.option_chains, [])
            )
            process.start()
            
            state.trade_processes[trade_uid] = {
                'process': process,
                'command_q': command_q,
                'snapshot_q': snapshot_q
            }
            
            # The worker process will handle its own state, including re-triggering actions if needed.
            
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
    
    logger.info("="*100)
    logger.info("🚀 STARTING LIVE STRADDLE TRADING DASHBOARD")
    logger.info("="*100)
    
    try:
        # ══════════════════════════════════════════════════════════════════
        # STEP 1: DATABASE INITIALIZATION
        # ══════════════════════════════════════════════════════════════════
        logger.info("📊 Step 1/7: Initializing database...")
        state.db = Database()
        logger.info("✅ Database ready")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 2: SHARED DATA & PROCESS MANAGEMENT
        # ══════════════════════════════════════════════════════════════════
        logger.info("🧠 Step 2/7: Initializing Shared Data and Process Manager...")
        state.shared_data = SharedDataManager(create=True)
        state.trade_processes = {} # To store {trade_uid: {'process': Process, 'command_q': Queue, ...}}
        # Make shared data accessible via state for other modules
        state.prices = state.shared_data.prices_array
        state.option_chains = state.shared_data.option_chains_proxy
        logger.info("✅ Shared data structures ready")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 1.5: INITIALIZE STATE QUEUES
        # ══════════════════════════════════════════════════════════════════
        state.cancellation_flags = {}
        # Initialize trade data cache for DB lag protection
        if not hasattr(state, 'trade_data_cache'):
            state.trade_data_cache = {}
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 2: XTS INTERACTIVE API LOGIN
        # ══════════════════════════════════════════════════════════════════
        logger.info("🔐 Step 3/7: Logging into XTS Interactive API...")
        
        xt_i = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        response_i = xt_i.interactive_login()
        
        if response_i.get('type') != 'success':
            raise Exception(f"Interactive login failed: {response_i.get('description', 'Unknown error')}")
            
        # Persist token for Order Book Service
        await persist_interactive_token(response_i['result']['token'], response_i['result']['userID'], response_i['result']['isInvestorClient'])
        
        # Force isInvestorClient to False to ensure clientID is always passed for pro accounts
        xt_i.isInvestorClient = False
        logger.info("Forcing isInvestorClient to False for Pro account order placement.")

        state.xt_i = xt_i  # Store interactive client in global state
        
        logger.info(f"✅ Interactive API logged in")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 4: START MICROSERVICES
        # ══════════════════════════════════════════════════════════════════
        logger.info("⚙️ Step 4/7: Starting microservices...")
        state.services = {}
        
        # 1. Market Data Service (Port 8001)
        if is_port_in_use(8001):
            logger.warning("⚠️  Port 8001 is busy. Assuming Market Data Service is running manually.")
        else:
            state.services['marketdata'] = subprocess.Popen([sys.executable, "marketdata_service.py"])
            logger.info("   ✅ Market Data Service started (Port 8001)")

        # 2. Order Book Service (Port 8002)
        if is_port_in_use(8002):
            logger.warning("⚠️  Port 8002 is busy. Assuming Order Book Service is running manually.")
        else:
            state.services['orderbook'] = subprocess.Popen([sys.executable, "order_book_service.py"])
            logger.info("   ✅ Order Book Service started (Port 8002)")

        # 3. Snapshot Service (Port 8003)
        snapshot_port = getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)
        if is_port_in_use(snapshot_port):
            logger.warning(f"⚠️  Port {snapshot_port} is busy. Assuming Snapshot Service is running manually.")
        else:
            state.services['snapshot'] = subprocess.Popen([sys.executable, "snapshot_service.py"])
            logger.info(f"   ✅ Snapshot Service started (Port {snapshot_port})")

        # ══════════════════════════════════════════════════════════════════
        # STEP 3: XTS MARKET DATA API LOGIN
        # ══════════════════════════════════════════════════════════════════
        logger.info("📈 Step 4/7: Market Data Service is separate. Skipping login.")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 5: EVENT BUS INITIALIZATION
        # ══════════════════════════════════════════════════════════════════
        logger.info("🚌 Step 5/7: Initializing Event Bus...")
        
        event_bus = EventBus()
        set_event_bus(event_bus)
        
        # Register event handlers (hedge, SL, roll, square-off)
        register_event_handlers()
        
        # Start event bus processing loop
        event_bus_task = asyncio.create_task(event_bus.process_events())
        background_tasks.append(event_bus_task)
        
        logger.info("✅ Event Bus started (Priority: HEDGE>SL>SQUARE_OFF>ROLL)")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 6: SET GLOBAL INSTANCES
        # ══════════════════════════════════════════════════════════════════
        logger.info("🔧 Step 6/7: Setting global instances...")
        
        # Set interactive instance for order manager
        set_interactive_instance(xt_i)
        logger.info("   ✅ Order manager instance set")
        
        # Initialize HTTP client for trading module to allow fallback fetches
        host = getattr(config, 'HOST', '127.0.0.1')
        connect_host = host if host != '0.0.0.0' else '127.0.0.1'
        set_http_client_instance(connect_host, config.MARKET_DATA_PORT)
        logger.info("   ✅ Trading Data Client initialized")
        
        # Initialize order executor
        client_id = getattr(cred, 'clientID', None) # ✅ FIX: Use 'clientID' to match the variable name in your cred.py and test scripts.
        set_order_executor(
            xt_interactive=xt_i,
            max_concurrent=20,
            client_id=client_id
        )
        logger.info(f"   ✅ Order executor initialized for clientID: '{client_id}' (max_concurrent=20)")
        
        # Set WebSocket clients reference
        set_websocket_clients(websocket_clients)
        logger.info("   ✅ WebSocket clients set")
        
        logger.info("✅ All global instances configured")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 7: START BACKGROUND TASKS
        # ══════════════════════════════════════════════════════════════════
        logger.info("🔄 Step 7/7: Starting background tasks...")
        
        # Order book sync
        # background_tasks.append(asyncio.create_task(update_order_book_loop())) # Removed, handled by service
        logger.info("   ✅ Order book updater")
        
        # NEW: Real-time data listener for the Market Data Service
        background_tasks.append(asyncio.create_task(market_data_service_listener()))
        logger.info(f"   ✅ Market Data Service listener started (WebSocket)")

        # Trade snapshots processor (reads from queues)
        task3 = asyncio.create_task(create_trade_snapshots_loop())
        background_tasks.append(task3)
        logger.info("   ✅ Trade snapshots creator")
        
        # WebSocket Keep-Alive
        task4 = asyncio.create_task(websocket_keepalive_loop())
        background_tasks.append(task4)
        logger.info("   ✅ WebSocket keep-alive started")

        logger.info("✅ Background tasks running")
        
        # ══════════════════════════════════════════════════════════════════
        # STEP 8: SUBSCRIBE TO ACTIVE TRADES (via Market Data Service)
        # ══════════════════════════════════════════════════════════════════
        # Pre-warm option chain cache
        logger.info("Pre-warming option chain cache for NIFTY...")
        nifty_chain = await get_option_chain_from_service("NIFTY")
        if nifty_chain:
            state.option_chains['NIFTY'] = nifty_chain

        await asyncio.sleep(2) # Give services a moment to start
        
        # Subscribe to active straddles
        logger.info("📡 Subscribing to active straddles...")
        await subscribe_active_straddles()
        logger.info("✅ Active straddles subscribed")
        
        # Restore monitors for active trades
        await restore_active_trades()
        
        # ══════════════════════════════════════════════════════════════════
        # STARTUP COMPLETE
        # ══════════════════════════════════════════════════════════════════
        logger.info("="*100)
        logger.info("✅ INITIALIZATION COMPLETE - SYSTEM READY")
        logger.info("="*100)
        logger.info(f"🌐 Dashboard URL:    http://localhost:{config.PORT}")
        logger.info(f"📊 WebSocket URL:    ws://localhost:{config.PORT}/ws")
        logger.info(f"📈 Market Data Svc:  http://localhost:{config.MARKET_DATA_PORT}")
        logger.info(f"💾 Database:         {state.db.db_name}")
        logger.info(f"⚡ Max Orders:        20 concurrent")
        logger.info(f"🚌 Event Bus:        Active (Priority-based)")
        logger.info(f"📈 Active Straddles: {len(state.db.get_active_straddles())}")
        logger.info(f"💰 Cached Prices:    {len(state.prices)} instruments")
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
    
    # Application running - yield control
    yield
    
    # ══════════════════════════════════════════════════════════════════════
    # SHUTDOWN SEQUENCE (Triggered by Ctrl+C or app.shutdown())
    # ══════════════════════════════════════════════════════════════════════
    logger.info("="*100)
    logger.info("🛑 SHUTDOWN INITIATED")
    logger.info("="*100)
    
    try:
        # Stop all trade processes
        logger.info("🛑 Stopping all trade processes...")
        for trade_uid, process_info in state.trade_processes.items():
            try:
                process_info['command_q'].put({'command': 'STOP'})
                process_info['process'].join(timeout=10) # Wait for graceful shutdown
                if process_info['process'].is_alive():
                    process_info['process'].terminate() # Force terminate if stuck
                logger.info(f"   ✅ Process for trade {trade_uid} stopped.")
            except Exception as e:
                logger.warning(f"   ⚠️ Error stopping process for {trade_uid}: {e}")

        # Stop Microservices
        if hasattr(state, 'services'):
            logger.info("🛑 Stopping microservices...")
            for name, proc in state.services.items():
                proc.terminate()
                logger.info(f"   ✅ {name} service stopped")

        # Stop Event Bus
        if event_bus:
            logger.info("🚌 Stopping Event Bus...")
            try:
                await event_bus.stop()
                logger.info("   ✅ Event Bus stopped")
            except Exception as e:
                logger.warning(f"   ⚠️  Event Bus stop error: {e}")
        
        # Cancel all background tasks
        logger.info("🔄 Cancelling background tasks...")
        for task in background_tasks:
            if not task.done():
                task.cancel()
        
        # Wait for tasks to complete cancellation
        if background_tasks:
            await asyncio.gather(*background_tasks, return_exceptions=True)
        logger.info("   ✅ Background tasks stopped")

        # Close shared data manager
        if state.shared_data:
            logger.info("🧠 Closing shared data manager...")
            state.shared_data.close(unlink=True)
            state.shared_data = None
            logger.info("   ✅ Shared data manager closed")

        
        # Close database
        if state.db:
            logger.info("💾 Closing database...")
            try:
                state.db.close()
                state.db = None # Explicitly set to None after closing
                logger.info("   ✅ Database closed")
            except Exception as e:
                logger.warning(f"   ⚠️  Database close error: {e}")
        
        logger.info("="*100)
        logger.info("✅ SHUTDOWN COMPLETE - GOODBYE!")
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
app.include_router(api_router)

@app.post("/api/straddle/update-config/{trade_uid}")
async def api_update_trade_config(trade_uid: str, request: UpdateConfigRequest):
    """
    API endpoint to update the configuration of a live trade.
    Dispatches a command to the corresponding trade process.
    """
    # --- FIX: Handle PENDING trades by updating the DB directly ---
    loop = asyncio.get_event_loop()
    trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found in DB.")

    if trade.get('status') == 'PENDING':
        logger.info(f"Updating config for PENDING trade {trade_uid} directly in DB.")
        
        # Reconstruct the 'monitors' object from the request
        new_monitors_config = {
            'sl': {'sl_bps': request.sl_bps, 'start_time': request.sl_start_time, 'interval': trade['monitors']['sl']['interval'], 'running': False, 'sl_points': 0},
            'hedge': {'hedge_div': request.hedge_div, 'straddle_div': request.straddle_div, 'start_time': request.hedge_start_time, 'interval': trade['monitors']['hedge']['interval'], 'running': False},
            'roll': {'roll_straddle_div': request.roll_straddle_div, 'start_time': request.roll_start_time, 'interval': trade['monitors']['roll']['interval'], 'running': False},
            'square_off': {'exit_time': request.exit_time, 'running': False}
        }
        
        trade['monitors'] = new_monitors_config
        trade['config'].update(request.dict()) # Update the flat config as well
        
        await loop.run_in_executor(None, state.db.insert_straddle, trade)
        
        return {'success': True, 'message': 'Pending trade configuration updated successfully.'}
    # --- END FIX ---

    if trade_uid in state.trade_processes:
        logger.info(f"Dispatching UPDATE_CONFIG command to process for trade {trade_uid}.")
        process_info = state.trade_processes[trade_uid]
        if not process_info['process'].is_alive():
            logger.error(f"Process for trade {trade_uid} is not alive. Cannot update config.")
            # Clean up dead process
            if trade_uid in state.trade_processes:
                del state.trade_processes[trade_uid]
            raise HTTPException(status_code=404, detail="Trade process is not running.")
        
        # Send the new config to the trade's dedicated process
        process_info['command_q'].put({
            'command': 'UPDATE_CONFIG',
            'data': request.dict()
        })
        return {'success': True, 'message': 'Configuration update command dispatched.'}
    else:
        logger.error(f"Process for trade {trade_uid} not found. Cannot update config.")
        raise HTTPException(status_code=404, detail="Trade process not found.")

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
        "active_straddles": active_straddles,
        "event_bus": "active" if event_bus and event_bus.running else "inactive",
        "version": "2.0.0"
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
