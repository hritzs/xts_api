# c:\Users\Administrator\Desktop\api_v2_microservices\marketdata_service.py

import asyncio
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import time
from fastapi import WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from typing import List, Dict, Set

# Local imports from the main project structure
# We assume this service runs from the same root directory
from utils.logger import logger
from models.state import state  # This state is shared, so it's fine.
# The socket callbacks (on_socket_connect, etc.) are defined directly in this file,
# so no import is needed for them. The set_main_event_loop is also not needed here.

# Import from the *server-side* marketdata package
# The service-side implementation of the chain provider is in the 'trading' package
from trading.chain_provider import set_xts_instances, get_option_chain as build_get_option_chain, get_spot_details
from market_data.tasks import update_option_chain_cache_loop, process_market_data_queue, rest_polling_loop, monitor_xts_socket_status, calculate_greeks_loop
from utils.shared_data import SharedDataManager
import config
import cred

# XTS imports
from Connect import XTSConnect
from MarketDataSocketClient import MDSocket_io

# --- Globals for this service ---
xt_m = None
md_socket = None
background_tasks = []
main_event_loop = None  # This will store the event loop for threadsafe operations
backend_ws_clients: Set[WebSocket] = set() # NEW: Sockets for backend clients (main app)

class SubscriptionItem(BaseModel):
    symbol: str
    tokens: List[int]

class SubscriptionRequest(BaseModel):
    subscriptions: List[SubscriptionItem]
class BulkDepthRequest(BaseModel):
    instruments: List[Dict]
class BulkLTPRequest(BaseModel):
    tokens: List[int]


# --- Socket Callbacks (logic from market_data/socket_callbacks.py) ---
def on_socket_connect():
    """Socket.IO connected"""
    state.socket_connected = True
    state.data_source = "WEBSOCKET"
    logger.info("✅ [MarketData Service] Socket CONNECTED. Data source is WEBSOCKET.")

def on_socket_disconnect():
    """Socket.IO disconnected"""
    state.socket_connected = False
    state.data_source = "REST_POLL"
    logger.warning("🔌 [MarketData Service] Socket DISCONNECTED. Data source changed to REST_POLL.")

def on_socket_error(error):
    """Socket.IO error"""
    logger.error(f"❌ [MarketData Service] Socket error: {error}")

def _queue_tick_data(data: dict):
    """Helper to put tick data onto the asyncio queue from a sync thread."""
    try:
        if not isinstance(data, dict): return
        token = data.get('ExchangeInstrumentID')
        ltp = data.get('LastTradedPrice') or data.get('Touchline', {}).get('LastTradedPrice')
        if token and ltp:
            # The process_market_data_queue task will handle updating the state.
            # This function's only job is to pass the data from the socket thread
            # to the asyncio event loop.
            if main_event_loop and hasattr(state, 'market_data_queue') and state.market_data_queue:
                asyncio.run_coroutine_threadsafe(
                    state.market_data_queue.put(data), main_event_loop
                )
    except Exception as e:
        logger.error(f"❌ [MarketData Service] Error queuing tick data: {e}")

def on_message1512_json_full(data):
    _queue_tick_data(data)

def on_message1512_json_partial(data):
    _queue_tick_data(data)

# --- NEW: Backend Broadcasting Logic ---
async def broadcast_to_backends(message: dict):
    """Broadcasts a message to all connected backend WebSocket clients."""
    disconnected_clients = set()
    for client in list(backend_ws_clients):
        try:
            await client.send_json(message)
        except (WebSocketDisconnect, ConnectionResetError, RuntimeError):
            disconnected_clients.add(client)
        except Exception as e:
            logger.error(f"Error broadcasting to backend client: {e}")
            disconnected_clients.add(client)

    for client in disconnected_clients:
        backend_ws_clients.discard(client)

async def broadcast_manager():
    """Listens on the broadcast queue and sends messages to backend clients."""
    logger.info("🚀 Starting Broadcast Manager for backend clients.")
    while True:
        try:
            message = await state.broadcast_queue.get()
            await broadcast_to_backends(message)
            state.broadcast_queue.task_done()
        except asyncio.CancelledError:
            logger.info("Broadcast Manager cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in broadcast_manager: {e}", exc_info=True)

async def process_and_broadcast_market_data_queue():
    """
    Replaces the old `process_market_data_queue`.
    This task processes ticks, updates local state, and batches updates for broadcasting.
    """
    logger.info("🚀 Starting Market Data Queue Processor with Broadcasting.")
    price_batch = {}
    last_broadcast_time = time.time()

    while True:
        try:
            # Wait for the first item, but then gather more for a short period.
            tick = await asyncio.wait_for(state.market_data_queue.get(), timeout=0.5)
            token = tick.get('ExchangeInstrumentID')
            ltp = tick.get('LastTradedPrice') or tick.get('Touchline', {}).get('LastTradedPrice')
            if token and ltp:
                # --- FIX: Use the shared data manager to update the shared price array ---
                if hasattr(state, 'shared_data') and state.shared_data:
                    state.shared_data.update_price(token, float(ltp))
                # --- END FIX ---
                price_batch[token] = float(ltp)
            state.market_data_queue.task_done()

            # Broadcast if batch is full or timer expires
            if len(price_batch) >= 200 or (time.time() - last_broadcast_time) > 0.2:
                if price_batch:
                    await state.broadcast_queue.put({'type': 'price_update', 'data': price_batch.copy()})
                    price_batch.clear()
                    last_broadcast_time = time.time()
        except asyncio.TimeoutError:
            # If the queue is empty for a bit, broadcast any remaining items in the batch.
            if price_batch:
                await state.broadcast_queue.put({'type': 'price_update', 'data': price_batch.copy()})
                price_batch.clear()
                last_broadcast_time = time.time()
        except asyncio.CancelledError:
            logger.info("Market Data Queue Processor cancelled.")
            break
        except Exception as e:
            logger.error(f"Error in market data queue processor: {e}", exc_info=True)
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan manager for the Market Data Service."""
    global xt_m, md_socket, background_tasks

    logger.info("="*100)
    logger.info("🚀 STARTING MARKET DATA MICROSERVICE")
    logger.info("="*100)

    try:
        # Initialize state queues for this service
        state.market_data_queue = asyncio.Queue(maxsize=config.MARKET_DATA_QUEUE_SIZE)
        state.broadcast_queue = asyncio.Queue() # NEW: For broadcasting to backend clients

        # --- FIX: Initialize Shared Data Manager ---
        # This service is the producer of market data, so it creates the shared memory.
        logger.info("🧠 Initializing Shared Data for Market Data Service (Attempting to attach)...")
        try:
            # Try to attach to existing memory first (created by main.py)
            state.shared_data = SharedDataManager(create=False)
            logger.info("✅ Attached to existing Shared Data Manager")
        except Exception as e:
            logger.warning(f"⚠️ Could not attach to existing Shared Data ({e}). Creating new...")
            state.shared_data = SharedDataManager(create=True)
            logger.info("✅ Created new Shared Data Manager")
            
        state.prices = state.shared_data.prices_array
        
        # --- FIX: Robust access to option_chains_proxy ---
        # Use getattr to safely check for the attribute, handling cases where it might be missing on the proxy object
        state.option_chains = getattr(state.shared_data, 'option_chains_proxy', None)
        if state.option_chains is None:
             logger.warning("⚠️ Shared option_chains_proxy not found. Falling back to local dictionary.")
             state.option_chains = {}
        logger.info("✅ Shared data structures ready for Market Data Service")
        
        # Market Data API Login
        logger.info("📈 Logging into XTS Market Data API...")
        xt_m = XTSConnect(cred.API_KEY_M, cred.API_SECRET_M, "WEBAPI")
        response_m = xt_m.marketdata_login()
        if response_m.get('type') != 'success':
            raise Exception(f"Market data login failed: {response_m.get('description', 'Unknown error')}")
        logger.info("✅ Market Data API logged in")
        
        token = response_m['result']['token']
        user_id = response_m['result']['userID']

        # Socket.IO Client Initialization
        logger.info("🔌 Initializing Socket.IO client...")
        md_socket = MDSocket_io(token, user_id)
        md_socket.on_connect = on_socket_connect
        md_socket.on_disconnect = on_socket_disconnect
        md_socket.on_error = on_socket_error
        md_socket.on_message1512_json_full = on_message1512_json_full
        md_socket.on_message1512_json_partial = on_message1512_json_partial
        logger.info("✅ Socket callbacks configured")

        # Set global instances for modules used by this service
        global main_event_loop; main_event_loop = asyncio.get_event_loop() # Set local main_event_loop
        set_xts_instances(xt_m, md_socket)

        # Start Background Tasks
        logger.info("🔄 Starting background tasks for Market Data Service...")
        task1 = asyncio.create_task(process_and_broadcast_market_data_queue()) # REPLACED
        task2 = asyncio.create_task(rest_polling_loop()) # This can still run for fallbacks
        task3 = asyncio.create_task(update_option_chain_cache_loop())
        task4 = asyncio.create_task(monitor_xts_socket_status())
        task5 = asyncio.create_task(calculate_greeks_loop()) # NEW: Real-time greeks calculator
        task6 = asyncio.create_task(broadcast_manager()) # NEW
        background_tasks.extend([task1, task2, task3, task4, task5, task6])
        logger.info("✅ Market Data background tasks running")

        # Connect Socket.IO in a separate thread
        def socket_thread_func():
            try:
                logger.info("   🔗 Socket thread started for Market Data Service")
                md_socket.connect()
            except Exception as e:
                logger.error(f"   ❌ Socket thread error: {e}")
        
        socket_thread = threading.Thread(target=socket_thread_func, daemon=True)
        socket_thread.start()
        logger.info("✅ Socket thread launched")
        
        await asyncio.sleep(5) # Wait for socket to connect

        logger.info("="*100)
        logger.info("✅ MARKET DATA SERVICE READY")
        logger.info(f"   Listening on: http://localhost:{config.MARKET_DATA_PORT}")
        logger.info("="*100)

    except Exception as e:
        logger.error(f"❌ MARKET DATA SERVICE STARTUP FAILED: {e}", exc_info=True)
        raise

    yield

    # Shutdown sequence
    logger.info("="*100)
    logger.info("🛑 SHUTTING DOWN MARKET DATA SERVICE")
    logger.info("="*100)
    
    for task in background_tasks:
        if not task.done():
            task.cancel()
    
    if background_tasks:
        await asyncio.gather(*background_tasks, return_exceptions=True)
    
    if md_socket:
        md_socket.disconnect()
    
    logger.info("✅ MARKET DATA SERVICE SHUTDOWN COMPLETE")

app = FastAPI(
    title="Market Data Microservice",
    description="Provides live option chains, greeks, and prices.",
    version="1.0.0",
    lifespan=lifespan
)

# Get the main app's port from config to build the origins list
main_app_port = getattr(config, 'PORT', 8000)

# Define allowed origins. This is critical when allow_credentials=True.
# The wildcard '*' is not permitted by browsers when credentials are included.
# We include variations for browser access (with port) and backend access (without port).
allowed_origins = [
    f"http://localhost:{main_app_port}",
    f"http://127.0.0.1:{main_app_port}",
    "http://localhost", # Origin for the backend client (main app)
    "http://127.0.0.1",  # Origin for the backend client (main app)
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API Endpoints for the service ---

@app.get("/api/health")
async def health():
    return {
        "status": "ok",
        "service": "Market Data",
        "socket_connected": state.socket_connected,
        "cached_prices": len(state.prices),
        "cached_chains": list(state.option_chains.keys())
    }

@app.get("/api/option-chain/{symbol}")
async def api_get_option_chain_data(symbol: str):
    # The get_option_chain from trading.chain_provider is a blocking, synchronous function.
    # It will build the chain if it's not in the cache. It also now handles broadcasting.
    # We run it in an executor to avoid blocking the service's event loop.
    logger.info(f"📥 API Request: Get option chain for {symbol}")
    loop = asyncio.get_event_loop()
    chain = await loop.run_in_executor(None, build_get_option_chain, symbol.upper())

    if not chain:
        logger.error(f"❌ Failed to build option chain for {symbol}")
        # If even the build fails, return an error. This is now a real failure, not just a cache miss.
        # The client expects a JSON response, not necessarily an HTTP error code, so we'll stick to that pattern
        # but provide a more informative error.
        return {"success": False, "error": f"Failed to build or find option chain for {symbol} after attempting build."}

    # The background task will still keep the cache warm, so most calls will be fast.
    # This change just handles the startup race condition and provides a fallback if the cache is ever stale.
    return {"success": True, "data": chain}

@app.websocket("/ws/data")
async def backend_websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for backend clients (e.g., the main application)."""
    # Log the headers from the incoming handshake request for debugging purposes.
    # This helps verify that headers like 'Origin' are being received correctly.
    headers = dict(websocket.scope['headers'])
    client_host = websocket.client.host
    logger.info(f"--- WebSocket connection request received for /ws/data from {client_host} ---")
    logger.info(f"Handshake Headers: {headers}")

    await websocket.accept()
    backend_ws_clients.add(websocket)
    logger.info(f"✅ Backend WebSocket client connected from {client_host}. Total: {len(backend_ws_clients)}")
    try:
        while True:
            await websocket.receive_text()  # Keep connection alive
    except WebSocketDisconnect:
        backend_ws_clients.discard(websocket)
        logger.warning(f"🔌 Backend WebSocket client from {client_host} disconnected. Total: {len(backend_ws_clients)}")

@app.get("/api/prices")
async def api_get_prices():
    # This endpoint is superseded by /api/bulk-ltp, but let's make it consistent
    return {"success": True, "data": state.prices}

@app.get("/api/spot-details/{symbol}")
async def api_get_spot_details(symbol: str):
    # This function now lives in the service, so we can call it directly
    # We run it in an executor because get_spot_details can make blocking network calls
    loop = asyncio.get_event_loop()
    details = await loop.run_in_executor(None, get_spot_details, symbol)
    if not details:
        return {"success": False, "error": f"Spot details for {symbol} could not be determined."}
    return {"success": True, "data": details}

@app.get("/api/ltp/{segment}/{token}")
async def api_get_ltp(segment: int, token: int):
    """Returns the last traded price for a single token from the cache."""
    price = state.get_price(token)
    if price is None:
        # Fallback to REST call if not in cache
        # Use the service's internal (sync) get_ltp function from the correct provider
        from trading.chain_provider import get_ltp as fetch_ltp
        loop = asyncio.get_event_loop()
        # Run the synchronous broker API call in a thread to avoid blocking the service
        price = await loop.run_in_executor(None, fetch_ltp, token, segment)
        if price == 0.0:
            raise HTTPException(status_code=404, detail=f"Price for token {token} not found in cache or via REST.")
    return {"success": True, "ltp": price}

@app.post("/api/subscribe")
async def api_subscribe_instruments(request: SubscriptionRequest):
    """Subscribes to a list of instrument tokens, grouped by symbol."""
    if not md_socket or not md_socket.sid.connected:
        raise HTTPException(status_code=503, detail="Market data socket not connected.")

    # This is the new, robust logic.
    # It uses the provided symbol to determine the segment, instead of relying on a fragile cache lookup.
    from trading.chain_provider import SYMBOL_CONFIG

    instruments_by_segment = {}
    for sub_item in request.subscriptions:
        symbol_upper = sub_item.symbol.upper()
        
        # Find the base symbol (e.g., "NIFTY" from "NIFTY 50")
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        
        if not base_symbol:
            logger.warning(f"Subscription skipped for symbol '{symbol_upper}': Not found in SYMBOL_CONFIG.")
            continue
            
        segment = SYMBOL_CONFIG[base_symbol].get('segment')
        if not segment:
            logger.warning(f"Subscription skipped for symbol '{symbol_upper}': No segment defined in SYMBOL_CONFIG.")
            continue
        
        if segment not in instruments_by_segment:
            instruments_by_segment[segment] = []
        
        # Add all tokens for this symbol to the correct segment group
        instruments_by_segment[segment].extend(sub_item.tokens)

    success_count = 0
    failed_segments = []
    for segment, tokens in instruments_by_segment.items():
        # Remove duplicates
        unique_tokens = list(set(tokens))
        instruments_payload = [{'exchangeSegment': segment, 'exchangeInstrumentID': t} for t in unique_tokens]
        # md_socket.send_subscription is a synchronous call in the provided library
        response = md_socket.send_subscription(instruments_payload, config.MESSAGE_CODE_LTP)
        if response and response.get('type') == 'success':
            success_count += len(unique_tokens)
            for token in unique_tokens:
                state.add_subscription(token) # Track subscribed tokens
        else:
            failed_segments.append(segment)
            logger.error(f"Failed to subscribe to {len(unique_tokens)} instruments for segment {segment}.")

    if failed_segments:
        return {"success": False, "error": f"Subscription failed for segments: {failed_segments}"}

    return {"success": True, "message": f"Subscription request for {success_count} instruments sent."}

@app.post("/api/bulk-ltp")
async def api_get_bulk_ltp(request: BulkLTPRequest):
    """Returns the last traded price for a list of tokens from the cache."""
    prices = {token: state.get_price(token) for token in request.tokens if state.get_price(token) is not None}
    return {"success": True, "data": prices}

@app.post("/api/bulk-market-depth")
async def api_get_bulk_market_depth(request: BulkDepthRequest):
    """
    Returns the L1 market depth (bid/ask) for a list of instruments.
    This is called by the main app's OrderExecutor to calculate limit prices.
    """
    from trading.chain_provider import get_bulk_market_depth as fetch_bulk_depth
    
    loop = asyncio.get_event_loop()
    # The chain_provider's get_bulk_market_depth is a synchronous function that calls the broker API.
    # We run it in an executor to avoid blocking the service's event loop.
    depth_map = await loop.run_in_executor(None, fetch_bulk_depth, request.instruments)
    return {"success": True, "data": depth_map}

if __name__ == "__main__":
    import uvicorn
    # Make sure you have added MARKET_DATA_PORT = 8001 to your config.py
    logger.info(f"🚀 Starting Market Data Service on port {config.MARKET_DATA_PORT}")
    uvicorn.run(
        "marketdata_service:app",
        host=config.HOST,
        port=config.MARKET_DATA_PORT,
        log_level="info"
    )
