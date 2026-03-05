"""
Order Book Service — Dedicated Process (Port 8002)
Fetches order book every 5s, serves via HTTP/WebSocket.
Reads login token from shared DB — no conflicts.
"""
import asyncio
import json
import sqlite3
import os
from datetime import datetime
from typing import Dict, List, Optional

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

import config
from utils.logger import logger
from models.state import state
from database.db_manager import Database

app = FastAPI(title="Order Book Service", version="1.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

websocket_clients: List[WebSocket] = []
last_order_book: Optional[List[Dict]] = None
last_fetch_time: Optional[datetime] = None
login_token_data: Optional[Dict] = None

TOKEN_DB = os.path.abspath("shared_tokens.db")

def get_login_token_data() -> Optional[Dict]:
    """Read login token data from shared SQLite DB."""
    global login_token_data
    try:
        conn = sqlite3.connect(TOKEN_DB)
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS tokens (key TEXT PRIMARY KEY, value TEXT, timestamp DATETIME)")
        cursor.execute("SELECT value FROM tokens WHERE key = 'xts_interactive_token'")
        result = cursor.fetchone()
        conn.close()
        
        if result:
            token_data = json.loads(result[0])
            if token_data.get('token'):
                login_token_data = token_data
                return login_token_data
    except Exception as e:
        logger.error(f"❌ OrderBookService: Failed to load token: {e}")
    return None

async def refresh_order_book():
    """Fetch order book using shared token."""
    global last_order_book, last_fetch_time
    
    token_data = get_login_token_data()
    if not token_data:
        logger.error("OrderBookService: No login token available")
        return
    
    try:
        from trading.order_manager import get_order_book_with_token
        # Run in executor to avoid blocking
        orders = await asyncio.get_event_loop().run_in_executor(
            None, 
            lambda: get_order_book_with_token(token_data['token'], token_data.get('userID'), token_data.get('isInvestorClient'))
        )
        
        if orders is not None:
            last_order_book = orders
            last_fetch_time = datetime.now()
            
            # Update DB
            if hasattr(state, 'db') and state.db:
                await asyncio.get_event_loop().run_in_executor(None, state.db.insert_orders_bulk, last_order_book)
            
            logger.info(f"📋 OrderBookService: Refreshed {len(last_order_book)} orders")
            
            # Broadcast
            msg = {"count": len(last_order_book), "timestamp": last_fetch_time.isoformat()}
            for client in websocket_clients[:]:
                try:
                    await client.send_json(msg)
                except:
                    websocket_clients.remove(client)
    except Exception as e:
        logger.error(f"❌ OrderBookService: Fetch failed: {e}")

@app.get("/orderbook")
async def api_get_order_book():
    # If data is stale (> 2s), try refresh, but don't block indefinitely
    if not last_fetch_time or (datetime.now() - last_fetch_time).total_seconds() > 2:
        await refresh_order_book()
    return {"order_book": last_order_book or [], "timestamp": last_fetch_time.isoformat() if last_fetch_time else None}

@app.websocket("/ws")
async def ws_orderbook(websocket: WebSocket):
    await websocket.accept()
    websocket_clients.append(websocket)
    try:
        while True:
            if last_order_book is not None:
                await websocket.send_json({"count": len(last_order_book), "timestamp": last_fetch_time.isoformat() if last_fetch_time else None})
            await asyncio.sleep(3)
    except WebSocketDisconnect:
        websocket_clients.remove(websocket)

@app.on_event("startup")
async def startup_event():
    # Initialize DB connection for this process
    state.db = Database()
    logger.info("✅ OrderBookService: Database connected")
    await refresh_order_book()
    asyncio.create_task(background_loop())

async def background_loop():
    while True:
        await refresh_order_book()
        await asyncio.sleep(5)

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8002)
