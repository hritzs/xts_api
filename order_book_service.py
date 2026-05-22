"""
Order Book Service — Dedicated Process (Port 8002)
Fetches order book every 1s (was 5s), serves via HTTP and ZMQ REP.
"""
import asyncio
import json
import sqlite3
import os
import zmq
import zmq.asyncio
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
app.add_middleware(
    CORSMiddleware, allow_origins=["*"],
    allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

websocket_clients:  List[WebSocket]  = []
last_order_book:    Optional[List[Dict]] = None
last_fetch_time:    Optional[datetime]   = None
login_token_data:   Optional[Dict]       = None
_xt_instance = None   # ← cached XTSConnect instance, never recreated

TOKEN_DB = os.path.abspath("shared_tokens.db")

REFRESH_INTERVAL = 1.0   # ← was 5s, now 1s


def get_login_token_data() -> Optional[Dict]:
    global login_token_data
    try:
        conn   = sqlite3.connect(TOKEN_DB)
        cursor = conn.cursor()
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS tokens "
            "(key TEXT PRIMARY KEY, value TEXT, timestamp DATETIME)"
        )
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


def _get_or_create_xt_instance():
    """Return cached XTSConnect instance — create only once, reuse forever."""
    global _xt_instance
    if _xt_instance is not None:
        return _xt_instance
    token_data = get_login_token_data()
    if not token_data:
        return None
    try:
        from Connect import XTSConnect
        import cred
        xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        xt._set_common_variables(
            token_data['token'],
            token_data.get('userID'),
            token_data.get('isInvestorClient')
        )
        xt.isInvestorClient = False
        _xt_instance = xt
        logger.info("✅ OrderBookService: XTSConnect instance created and cached.")
        return _xt_instance
    except Exception as e:
        logger.error(f"❌ OrderBookService: Failed to create XTSConnect: {e}")
        return None


async def refresh_order_book():
    global last_order_book, last_fetch_time, _xt_instance
    xt = _get_or_create_xt_instance()
    if not xt:
        logger.error("OrderBookService: No XTS instance available")
        return

    try:
        import cred
        loop = asyncio.get_event_loop()

        def _fetch():
            try:
                client_id = getattr(cred, 'clientID', None)
                response  = xt.get_order_book(clientID=client_id)
                if response and response.get('type') == 'success':
                    result = response.get('result', {})
                    if isinstance(result, dict):
                        return result.get('orderList', []) or result.get('OrderList', [])
                    elif isinstance(result, list):
                        return result
            except Exception as e:
                logger.warning(f"OrderBookService: fetch error — {e}")
                # If token expired, reset instance so next call recreates
                global _xt_instance
                _xt_instance = None
            return None

        orders = await loop.run_in_executor(None, _fetch)

        if orders is not None:
            last_order_book = orders
            last_fetch_time = datetime.now()

            if hasattr(state, 'db') and state.db:
                await loop.run_in_executor(None, state.db.insert_orders_bulk, last_order_book)

            logger.debug(f"📋 OrderBookService: {len(last_order_book)} orders refreshed")

            # Notify WS clients
            msg = {"count": len(last_order_book), "timestamp": last_fetch_time.isoformat()}
            dead = []
            for client in websocket_clients:
                try:
                    await client.send_json(msg)
                except Exception:
                    dead.append(client)
            for c in dead:
                if c in websocket_clients:
                    websocket_clients.remove(c)

    except Exception as e:
        logger.error(f"❌ OrderBookService: Refresh failed: {e}")


@app.get("/orderbook")
async def api_get_order_book():
    """Return latest cached order book, refreshing if stale > 1s."""
    if not last_fetch_time or (datetime.now() - last_fetch_time).total_seconds() > 1:
        await refresh_order_book()
    return {
        "order_book": last_order_book or [],
        "timestamp":  last_fetch_time.isoformat() if last_fetch_time else None,
    }


@app.websocket("/ws")
async def ws_orderbook(websocket: WebSocket):
    await websocket.accept()
    websocket_clients.append(websocket)
    try:
        while True:
            if last_order_book is not None:
                await websocket.send_json({
                    "count":     len(last_order_book),
                    "timestamp": last_fetch_time.isoformat() if last_fetch_time else None,
                })
            await asyncio.sleep(2)
    except WebSocketDisconnect:
        if websocket in websocket_clients:
            websocket_clients.remove(websocket)


async def zmq_rep_server():
    """
    ZMQ REP server — serves cached order book directly to verifier_service.
    No HTTP overhead. Reuses in-memory cache.
    """
    ctx = zmq.asyncio.Context.instance()
    socket = ctx.socket(zmq.REP)
    socket.bind(f"tcp://*:{config.ZMQ_ORDERBOOK_REQ_PORT}")
    logger.info(f"📡 OrderBookService: ZMQ REP listening on port {config.ZMQ_ORDERBOOK_REQ_PORT}")
    while True:
        try:
            req = await socket.recv_json()
            if req.get("command") == "get_order_book":
                await socket.send_json({
                    "order_book": last_order_book or [],
                    "timestamp":  last_fetch_time.isoformat() if last_fetch_time else None,
                })
        except Exception as e:
            logger.error(f"❌ ZMQ REP error: {e}")
            try:
                await socket.send_json({"order_book": [], "error": str(e)})
            except Exception:
                pass


@app.on_event("startup")
async def startup_event():
    state.db = Database()
    logger.info("✅ OrderBookService: Database connected")
    await refresh_order_book()
    asyncio.create_task(background_loop())
    asyncio.create_task(zmq_rep_server())   # ← start ZMQ server alongside HTTP


async def background_loop():
    while True:
        await refresh_order_book()
        await asyncio.sleep(REFRESH_INTERVAL)   # ← 1s, was 5s


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8002)
