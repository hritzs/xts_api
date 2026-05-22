"""
api/websocket.py — WebSocket Handler

Manages frontend WebSocket connections.
On connect: immediately pushes cached chain headers so UI shows Spot/Syn.Fut
            without waiting for next tick.
chain_header_update messages flow through broadcast_message() same as any
other message — no filtering needed here.
"""
import asyncio
from typing import Set, Dict
from fastapi import WebSocket, WebSocketDisconnect
from utils.logger import logger
from background.tasks import broadcast_message


# ── Connected clients ─────────────────────────────────────────────────────────
websocket_clients: Set[WebSocket] = set()

# ── Chain header cache ────────────────────────────────────────────────────────
# Stores latest chain_header_update per symbol so new connections get
# immediate Spot / Syn.Fut values without waiting for the next tick.
_last_chain_headers: Dict[str, dict] = {}


def update_chain_header_cache(msg: dict):
    """
    Called by background/tasks.py whenever a chain_header_update arrives
    from ZMQ. Keeps _last_chain_headers up to date so new WS clients can
    be seeded on connect.
    """
    symbol = msg.get('symbol')
    if symbol:
        _last_chain_headers[symbol] = msg


async def _seed_new_client(websocket: WebSocket):
    """
    Push all cached chain headers to a freshly connected client so the
    UI header (Spot / Syn.Fut / ATM) is populated immediately.
    """
    if not _last_chain_headers:
        return
    try:
        for msg in _last_chain_headers.values():
            await asyncio.wait_for(websocket.send_json(msg), timeout=2.0)
    except Exception as e:
        logger.warning(f"⚠️ Could not seed chain headers to new client: {e}")


async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates.

    On connect:
      1. Accept and register client.
      2. Immediately push cached chain headers (Spot / Syn.Fut).
    While open:
      - Keeps connection alive; all broadcasts come from broadcast_message().
    """
    await websocket.accept()
    websocket_clients.add(websocket)
    logger.info(f"🔌 Frontend WebSocket connected (Total: {len(websocket_clients)})")

    # Seed the new client with last-known Spot / Syn.Fut immediately
    await _seed_new_client(websocket)

    try:
        while True:
            # Keep alive — receive client pings / close frames
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info("🔌 Frontend WebSocket disconnected")
    except asyncio.CancelledError:
        logger.info("🔌 WebSocket cancelled during shutdown")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {type(e).__name__} - {e}")
    finally:
        if websocket in websocket_clients:
            websocket_clients.remove(websocket)
        logger.info(f"🔌 WebSocket cleaned up (Remaining: {len(websocket_clients)})")
