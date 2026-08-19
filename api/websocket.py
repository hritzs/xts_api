"""
api/websocket.py — WebSocket Handler

Manages frontend WebSocket connections.

Single-source-of-truth rule for option-chain UI:
- On connect, seed the client with the latest published option_chain_update snapshots.
- Do NOT seed special chain_header_update messages for option-chain hydration.
- All real-time messages still flow through broadcast_message().
"""

import asyncio
import traceback
from typing import Set

from fastapi import WebSocket, WebSocketDisconnect

from utils.logger import logger
from background.tasks import broadcast_message  # keep re-export for api/__init__.py compatibility
from models.state import state


# ── Connected clients ─────────────────────────────────────────────────────────
websocket_clients: Set[WebSocket] = set()


async def _seed_new_client(websocket: WebSocket):
    """
    Push all cached published option-chain snapshots to a freshly connected client
    so the UI header/table/ATM are populated immediately from the canonical backend snapshot.
    """
    published = getattr(state, "published_option_chains", {}) or {} # Ensure this exists on state
    if not published:
        return

    try:
        for snapshot in published.values():
            if not snapshot:
                continue

            # The frontend expects the snapshot to be inside a 'data' key.
            # The snapshot itself is the canonical object.
            await asyncio.wait_for(
                websocket.send_json({
                    "type": "option_chain_update",
                    "data": snapshot
                }),
                timeout=5.0 # Increased timeout for potentially large initial payload
            )
    except WebSocketDisconnect:
        logger.info("🔌 Client disconnected while seeding published option-chain snapshots")
    except asyncio.CancelledError:
        logger.info("🔌 Published option-chain seeding cancelled during shutdown")
        raise
    except Exception as e:
        tb = traceback.format_exc()
        logger.warning(f"⚠️ Could not seed published option-chain snapshots to new client: {e}\n{tb}")


async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates.

    On connect:
      1. Accept and register client.
      2. Immediately push published option-chain snapshots.

    While open:
      - Keeps connection alive; all broadcasts come from broadcast_message().
    """
    await websocket.accept()
    websocket_clients.add(websocket)
    logger.info(f"🔌 Frontend WebSocket connected (Total: {len(websocket_clients)})")

    await _seed_new_client(websocket)

    try:
        while True:
            await websocket.receive_text()

    except WebSocketDisconnect:
        logger.info("🔌 Frontend WebSocket disconnected")
    except asyncio.CancelledError:
        logger.info("🔌 WebSocket cancelled during shutdown")
        raise
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(f"❌ WebSocket error: {type(e).__name__} - {e}\n{tb}")
    finally:
        websocket_clients.discard(websocket)
        logger.info(f"🔌 WebSocket cleaned up (Remaining: {len(websocket_clients)})")