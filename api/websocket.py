"""
WebSocket Handler - Fixed Graceful Shutdown
"""
import asyncio
import json
import time
from typing import Set
from fastapi import WebSocket, WebSocketDisconnect
from utils.logger import logger
from background.tasks import broadcast_message # FIX: Re-export to solve ImportError


# Global set of connected WebSocket clients
websocket_clients: Set[WebSocket] = set()


async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time updates
    
    Fixed: Graceful shutdown handling
    """
    await websocket.accept()
    websocket_clients.add(websocket)
    
    logger.info(f"🔌 Frontend WebSocket connected (Total: {len(websocket_clients)})")
    
    try:
        while True:
            # Wait for incoming messages (e.g. client pongs or close)
            # This keeps the connection open without sending data from this coroutine,
            # preventing concurrent write errors with the broadcast task.
            await websocket.receive_text()
                
    except WebSocketDisconnect:
        logger.info("🔌 Frontend WebSocket disconnected")
    except asyncio.CancelledError:
        logger.info("🔌 WebSocket cancelled during shutdown")
    except Exception as e:
        logger.error(f"❌ WebSocket error: {type(e).__name__} - {e}")
    finally:
        # Clean up
        if websocket in websocket_clients:
            websocket_clients.remove(websocket)
        logger.info(f"🔌 WebSocket cleaned up (Remaining: {len(websocket_clients)})")
