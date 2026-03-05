import asyncio
import time
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any
import config
from utils.logger import logger

app = FastAPI(title="Snapshot Service", version="1.0.0")

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Connected WebSocket clients
clients: List[WebSocket] = []
last_log_time = 0

class SnapshotBatch(BaseModel):
    updates: List[Dict[str, Any]]

@app.websocket("/ws/snapshots")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    clients.append(websocket)
    logger.info(f"✅ Client connected to Snapshot Service. Total: {len(clients)}")
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.remove(websocket)
        logger.info(f"🔌 Client disconnected from Snapshot Service. Total: {len(clients)}")

@app.post("/api/push-snapshots")
async def push_snapshots(batch: SnapshotBatch):
    """
    Endpoint for the main application to push aggregated snapshots.
    This service then broadcasts them to all connected UI clients.
    """
    global last_log_time
    current_time = time.time()
    if current_time - last_log_time > 10:  # Log status every 10 seconds to show activity
        if clients:
            logger.info(f"⚡ Snapshot Service Active: Broadcasting {len(batch.updates)} updates to {len(clients)} clients.")
        elif batch.updates:
            logger.info(f"💤 Snapshot Service Idle: Receiving {len(batch.updates)} updates, but no clients connected.")
        last_log_time = current_time

    if not clients:
        return {"status": "ok", "message": "No clients connected"}
    
    message = {
        'type': 'pnl_batch_update',
        'data': batch.updates
    }
    
    disconnected = []
    for client in clients:
        try:
            await client.send_json(message)
        except Exception as e:
            logger.warning(f"Failed to send to client: {e}")
            disconnected.append(client)
    
    for client in disconnected:
        if client in clients:
            clients.remove(client)
            
    return {"status": "ok", "broadcast_count": len(clients)}

@app.get("/health")
async def health():
    return {"status": "ok", "clients": len(clients)}

if __name__ == "__main__":
    import uvicorn
    # Use a distinct port for the snapshot service (e.g., 8003)
    # Ensure this port is added to your config.py or handled dynamically
    port = getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)
    logger.info(f"🚀 Starting Snapshot Service on port {port}")
    uvicorn.run(app, host="0.0.0.0", port=port)