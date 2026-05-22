"""
Configuration and Constants
"""
from datetime import timedelta, timezone
from typing import Final

# Timezone
IST: Final = timezone(timedelta(hours=5, minutes=30))

# Server
HOST: Final = "0.0.0.0"

# Exchange Segments
EXCHANGE_NSEFO: Final = 2
EXSEG: Final = EXCHANGE_NSEFO  # For backward compatibility

# Market Data Message Codes
MESSAGE_CODE_LTP: Final = 1512

# Port for the main FastAPI application
PORT: Final = 5000

# ── HTTP Service Ports ───────────────────────────────────────────────────────
# These are for services that still expose a UI or health check via HTTP
MARKET_DATA_PORT: Final = 8001
ORDER_SERVICE_PORT: Final = 8002
SNAPSHOT_SERVICE_PORT: Final = 8003
VERIFIER_SERVICE_PORT: Final = 8004

# ── ZMQ Communication Ports ──────────────────────────────────────────────────
# Used for inter-service communication. All ports must be unique.
ZMQ_MARKETDATA_REQ_PORT: Final = 5560   # REQ/REP  — queries (md_service)
ZMQ_MARKETDATA_PUB_PORT: Final = 5561   # PUB/SUB  — price broadcasts (md_service)
ZMQ_MARKETDATA_SUB_PORT: Final = 5562   # PUSH/PULL — subscription commands (md_service)
ZMQ_TICK_PUB_PORT: Final = 5563         # PUB/SUB  — tick signals → run_dev (md_service)
ZMQ_FILLS_PUB_PORT: Final = 5564        # PUB/SUB  — fills → run_dev (reconciler)
ZMQ_VERIFIER_PULL_PORT: Final = 5565    # PULL     — job submission → reconciler (reconciler)
ZMQ_SNAPSHOT_PULL_PORT: Final = 5566      # PULL - verification completion -> snapshot_service
ZMQ_SNAPSHOT_FORCE_PULL_PORT: Final = 5567 # PULL - force snapshot -> snapshot_service
ZMQ_ORDERBOOK_REQ_PORT  = 5569

# Option Chain Settings
STRIKE_GAPS: Final = {
    'NIFTY': 50,
    'BANKNIFTY': 100,
    'FINNIFTY': 50,
    'MIDCPNIFTY': 25,
    'SENSEX': 100
}

# Trading Settings
DEFAULT_LOTS: Final = 1
DEFAULT_PRODUCT_TYPE: Final = "MIS"
DEFAULT_ORDER_TYPE: Final = "MARKET" # This is converted to LIMIT before placing
# Maximum quantity per single order to avoid broker limits. This is a fixed value for F&O.
MAX_ORDER_QTY: Final = 1800

# Queue Settings
MARKET_DATA_QUEUE_SIZE: Final = 2000

# Update Intervals (seconds)
ORDER_BOOK_UPDATE_INTERVAL: Final = 3
PNL_UPDATE_INTERVAL: Final = 1
REST_POLLING_INTERVAL: Final = 1

# Risk-free rate for Greeks
RISK_FREE_RATE: Final = 0.0
# In config.py
ZMQ_RECONCILER_REQ_PORT = 5568
# Database
# This file is assumed to exist and contain configuration variables.

# FastAPI main app port
PORT = 5000
HOST = "0.0.0.0"

# Microservice HTTP ports (for health checks and direct API calls)
MARKET_DATA_PORT = 8001
SNAPSHOT_SERVICE_PORT = 8003
VERIFIER_SERVICE_PORT = 8004 # Assuming reconciler has an HTTP endpoint for status/debug

# ZMQ Ports
ZMQ_MARKETDATA_REQ_PORT = 5560
ZMQ_MARKETDATA_PUB_PORT = 5561
ZMQ_MARKETDATA_PULL_PORT = 5562 # For market data service to pull requests
ZMQ_TICK_PUB_PORT = 5563 # For market data service to publish ticks

ZMQ_FILLS_PUB_PORT = 5564 # Order Reconciler publishes order fills/updates (subscribed by main app)
ZMQ_VERIFIER_PULL_PORT = 5565 # Order Reconciler pulls verification jobs from main app
ZMQ_SNAPSHOT_PULL_PORT = 5566 # Snapshot Service pulls verification completion notifications from main app
ZMQ_SNAPSHOT_FORCE_PULL_PORT = 5567 # Snapshot Service pulls force snapshot requests from main app

# Database Name
DATABASE_NAME = "straddle_trades.db"
DATABASE_NAME: Final = 'straddle_trades.db'
