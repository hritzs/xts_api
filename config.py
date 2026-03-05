"""
Configuration and Constants
"""
from datetime import timezone, timedelta
from typing import Final

# Timezone
IST: Final = timezone(timedelta(hours=5, minutes=30))

# Exchange Segments
EXCHANGE_NSEFO: Final = 2
EXSEG: Final = EXCHANGE_NSEFO  # For backward compatibility

# Market Data Message Codes
MESSAGE_CODE_LTP: Final = 1512# c:\Users\Administrator\Desktop\api_v2_microservices\config.py

# ... (other existing configurations) ...

# Host for the main application and market data service
HOST = "127.0.0.1" # Or "localhost", or your desired host IP

# Port for the main FastAPI application
PORT = 5000 # Or your desired port for the main app

# Port for the Market Data Microservice
MARKET_DATA_PORT = 8001 # This is the missing attribute

# Port for the Order Book Service
ORDER_SERVICE_PORT = 8002

# Port for the Snapshot Service
SNAPSHOT_SERVICE_PORT = 8003


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

# Database
DATABASE_NAME: Final = "straddle_trades.db"

# Server
HOST: Final = "0.0.0.0"
PORT: Final = 5000
