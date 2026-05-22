"""
test_websocket_feed.py

Connects to the XTS Market Data WebSocket and subscribes to all cash indices
to diagnose which ones are providing real-time ticks.

Usage: python test_websocket_feed.py
"""
import time
import json
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

try:
    from Connect import XTSConnect
    from MarketDataSocketClient import MDSocket_io
    import cred
except ImportError as e:
    print(f"Error: Missing required libraries. Please ensure all project files are accessible. Details: {e}")
    sys.exit(1)

# --- Configuration ---
# Tokens from your trading/chain_provider.py
# (Segment, Token)
INSTRUMENTS_TO_TEST = {
    "NIFTY":      (1, 26000),
    "BANKNIFTY":  (1, 26001),
    "FINNIFTY":   (1, 26034),
    "MIDCPNIFTY": (1, 26121),
    "SENSEX":     (11, 26065),
    "BANKEX":     (11, 26118),
}

# --- Main Logic ---
def main():
    logging.info("🚀 Starting WebSocket Data Feed Test...")

    # 1. Login to Market Data API
    try:
        xt_m = XTSConnect(cred.API_KEY_M, cred.API_SECRET_M, source="WEBAPI")
        response_m = xt_m.marketdata_login()
        if response_m.get('type') != 'success':
            logging.error(f"❌ Market data login failed: {response_m.get('description')}")
            return
        logging.info("✅ Market Data API Login Successful.")
        xts_token = response_m['result']['token']
        user_id = response_m['result']['userID']
    except Exception as e:
        logging.error(f"❌ Exception during login: {e}")
        return

    # 2. Setup WebSocket Client
    md_socket = MDSocket_io(xts_token, user_id)

    # --- Define WebSocket event handlers ---
    def on_connect():
        logging.info("✅ WebSocket CONNECTED!")
        
        # Subscribe to cash indices (1510)
        cash_instruments = [
            {'exchangeSegment': seg, 'exchangeInstrumentID': token}
            for name, (seg, token) in INSTRUMENTS_TO_TEST.items()
        ]
        logging.info(f"📡 Subscribing to {len(cash_instruments)} cash indices with message code 1510...")
        md_socket.send_subscription(cash_instruments, 1510)

    def on_disconnect():
        logging.warning("🔌 WebSocket DISCONNECTED.")

    def on_error(error):
        logging.error(f"❌ WebSocket ERROR: {error}")

    def on_1510_full(data):
        token = data.get('ExchangeInstrumentID')
        value = data.get('IndexValue')
        name = next((n for n, (s, t) in INSTRUMENTS_TO_TEST.items() if t == token), "UNKNOWN")
        logging.info(f"🔔 [1510] TICK for {name:<10} (Token: {token}) -> IndexValue: {value}")

    # Assign handlers
    md_socket.on_connect = on_connect
    md_socket.on_disconnect = on_disconnect
    md_socket.on_error = on_error
    md_socket.on_message1510_json_full = on_1510_full

    # 3. Connect and run
    try:
        md_socket.connect()
    except KeyboardInterrupt:
        logging.info("🛑 Test stopped by user.")
    finally:
        logging.info("👋 Shutting down...")
        md_socket.disconnect()

if __name__ == "__main__":
    main()