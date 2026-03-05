# c:\Users\Administrator\Desktop\api_v2_microservices\market_data\data_client.py
import httpx
import asyncio
import websockets
import json
from typing import Dict, Optional, List
from utils.logger import logger
from models.state import state
import config

# Determine the correct host to connect to.
# A server might listen on '0.0.0.0' (all interfaces), but a client must connect to a specific IP.
# When running locally, this should be '127.0.0.1'.
_server_host = getattr(config, 'HOST', '127.0.0.1')
_client_connect_host = '127.0.0.1' if _server_host == '0.0.0.0' else _server_host

# Use a single, reusable async client for efficiency.
# The base URL is constructed from config, making it easy to change.
_http_client = httpx.AsyncClient(
    base_url=f"http://{_client_connect_host}:{config.MARKET_DATA_PORT}",
    timeout=10.0
)

async def get_option_chain_from_service(symbol: str) -> Optional[Dict]:
    """Fetches the option chain from the dedicated market data service."""
    try:
        response = await _http_client.get(f"/api/option-chain/{symbol.upper()}")
        response.raise_for_status()
        data = response.json()
        if data.get('success'):
            return data.get('data')
        else:
            logger.error(f"Market Data Service error fetching option chain for {symbol}: {data.get('error')}")
            return None
    except httpx.HTTPStatusError as e:
        try:
            error_data = e.response.json()
            logger.error(f"HTTP status error fetching option chain for {symbol}: {e.response.status_code} - {error_data.get('detail') or error_data.get('error')}")
        except Exception:
            logger.error(f"HTTP status error fetching option chain for {symbol}: {e.response.status_code} - {e.response.text}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Network error fetching option chain for {symbol}: {e}. Is Market Data Microservice running?")
        return None
    except Exception as e:
        logger.error(f"❌ Error processing option chain response for {symbol}: {e}", exc_info=True)
        return None

async def get_spot_details_from_service(symbol: str) -> Optional[Dict]:
    """Fetches spot details from the dedicated market data service."""
    try:
        response = await _http_client.get(f"/api/spot-details/{symbol.upper()}")
        response.raise_for_status()
        data = response.json()
        if data.get('success'):
            return data.get('data')
        else:
            logger.error(f"Market Data Service error fetching spot details for {symbol}: {data.get('error')}")
            return None
    except httpx.HTTPStatusError as e:
        try:
            error_data = e.response.json()
            logger.error(f"HTTP status error fetching spot details for {symbol}: {e.response.status_code} - {error_data.get('detail') or error_data.get('error')}")
        except Exception:
            logger.error(f"HTTP status error fetching spot details for {symbol}: {e.response.status_code} - {e.response.text}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Network error fetching spot details for {symbol}: {e}. Is Market Data Microservice running?")
        return None
    except Exception as e:
        logger.error(f"❌ Error processing spot details response for {symbol}: {e}", exc_info=True)
        return None

async def get_ltp_from_service(token: int, segment: int = config.EXCHANGE_NSEFO) -> float:
    """Fetches LTP for a given token from the Market Data Microservice."""
    try:
        response = await _http_client.get(f"/api/ltp/{segment}/{token}")
        response.raise_for_status()
        data = response.json()
        if data.get('success'):
            return float(data.get('ltp', 0.0))
        else:
            logger.warning(f"Market Data Service error fetching LTP for {token}: {data.get('error') or data.get('detail')}")
            return 0.0
    except httpx.RequestError:
        # Don't spam logs for network errors on single LTP fetches, as they can be frequent during startup/shutdown
        return 0.0
    except Exception as e:
        logger.error(f"Unexpected error fetching LTP for {token}: {e}", exc_info=True)
        return 0.0

async def get_market_depth_from_service(token: int) -> Optional[Dict]:
    """Fetches market depth for a given token from the Market Data Microservice."""
    try:
        # NOTE: This endpoint does not exist in the provided marketdata_service.py
        response = await _http_client.get(f"/api/market-depth/{token}")
        response.raise_for_status()
        data = response.json()
        if data.get('success'):
            return data.get('data')
        else:
            logger.warning(f"Market Data Service error fetching market depth for {token}: {data.get('error') or data.get('detail')}")
            return None
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching market depth for {token}: {e}. Is Market Data Microservice running?")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching market depth for {token}: {e}", exc_info=True)
        return None

async def get_bulk_market_depth_from_service(instruments: List[Dict]) -> Dict[int, Dict]:
    """Fetches market depth for multiple tokens from the Market Data Microservice."""
    try:
        # Pass the full instrument list, which includes exchange segments
        response = await _http_client.post("/api/bulk-market-depth", json={"instruments": instruments})
        response.raise_for_status()
        data = response.json()
        if data.get('success'):
            return {int(k): v for k, v in data.get('data', {}).items()}
        else:
            logger.warning(f"Market Data Service error fetching bulk market depth: {data.get('error') or data.get('detail')}")
            return {}
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching bulk market depth: {e}. Is Market Data Microservice running?")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error fetching bulk market depth: {e}", exc_info=True)
        return {}

async def get_bulk_ltp_from_service(tokens: List[int]) -> Dict[int, float]:
    """Fetches LTP for multiple tokens from the Market Data Microservice."""
    try:
        response = await _http_client.post("/api/bulk-ltp", json={"tokens": tokens})
        response.raise_for_status()
        data = response.json()
        if data.get('success'):
            # The service returns string keys, convert them to int
            return {int(k): float(v) for k, v in data.get('data', {}).items()}
        else:
            logger.warning(f"Market Data Service error fetching bulk LTP: {data.get('error') or data.get('detail')}")
            return {}
    except httpx.RequestError:
        # Don't spam logs for network errors on bulk LTP fetches
        return {}
    except Exception as e:
        logger.error(f"Unexpected error fetching bulk LTP: {e}", exc_info=True)
        return {}

async def market_data_service_listener():
    """
    Connects to the Market Data Microservice via WebSocket and listens for real-time updates.
    This single function replaces the previous polling loops for prices and option chains.
    """
    uri = f"ws://{_client_connect_host}:{config.MARKET_DATA_PORT}/ws/data"
    
    # Define extra headers to include the Origin. This is often required by server-side
    # CORS middleware, even for backend-to-backend WebSocket connections.
    # The websockets library does not send this by default, unlike a browser.
    extra_headers = {"Origin": f"http://{_client_connect_host}"}

    logger.info(f"🚀 Connecting to Market Data Service stream at {uri}...")
    logger.info(f"Handshake headers being sent: {extra_headers}")

    while True:
        try:
            async with websockets.connect(uri, extra_headers=extra_headers) as websocket:
                logger.info("✅ Connected to Market Data Service real-time stream.")
                while True:
                    message_str = await websocket.recv()
                    message = json.loads(message_str)
                    msg_type = message.get('type')

                    if msg_type == 'price_update':
                        prices = message.get('data', {})
                        if prices:
                            # Use the unified update_price method on state which handles both shared memory and local dict
                            for token, ltp in prices.items():
                                state.update_price(int(token), float(ltp))
                            
                            logger.debug(f"Received {len(prices)} price updates via WebSocket.")
                            # Re-broadcast the whole batch to frontend clients
                            from background.tasks import broadcast_message
                            # This is a fire-and-forget task to avoid blocking the listener
                            asyncio.create_task(broadcast_message({'type': 'price_update', 'data': prices}))
                    elif msg_type == 'option_chain_update':
                        symbol = message.get('symbol')
                        chain_data = message.get('data')
                        if symbol and chain_data:
                            # Ensure option_chains dict exists for local caching in worker processes
                            if state.option_chains is None:
                                state.option_chains = {}

                            state.update_option_chain(symbol, chain_data)
                            logger.info(f"Received and updated option chain for {symbol} via WebSocket.")
                            # Re-broadcast the chain update to frontend clients
                            from background.tasks import broadcast_message
                            asyncio.create_task(broadcast_message({
                                'type': 'option_chain_update',
                                'data': chain_data
                            }))
        except (websockets.exceptions.ConnectionClosedError, ConnectionRefusedError, OSError) as e:
            logger.error(f"🔌 Market Data Service stream disconnected: {type(e).__name__}. Reconnecting in 5s...")
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            logger.info("Price sync loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in price sync loop: {e}", exc_info=True)
            await asyncio.sleep(5)

async def initialize_market_data_client():
    """Initializes the HTTP client for the market data service."""
    global _http_client
    if _http_client.is_closed:
        _http_client = httpx.AsyncClient(
            base_url=f"http://{_client_connect_host}:{config.MARKET_DATA_PORT}",
            timeout=10.0
        )
    
    # Initialize local state caches if they are missing (e.g. in worker processes)
    if state.prices is None:
        state.prices = {}
    if state.option_chains is None:
        state.option_chains = {}
    logger.info("✅ Market Data Client initialized.")

async def close_market_data_client():
    """Closes the HTTP client."""
    global _http_client
    if not _http_client.is_closed:
        await _http_client.aclose()
        logger.info("✅ Market Data Client closed.")

async def sync_prices_from_service_loop():
    """Alias for market_data_service_listener."""
    await market_data_service_listener()

async def subscribe_active_straddles():
    """
    Subscribes to active straddle instruments by sending them to the Market Data Microservice.
    The microservice is responsible for maintaining the actual socket subscriptions.
    """
    if not state.db:
        logger.error("❌ Database not initialized for active straddle subscription.")
        return

    try:
        straddles = state.db.get_active_straddles()
        if not straddles:
            logger.info("ℹ️ No active straddles to subscribe.")
            return

        # Group tokens by symbol
        subscriptions_map = {} # symbol -> set of tokens
        
        for straddle in straddles:
            symbol = straddle.get('symbol')
            if not symbol: continue
            
            if symbol not in subscriptions_map:
                subscriptions_map[symbol] = set()
            
            if straddle.get('ce_token'): subscriptions_map[symbol].add(int(straddle['ce_token']))
            if straddle.get('pe_token'): subscriptions_map[symbol].add(int(straddle['pe_token']))
            if straddle.get('fut_token'): subscriptions_map[symbol].add(int(straddle['fut_token']))

        if not subscriptions_map:
            logger.info("No instruments found in active straddles to subscribe.")
            return

        # Construct payload
        subscriptions_payload = []
        for symbol, tokens in subscriptions_map.items():
            if tokens:
                subscriptions_payload.append({
                    "symbol": symbol,
                    "tokens": list(tokens)
                })

        if not subscriptions_payload:
            return

        response = await _http_client.post(
            "/api/subscribe",
            json={"subscriptions": subscriptions_payload},
            timeout=10.0
        )
        response.raise_for_status()
        data = response.json()

        if data.get('success'):
            total_tokens = sum(len(item['tokens']) for item in subscriptions_payload)
            for item in subscriptions_payload:
                for token in item['tokens']:
                    state.add_subscription(token)
            logger.info(f"✅ Successfully requested subscription for {total_tokens} instruments from Market Data Service.")
        else:
            logger.error(f"❌ Market Data Service failed to subscribe instruments: {data.get('error') or data.get('detail')}")

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error during active straddle subscription: {e.response.status_code} - {e.response.text}")
    except httpx.RequestError as e:
        logger.error(f"Network error during active straddle subscription: {e}. Is Market Data Microservice running?")
    except Exception as e:
        logger.error(f"Unexpected error during active straddle subscription: {e}", exc_info=True)
