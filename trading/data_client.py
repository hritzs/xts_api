import httpx
import asyncio
from typing import Dict, Optional, List
from utils.logger import logger
from models.state import state
import config
import functools # Added for potential future use, not strictly needed for this fix.

# URL for the Market Data Microservice
# Initialize as None, will be set during app startup
_http_client: Optional[httpx.AsyncClient] = None
MARKET_DATA_SERVICE_URL: Optional[str] = None

def set_http_client_instance(host: str, port: int):
    global _http_client, MARKET_DATA_SERVICE_URL
    MARKET_DATA_SERVICE_URL = f"http://{host}:{port}"
    _http_client = httpx.AsyncClient(base_url=MARKET_DATA_SERVICE_URL, timeout=10.0)
    logger.info(f"✅ HTTP client for Market Data Service initialized: {MARKET_DATA_SERVICE_URL}")

async def _make_request(method: str, endpoint: str, **kwargs) -> Optional[Dict]:
    """
    Helper to make HTTP requests to the Market Data Microservice.
    """
    try:
        if _http_client is None or MARKET_DATA_SERVICE_URL is None:
            logger.error("❌ HTTP client for Market Data Service not initialized.")
            return None # --- FIX: Use the client's base_url, do not prepend the full URL again ---
        response = await _http_client.request(method, endpoint, **kwargs)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as e:
        logger.error(f"❌ Network error during request to {endpoint}: {e}. Is Market Data Microservice running at {MARKET_DATA_SERVICE_URL}?")
        return None
    except httpx.HTTPStatusError as e:
        logger.error(f"❌ HTTP status error during request to {endpoint}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error during request to {endpoint}: {e}", exc_info=True)
        return None
async def get_option_chain_from_service(symbol: str) -> Optional[Dict]:
    data = await _make_request("GET", f"/api/option-chain/{symbol.upper()}")
    if data and data.get('success'):
        return data.get('data')
    return None

async def get_spot_details_from_service(symbol: str) -> Optional[Dict]:
    data = await _make_request("GET", f"/api/spot-details/{symbol.upper()}")
    if data and data.get('success'):
        return data.get('data')
    return None

async def get_ltp_from_service(token: int, segment: int) -> float:
    data = await _make_request("GET", f"/api/ltp/{segment}/{token}")
    if data and data.get('success'):
        return float(data.get('ltp', 0.0))
    return 0.0

async def get_market_depth_from_service(token: int) -> Optional[Dict]:
    data = await _make_request("GET", f"/api/market-depth/{token}")
    if data and data.get('success'):
        return data.get('data')
    return None

async def get_bulk_market_depth_from_service(instruments: List[Dict]) -> Dict[int, Dict]:
    data = await _make_request("POST", "/api/bulk-market-depth", json={"instruments": instruments})
    if data and data.get('success'):
        return {int(k): v for k, v in data.get('data', {}).items()}
    return {}

async def subscribe_instruments_to_service(subscriptions: List[Dict]) -> bool:
    data = await _make_request("POST", "/api/subscribe", json={"subscriptions": subscriptions})
    return data and data.get('success', False)

async def sync_prices_loop():
    """
    Continuously fetches prices from the Market Data Microservice and updates local state.
    """
    logger.info("🔄 Starting price sync loop with Market Data Microservice.")
    if _http_client is None or MARKET_DATA_SERVICE_URL is None:
        logger.error("❌ HTTP client for Market Data Service not initialized. Cannot start price sync loop.")
        return

    while True:
        try:
            subscribed_tokens = list(state.subscribed_tokens)
            if not subscribed_tokens:
                logger.debug("No tokens subscribed for price sync. Waiting...")
                await asyncio.sleep(5)
                continue

            # Use the global _http_client
            prices_data = await _make_request("POST", "/api/bulk-ltp", json={"tokens": subscribed_tokens})
            if prices_data and prices_data.get('success'):
                for token, ltp in prices_data.get('data', {}).items():
                    state.update_price(int(token), float(ltp))
                logger.debug(f"Synced {len(prices_data.get('data', {}))} prices from Market Data Service.")

            await asyncio.sleep(config.PRICE_SYNC_INTERVAL) # e.g., 1 second
        except asyncio.CancelledError:
            logger.info("Price sync loop cancelled.")
            break
        except Exception as e:
            logger.error(f"❌ Unexpected error in price sync loop: {e}", exc_info=True)
            await asyncio.sleep(5)

async def subscribe_active_straddles():
    """
    Subscribes to active straddle instruments by sending them to the Market Data Microservice.
    The microservice is responsible for maintaining the actual socket subscriptions.
    """
    if not state.db:
        logger.error("❌ Database not initialized for active straddle subscription.")
        return
    if _http_client is None or MARKET_DATA_SERVICE_URL is None:
        logger.error("❌ HTTP client for Market Data Service not initialized. Cannot subscribe active straddles.")
        return

    try:
        straddles = state.db.get_active_straddles()
        if not straddles:
            logger.info("ℹ️ No active straddles to subscribe.")
            return

        # NEW: Group tokens by symbol to ensure correct segment is used for subscription
        subscriptions_by_symbol: Dict[str, set] = {}
        for straddle in straddles:
            symbol = straddle.get('symbol')
            if not symbol:
                continue
            
            if symbol not in subscriptions_by_symbol:
                subscriptions_by_symbol[symbol] = set()
                
            if straddle.get('ce_token'):
                subscriptions_by_symbol[symbol].add(int(straddle['ce_token']))
            if straddle.get('pe_token'):
                subscriptions_by_symbol[symbol].add(int(straddle['pe_token']))
            if straddle.get('fut_token'):
                subscriptions_by_symbol[symbol].add(int(straddle['fut_token']))

        if not subscriptions_by_symbol:
            logger.info("No instruments found in active straddles to subscribe.")
            return

        # Convert to the list format the new API will expect
        subscriptions_payload = [
            {"symbol": symbol, "tokens": list(tokens)}
            for symbol, tokens in subscriptions_by_symbol.items()
        ]
        success = await subscribe_instruments_to_service(subscriptions_payload)

        if success:
            all_subscribed_tokens = {token for sub in subscriptions_payload for token in sub['tokens']}
            for token in all_subscribed_tokens:
                state.add_subscription(token)
            logger.info(f"✅ Successfully requested subscription for {len(all_subscribed_tokens)} instruments from Market Data Service.")
        else:
            logger.error(f"❌ Market Data Service failed to subscribe instruments.")

    except Exception as e:
        logger.error(f"Unexpected error during active straddle subscription: {e}", exc_info=True)

# Assuming the microservice has a bulk LTP endpoint for efficiency
async def get_bulk_ltp_from_service(tokens: List[int]) -> Dict[int, float]:
    """Fetches LTP for multiple tokens from the Market Data Microservice."""
    data = await _make_request("POST", "/api/bulk-ltp", json={"tokens": tokens})
    if data and data.get('success'):
        # The service returns keys as strings, convert them back to int
        return {int(k): float(v) for k, v in data.get('data', {}).items()}
    logger.warning(f"Market Data Service error fetching bulk LTP: {data.get('error') if data else 'No response'}")
    return {}