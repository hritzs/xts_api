import httpx
import asyncio
from typing import Dict, Optional, List
from utils.logger import logger
from models.state import state
import config

# URL for the Market Data Microservice
MARKET_DATA_SERVICE_URL = f"http://localhost:{config.MARKET_DATA_PORT}"

async def get_option_chain_from_service(symbol: str) -> Optional[Dict]:
    """
    Fetches option chain data for a given symbol from the Market Data Microservice.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/option-chain/{symbol.upper()}")
            response.raise_for_status() # Raise an exception for 4xx/5xx responses
            data = response.json()
            if data.get('success'):
                return data.get('data')
            else:
                logger.error(f"Market Data Service error fetching option chain for {symbol}: {data.get('error')}")
                return None
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP status error fetching option chain for {symbol}: {e}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Network error fetching option chain for {symbol}: {e}. Is Market Data Microservice running at {MARKET_DATA_SERVICE_URL}?")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching option chain for {symbol}: {e}", exc_info=True)
        return None

async def get_spot_details_from_service(symbol: str) -> Optional[Dict]:
    """
    Fetches spot details for a given symbol from the Market Data Microservice.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/spot-details/{symbol.upper()}")
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return data.get('data')
            else:
                logger.error(f"Market Data Service error fetching spot details for {symbol}: {data.get('error')}")
                return None
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP status error fetching spot details for {symbol}: {e}")
        return None
    except httpx.RequestError as e:
        logger.error(f"Network error fetching spot details for {symbol}: {e}. Is Market Data Microservice running at {MARKET_DATA_SERVICE_URL}?")
        return None
    except Exception as e:
        logger.error(f"Unexpected error fetching spot details for {symbol}: {e}", exc_info=True)
        return None

async def get_ltp_from_service(token: int) -> float:
    """Fetches LTP for a given token from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/ltp/{token}")
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return float(data.get('ltp', 0.0))
            else:
                logger.warning(f"Market Data Service error fetching LTP for {token}: {data.get('error')}")
                return 0.0
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching LTP for {token}: {e}. Is Market Data Microservice running?")
        return 0.0
    except Exception as e:
        logger.error(f"Unexpected error fetching LTP for {token}: {e}", exc_info=True)
        return 0.0

async def get_market_depth_from_service(token: int) -> Optional[Dict]:
    """Fetches market depth for a given token from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/market-depth/{token}")
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return data.get('data')
            else:
                logger.warning(f"Market Data Service error fetching market depth for {token}: {data.get('error')}")
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
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{MARKET_DATA_SERVICE_URL}/bulk-market-depth", json={"instruments": instruments})
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return {int(k): v for k, v in data.get('data', {}).items()}
            else:
                logger.warning(f"Market Data Service error fetching bulk market depth: {data.get('error')}")
                return {}
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching bulk market depth: {e}. Is Market Data Microservice running?")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error fetching bulk market depth: {e}", exc_info=True)
        return {}

async def sync_prices_loop():
    """
    Continuously fetches prices from the Market Data Microservice and updates local state.
    """
    logger.info("🔄 Starting price sync loop with Market Data Microservice.")
    while True:
        try:
            subscribed_tokens = list(state.subscribed_tokens)
            if not subscribed_tokens:
                logger.debug("No tokens subscribed for price sync. Waiting...")
                await asyncio.sleep(5)
                continue

            prices_data = await get_bulk_ltp_from_service(subscribed_tokens) # Assuming a bulk LTP endpoint in microservice
            if prices_data:
                for token, ltp in prices_data.items():
                    state.update_price(token, float(ltp))
                logger.debug(f"Synced {len(prices_data)} prices from Market Data Service.")

            await asyncio.sleep(1) # Poll every 1 second
        except asyncio.CancelledError:
            logger.info("Price sync loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in price sync loop: {e}", exc_info=True)
            await asyncio.sleep(5)

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

        tokens_to_subscribe = set()
        for straddle in straddles:
            if straddle.get('ce_token'):
                tokens_to_subscribe.add(int(straddle['ce_token']))
            if straddle.get('pe_token'):
                tokens_to_subscribe.add(int(straddle['pe_token']))
            if straddle.get('fut_token'):
                tokens_to_subscribe.add(int(straddle['fut_token']))

        if not tokens_to_subscribe:
            logger.info("No instruments found in active straddles to subscribe.")
            return

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{MARKET_DATA_SERVICE_URL}/subscribe",
                json={"tokens": list(tokens_to_subscribe)},
                timeout=5.0
            )
            response.raise_for_status()
            data = response.json()

            if data.get('success'):
                for token in tokens_to_subscribe:
                    state.add_subscription(token)
                logger.info(f"✅ Successfully requested subscription for {len(tokens_to_subscribe)} instruments from Market Data Service.")
            else:
                logger.error(f"❌ Market Data Service failed to subscribe instruments: {data.get('error')}")

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error during active straddle subscription: {e}")
    except httpx.RequestError as e:
        logger.error(f"Network error during active straddle subscription: {e}. Is Market Data Microservice running at {MARKET_DATA_SERVICE_URL}?")
    except Exception as e:
        logger.error(f"Unexpected error during active straddle subscription: {e}", exc_info=True)

# Assuming the microservice has a bulk LTP endpoint for efficiency
async def get_bulk_ltp_from_service(tokens: List[int]) -> Dict[int, float]:
    """Fetches LTP for multiple tokens from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(f"{MARKET_DATA_SERVICE_URL}/bulk-ltp", json={"tokens": tokens})
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return {int(k): float(v) for k, v in data.get('data', {}).items()}
            else:
                logger.warning(f"Market Data Service error fetching bulk LTP: {data.get('error')}")
                return {}
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching bulk LTP: {e}. Is Market Data Microservice running?")
        return {}
    except Exception as e:
        logger.error(f"Unexpected error fetching bulk LTP: {e}", exc_info=True)
        return {}