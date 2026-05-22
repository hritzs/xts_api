import asyncio
import httpx
from typing import Dict, Optional, List

from utils.logger import logger
from models.state import state
import config


# ── Service URLs ──────────────────────────────────────────────────────────────
MARKET_DATA_SERVICE_URL = f"http://localhost:{config.MARKET_DATA_PORT}"
SNAPSHOT_SERVICE_URL    = f"http://localhost:{getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}"


# ── Option chain ──────────────────────────────────────────────────────────────

async def get_option_chain_from_service(symbol: str) -> Optional[Dict]:
    """Fetches option chain data for a given symbol from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/option-chain/{symbol.upper()}")
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return data.get('data')
            logger.error(f"Market Data Service error for {symbol}: {data.get('error')}")
            return None
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching option chain for {symbol}: {e}")
    except httpx.RequestError as e:
        logger.error(f"Network error fetching option chain for {symbol}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching option chain for {symbol}: {e}", exc_info=True)
    return None


# ── Spot details ──────────────────────────────────────────────────────────────

async def get_spot_details_from_service(symbol: str) -> Optional[Dict]:
    """Fetches spot details for a given symbol from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/spot-details/{symbol.upper()}")
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return data.get('data')
            logger.error(f"Market Data Service error for {symbol}: {data.get('error')}")
            return None
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error fetching spot details for {symbol}: {e}")
    except httpx.RequestError as e:
        logger.error(f"Network error fetching spot details for {symbol}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching spot details for {symbol}: {e}", exc_info=True)
    return None


# ── Single LTP ────────────────────────────────────────────────────────────────

async def get_ltp_from_service(token: int) -> float:
    """Fetches LTP for a given token from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/ltp/{token}")
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return float(data.get('ltp', 0.0))
            logger.warning(f"LTP error for token {token}: {data.get('error')}")
            return 0.0
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching LTP for {token}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching LTP for {token}: {e}", exc_info=True)
    return 0.0


# ── Bulk LTP ──────────────────────────────────────────────────────────────────

async def get_bulk_ltp_from_service(tokens: List[int]) -> Dict[int, float]:
    """Fetches LTP for multiple tokens from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{MARKET_DATA_SERVICE_URL}/bulk-ltp",
                json={"tokens": tokens}
            )
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return {int(k): float(v) for k, v in data.get('data', {}).items()}
            logger.warning(f"Bulk LTP error: {data.get('error')}")
            return {}
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching bulk LTP: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching bulk LTP: {e}", exc_info=True)
    return {}


# ── Market depth ──────────────────────────────────────────────────────────────

async def get_market_depth_from_service(token: int) -> Optional[Dict]:
    """Fetches market depth for a given token from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{MARKET_DATA_SERVICE_URL}/market-depth/{token}")
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return data.get('data')
            logger.warning(f"Market depth error for token {token}: {data.get('error')}")
            return None
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching market depth for {token}: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching market depth for {token}: {e}", exc_info=True)
    return None


async def get_bulk_market_depth_from_service(instruments: List[Dict]) -> Dict[int, Dict]:
    """Fetches market depth for multiple tokens from the Market Data Microservice."""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{MARKET_DATA_SERVICE_URL}/bulk-market-depth",
                json={"instruments": instruments}
            )
            response.raise_for_status()
            data = response.json()
            if data.get('success'):
                return {int(k): v for k, v in data.get('data', {}).items()}
            logger.warning(f"Bulk market depth error: {data.get('error')}")
            return {}
    except httpx.RequestError as e:
        logger.warning(f"Network error fetching bulk market depth: {e}")
    except Exception as e:
        logger.error(f"Unexpected error fetching bulk market depth: {e}", exc_info=True)
    return {}


# ── Snapshot service ──────────────────────────────────────────────────────────

async def get_snapshot_from_service(trade_uid: str) -> Optional[Dict]:
    """Fetches the latest computed snapshot for a trade from snapshot_service."""
    try:
        async with httpx.AsyncClient(timeout=1.0) as client:
            resp = await client.get(f"{SNAPSHOT_SERVICE_URL}/api/snapshots/{trade_uid}")
            if resp.status_code == 200:
                return resp.json()
            logger.debug(f"Snapshot not found for {trade_uid} (status {resp.status_code})")
    except Exception as e:
        logger.debug(f"Could not fetch snapshot for {trade_uid}: {e}")
    return None


# ── Subscriptions ─────────────────────────────────────────────────────────────

async def subscribe_active_straddles():
    """Subscribes active straddle instruments by sending tokens to the Market Data Microservice."""
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
            for key in ('ce_token', 'pe_token', 'fut_token'):
                if straddle.get(key):
                    tokens_to_subscribe.add(int(straddle[key]))

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
                logger.info(f"✅ Subscribed {len(tokens_to_subscribe)} instruments to Market Data Service.")
            else:
                logger.error(f"❌ Subscription failed: {data.get('error')}")

    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error during straddle subscription: {e}")
    except httpx.RequestError as e:
        logger.error(f"Network error during straddle subscription: {e}")
    except Exception as e:
        logger.error(f"Unexpected error during straddle subscription: {e}", exc_info=True)


# ── Price sync loop ───────────────────────────────────────────────────────────

async def sync_prices_loop():
    """Continuously fetches prices from the Market Data Microservice and updates local state."""
    logger.info("🔄 Starting price sync loop with Market Data Microservice.")
    while True:
        try:
            subscribed_tokens = list(state.subscribed_tokens)
            if not subscribed_tokens:
                logger.debug("No tokens subscribed for price sync. Waiting...")
                await asyncio.sleep(5)
                continue

            prices_data = await get_bulk_ltp_from_service(subscribed_tokens)
            if prices_data:
                for token, ltp in prices_data.items():
                    state.update_price(token, float(ltp))
                logger.debug(f"Synced {len(prices_data)} prices from Market Data Service.")

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("Price sync loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in price sync loop: {e}", exc_info=True)
            await asyncio.sleep(5)


# ── HTTP client lifecycle ─────────────────────────────────────────────────────

_http_client: Optional[httpx.AsyncClient] = None

def set_http_client_instance(host: str, port: int):
    """
    Initializes the shared HTTP client pointed at the market data service.
    Called once at startup by main.py after services are ready.
    Also dynamically updates MARKET_DATA_SERVICE_URL to use the actual host.
    """
    global _http_client, MARKET_DATA_SERVICE_URL
    MARKET_DATA_SERVICE_URL = f"http://{host}:{port}"
    _http_client = httpx.AsyncClient(
        base_url=MARKET_DATA_SERVICE_URL,
        timeout=2.0
    )
    logger.info(f"✅ Trading data client initialized → {MARKET_DATA_SERVICE_URL}")
