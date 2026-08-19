import asyncio
import httpx
from typing import Dict, Optional, List

from utils.logger import logger
from models.state import state
import config

from market_data.data_client import (
    get_option_chain_from_service as zmq_get_option_chain_from_service,
    get_spot_details_from_service as zmq_get_spot_details_from_service,
    get_ltp_from_service as zmq_get_ltp_from_service,
    get_bulk_ltp_from_service as zmq_get_bulk_ltp_from_service,
    get_bulk_market_depth_from_service as zmq_get_bulk_market_depth_from_service,
    subscribe_active_straddles as zmq_subscribe_active_straddles,
)

# ── Service URLs ──────────────────────────────────────────────────────────────
SNAPSHOT_SERVICE_URL = f"http://127.0.0.1:{getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}"

# Kept only for compatibility/logging; market data itself is ZMQ-based now.
MARKET_DATA_SERVICE_URL = f"zmq://127.0.0.1:{getattr(config, 'ZMQ_MARKETDATA_REQ_PORT', 5560)}"


# ── Option chain ──────────────────────────────────────────────────────────────

async def get_option_chain_from_service(symbol: str) -> Optional[Dict]:
    """Fetch option chain data for a given symbol from the Market Data Microservice via ZMQ."""
    try:
        return await zmq_get_option_chain_from_service(symbol)
    except Exception as e:
        logger.error(f"Unexpected error fetching option chain for {symbol}: {e}", exc_info=True)
        return None


# ── Spot details ──────────────────────────────────────────────────────────────

async def get_spot_details_from_service(symbol: str) -> Optional[Dict]:
    """Fetch spot details for a given symbol from the Market Data Microservice via ZMQ."""
    try:
        return await zmq_get_spot_details_from_service(symbol)
    except Exception as e:
        logger.error(f"Unexpected error fetching spot details for {symbol}: {e}", exc_info=True)
        return None


# ── Single LTP ────────────────────────────────────────────────────────────────

async def get_ltp_from_service(token: int) -> float:
    """Fetch LTP for a given token from the Market Data Microservice via ZMQ."""
    try:
        return await zmq_get_ltp_from_service(int(token))
    except Exception as e:
        logger.error(f"Unexpected error fetching LTP for {token}: {e}", exc_info=True)
        return 0.0


# ── Bulk LTP ──────────────────────────────────────────────────────────────────

async def get_bulk_ltp_from_service(tokens: List[int]) -> Dict[int, float]:
    """Fetch LTP for multiple tokens from the Market Data Microservice via ZMQ."""
    try:
        if not tokens:
            return {}
        return await zmq_get_bulk_ltp_from_service([int(t) for t in tokens])
    except Exception as e:
        logger.error(f"Unexpected error fetching bulk LTP: {e}", exc_info=True)
        return {}


# ── Market depth ──────────────────────────────────────────────────────────────

async def get_market_depth_from_service(token: int) -> Optional[Dict]:
    """Fetch market depth for a given token via the bulk ZMQ endpoint for compatibility."""
    try:
        token_int = int(token)
        instruments = [{
            "exchangeInstrumentID": token_int,
            "exchangeSegment": config.EXCHANGE_NSEFO
        }]
        depth_map = await zmq_get_bulk_market_depth_from_service(instruments)
        return depth_map.get(token_int)
    except Exception as e:
        logger.error(f"Unexpected error fetching market depth for {token}: {e}", exc_info=True)
        return None


async def get_bulk_market_depth_from_service(instruments: List[Dict]) -> Dict[int, Dict]:
    """Fetch market depth for multiple tokens from the Market Data Microservice via ZMQ."""
    if not instruments:
        logger.warning("Bulk market depth request skipped: no instruments provided.")
        return {}

    try:
        logger.info(
            f"📤 [trading.data_client] bulk-depth request count={len(instruments)} "
            f"sample={instruments[:3]}"
        )

        normalized_instruments = []
        for item in instruments:
            if not isinstance(item, dict):
                continue

            token = (
                item.get("exchangeInstrumentID")
                or item.get("ExchangeInstrumentID")
                or item.get("token")
            )
            segment = (
                item.get("exchangeSegment")
                or item.get("ExchangeSegment")
                or item.get("segment")
                or config.EXCHANGE_NSEFO
            )

            if token is None:
                continue

            normalized_instruments.append({
                "exchangeInstrumentID": int(token),
                "exchangeSegment": int(segment),
            })

        if not normalized_instruments:
            logger.warning("Bulk market depth request normalized to 0 valid instruments.")
            return {}

        depth_map = await zmq_get_bulk_market_depth_from_service(normalized_instruments)

        if depth_map:
            first_key = next(iter(depth_map), None)
            if first_key is not None:
                logger.debug("first depth item token (hidden)")
        else:
            logger.warning(
                f"Bulk market depth returned 0 items for {len(normalized_instruments)} instruments."
            )

        return depth_map

    except Exception as e:
        logger.error(f"Unexpected error fetching bulk market depth: {e}", exc_info=True)
        return {}


# ── Snapshot service ──────────────────────────────────────────────────────────

async def get_snapshot_from_service(trade_uid: str) -> Optional[Dict]:
    """Fetch the latest computed snapshot for a trade from snapshot_service over HTTP."""
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
    """Subscribe active straddle instruments via ZMQ."""
    try:
        await zmq_subscribe_active_straddles()
    except Exception as e:
        logger.error(f"Unexpected error during active straddle subscription: {e}", exc_info=True)


# ── Price sync loop ───────────────────────────────────────────────────────────

async def sync_prices_loop():
    """Continuously fetch prices from the Market Data Microservice and update local state."""
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
                    state.update_price(int(token), float(ltp))
                logger.debug(f"Synced {len(prices_data)} prices from Market Data Service.")

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("Price sync loop cancelled.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in price sync loop: {e}", exc_info=True)
            await asyncio.sleep(5)


# ── Client lifecycle ──────────────────────────────────────────────────────────

_http_client: Optional[httpx.AsyncClient] = None


def set_http_client_instance(host: str, port: int):
    """
    Compatibility stub.

    The market data service is ZMQ-based, not HTTP-based. We keep this function so
    existing startup code does not break, but it only updates a diagnostic string.
    """
    global MARKET_DATA_SERVICE_URL, _http_client
    MARKET_DATA_SERVICE_URL = f"zmq://{host}:{getattr(config, 'ZMQ_MARKETDATA_REQ_PORT', 5560)}"

    if _http_client is not None:
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_http_client.aclose())
        except Exception:
            pass
        _http_client = None

    logger.info(f"✅ Trading data client initialized → {MARKET_DATA_SERVICE_URL}")
