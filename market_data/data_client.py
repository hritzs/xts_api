# c:\Users\Administrator\Desktop\api_v2_microservices\market_data\data_client.py
import asyncio
import json
import zmq
import zmq.asyncio
from typing import Dict, Optional, List
from utils.logger import logger
from models.state import state
import config

_zmq_ctx: Optional[zmq.asyncio.Context] = None

def get_zmq_context() -> zmq.asyncio.Context:
    """Get a singleton ZMQ context."""
    global _zmq_ctx
    if _zmq_ctx is None or _zmq_ctx.closed:
        _zmq_ctx = zmq.asyncio.Context()
    return _zmq_ctx

async def send_zmq_request(command: str, payload: dict, timeout: int = 5000) -> dict:
    """Helper to send a REQ to the marketdata service and get a REP."""
    ctx = get_zmq_context()
    socket = ctx.socket(zmq.REQ)
    socket.connect(f"tcp://localhost:{config.ZMQ_MARKETDATA_REQ_PORT}")
    try:
        request = {"command": command, "payload": payload}
        await socket.send_json(request)

        poller = zmq.asyncio.Poller()
        poller.register(socket, zmq.POLLIN)
        if await poller.poll(timeout):
            response = await socket.recv_json()
            return response
        else:
            logger.error(f"ZMQ request '{command}' timed out after {timeout}ms.")
            return {"success": False, "error": "Request timed out"}
    except Exception as e:
        logger.error(f"ZMQ request '{command}' failed: {e}", exc_info=True)
        return {"success": False, "error": str(e)}
    finally:
        socket.close()

async def get_option_chain_from_service(symbol: str) -> Optional[Dict]:
    """Fetches the option chain from the dedicated market data service."""

    response = await send_zmq_request(
        "get_option_chain",
        {"symbol": symbol.upper()}
    )

    logger.debug("=" * 120)
    logger.debug("[RAW ZMQ RESPONSE]")

    try:
        data = response.get("data", {}) if response else {}

        logger.info(
            "[OPTION CHAIN RECEIVED] "
            f"symbol={data.get('symbol')} "
            f"expiry={data.get('expiry')} "
            f"strikes={len(data.get('chain', []))}"
        )

        chain = data.get("chain", [])
        if chain:
            logger.info(
                "[OPTION CHAIN ATM] "
                f"Strike={chain[0].get('strike')} "
                f"CE={chain[0].get('ce_ltp')} "
                f"PE={chain[0].get('pe_ltp')}"
            )

    except Exception as e:
        logger.debug(f"Unable to summarize response: {e}")

    logger.debug("=" * 120)

    if response and response.get("success"):

        chain = response.get("data")

        logger.debug("=" * 120)
        logger.info("[OPTION CHAIN RECEIVED]")

        logger.debug(f"Top Keys : {list(chain.keys())}")

        rows = chain.get("chain", [])

        logger.debug(f"Rows : {len(rows)}")

        if rows:

            logger.debug("=" * 80)
            logger.info("FIRST ROW")

            for k, v in rows[0].items():
                logger.debug(f"{k:<35} = {v}")

            logger.debug("=" * 80)

        return chain

    error = response.get("error", "Unknown error")
    logger.error(f"ZMQ error fetching option chain for {symbol}: {error}")

    return None

async def get_spot_details_from_service(symbol: str) -> Optional[Dict]:
    """Fetches spot details from the dedicated market data service."""
    response = await send_zmq_request("get_spot_details", {"symbol": symbol.upper()})
    if response and response.get('success'):
        return response.get('data')
    else:
        error = response.get('error', 'Unknown error')
        logger.error(f"ZMQ error fetching spot details for {symbol}: {error}")
        return None

async def get_ltp_from_service(token: int, segment: int = config.EXCHANGE_NSEFO) -> float:
    """Fetches LTP for a given token from the Market Data Microservice."""
    # Use bulk endpoint for efficiency, even for a single token
    response = await send_zmq_request("get_bulk_ltp", {"tokens": [token]})
    if response and response.get('success'):
        prices = response.get('data', {})
        # ZMQ response keys are strings
        return float(prices.get(str(token), 0.0))
    else:
        error = response.get('error', 'Unknown error')
        logger.warning(f"ZMQ error fetching LTP for {token}: {error}")
        return 0.0

async def get_bulk_market_depth_from_service(instruments: List[Dict]) -> Dict[int, Dict]:
    """Fetches market depth for multiple tokens from the Market Data Microservice."""
    if not instruments:
        logger.warning("ZMQ bulk market depth request skipped: no instruments provided.")
        return {}

    logger.debug(f"📤 [market_data.data_client] ZMQ bulk-depth request count={len(instruments)} "
        f"sample={instruments[:3]}"
    )

    response = await send_zmq_request("get_bulk_market_depth", {"instruments": instruments})

    success = bool(response and response.get("success"))
    payload = (response or {}).get("data", {}) or {}
    error = (response or {}).get("error")

    logger.info(
        # f"📥 [market_data.data_client] ZMQ bulk-depth response success={success} "
        f"items={len(payload)} error={error}"
    )

    if success:
        normalized = {int(k): v for k, v in payload.items()}
        first_key = next(iter(normalized), None)
        if first_key is not None:
            logger.info(
                f"✅ [market_data.data_client] first depth item token={first_key}"
            )
        else:
            logger.warning(
                f"ZMQ bulk depth returned 0 items for {len(instruments)} instruments "
                f"| raw={str(response)[:1500]}"
            )
        return normalized
    else:
        logger.warning(
            f"ZMQ error fetching bulk market depth: {error} | raw={str(response)[:1500]}"
        )
        return {}

async def get_bulk_ltp_from_service(tokens: List[int]) -> Dict[int, float]:
    """Fetches LTP for multiple tokens from the Market Data Microservice."""
    response = await send_zmq_request("get_bulk_ltp", {"tokens": tokens})
    if response and response.get('success'):
        # ZMQ response keys are strings
        return {int(k): float(v) for k, v in response.get('data', {}).items()}
    else:
        error = response.get('error', 'Unknown error')
        logger.warning(f"ZMQ error fetching bulk LTP: {error}")
        return {}

async def market_data_service_listener():
    """
    Connects to the Market Data Microservice via ZMQ SUB socket and listens for real-time updates.
    """
    ctx = get_zmq_context()
    sub_socket = ctx.socket(zmq.SUB)
    sub_socket.connect(f"tcp://localhost:{config.ZMQ_MARKETDATA_PUB_PORT}")

    # Subscribe to all topics (empty prefix)
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    logger.info(f"🚀 Listening to Market Data Service stream on tcp://localhost:{config.ZMQ_MARKETDATA_PUB_PORT}...")

    while True:
        try:
            topic, payload_bytes = await sub_socket.recv_multipart()
            message = json.loads(payload_bytes.decode('utf-8'))
            msg_type = message.get('type')

            if msg_type == 'price_update':
                prices = message.get('data', {})
                ts = message.get('ts')
                if prices:
                    state.bulk_update_prices({int(token): float(ltp) for token, ltp in prices.items()}, ts=ts)

            elif msg_type == 'depth_update':
                depth_map = message.get('data', {})
                ts = message.get('ts')
                if depth_map:
                    normalized = {int(token): depth for token, depth in depth_map.items()}
                    state.bulk_update_market_depth(normalized, ts=ts)

        except asyncio.CancelledError:
            logger.info("Market data listener cancelled.")
            break
        except Exception as e:
            logger.error(f"Unexpected error in market data listener: {e}", exc_info=True)
            await asyncio.sleep(5)

async def initialize_market_data_client():
    """Initializes the ZMQ client for the market data service."""
    get_zmq_context()
    if state.prices is None:
        state.prices = {}
    if state.option_chains is None:
        state.option_chains = {}
    logger.info("✅ Market Data Client initialized.")

async def close_market_data_client():
    """Closes the ZMQ context."""
    global _zmq_ctx
    if _zmq_ctx and not _zmq_ctx.closed:
        _zmq_ctx.term()
        logger.info("✅ ZMQ Market Data Client context terminated.")

async def sync_prices_from_service_loop():
    """Alias for market_data_service_listener."""
    await market_data_service_listener()

async def subscribe_active_straddles():
    """
    Sends subscription requests for active straddles to the Market Data Microservice via ZMQ PUSH.
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

        ctx = get_zmq_context()
        push_socket = ctx.socket(zmq.PUSH)
        push_socket.connect(f"tcp://localhost:{config.ZMQ_MARKETDATA_SUB_PORT}")
        
        try:
            payload = {
                "command": "subscribe",
                "payload": {"subscriptions": subscriptions_payload}
            }
            await push_socket.send_json(payload)

            # Update local state optimistically
            total_tokens = sum(len(item['tokens']) for item in subscriptions_payload)
            for item in subscriptions_payload:
                for token in item['tokens']:
                    state.add_subscription(token)
            logger.info(f"✅ Pushed subscription request for {total_tokens} instruments to Market Data Service.")
        finally:
            push_socket.close()

    except Exception as e:
        logger.error(f"Unexpected error during active straddle subscription: {e}", exc_info=True)
