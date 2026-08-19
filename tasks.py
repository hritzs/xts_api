"""
Background tasks for the Market Data service module.
"""
import asyncio
import traceback
from utils.logger import logger
import time
import json
import functools
from typing import List, Dict
import zmq
import zmq.asyncio

from models.state import state
import config


async def _push_depth_update_to_snapshot_service(token: int, depth: dict):
    """
    Forward compact normalized depth updates to snapshot_service over ZMQ.

    snapshot_service runs in a separate process, so plain in-memory writes in this
    process are not visible there. We therefore forward the normalized top-of-book
    payload over IPC.
    """
    ctx = zmq.asyncio.Context.instance()
    socket = ctx.socket(zmq.PUSH)
    try:
        socket.connect(f"tcp://127.0.0.1:{getattr(config, 'ZMQ_SNAPSHOT_PULL_PORT', 5566)}")
        await socket.send_json({
            "type": "depth_update",
            "token": int(token),
            "depth": {
                "ltp": depth.get("ltp", 0.0),
                "last_price": depth.get("last_price", 0.0),
                "bid": depth.get("bid", 0.0),
                "ask": depth.get("ask", 0.0),
                "bid_price": depth.get("bid_price", 0.0),
                "ask_price": depth.get("ask_price", 0.0),
                "bid_qty": depth.get("bid_qty", 0),
                "ask_qty": depth.get("ask_qty", 0),
                "depth_available": depth.get("depth_available", False),
                "_ts": time.time(),
            }
        })
    finally:
        socket.close(0)


def _extract_price_from_depth(depth: dict) -> float:
    """
    Extract a usable price from normalized depth payload.
    """
    if not depth:
        return 0.0

    p = (
        depth.get("ltp")
        or depth.get("last_price")
        or depth.get("ask")
        or depth.get("ask_price")
        or depth.get("bid")
        or depth.get("bid_price")
        or 0.0
    )
    try:
        p = float(p)
        return p if p > 0 else 0.0
    except Exception:
        return 0.0


async def update_option_chain_cache_loop():
    """Periodically build and cache the option chain with Greeks."""
    await asyncio.sleep(2)
    logger.info("🔄 [MarketData Service] Option chain cache updater started")

    from trading.chain_provider import get_option_chain, get_xts_market_api
    loop = asyncio.get_event_loop()

    while True:
        try:
            if not get_xts_market_api() or not state.db:
                logger.debug("[MarketData Service] Option chain loop paused: Dependencies not available (shutting down?).")
                await asyncio.sleep(10)
                continue

            symbols_to_update = ["NIFTY", "SENSEX"]

            for symbol in symbols_to_update:
                logger.debug(f"🔄 [MarketData Service] Starting option chain cache update for {symbol}...")

                await loop.run_in_executor(
                    None,
                    get_option_chain,
                    symbol,
                    15
                )

                chain = state.option_chains.get(symbol)

                if chain and hasattr(state, 'chain_shms'):
                    if symbol not in state.chain_shms:
                        from core.shared_memory import ChainSHM
                        state.chain_shms[symbol] = ChainSHM(symbol, create=True)
                    state.chain_shms[symbol].write(chain)

                if chain and hasattr(state, 'tick_publisher'):
                    asyncio.create_task(state.tick_publisher.publish(symbol))

            await asyncio.sleep(1)

        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] Option chain cache updater shutting down")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] Option chain cache loop error: {e}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(5)


async def process_market_data_queue():
    """
    Processes incoming market data ticks from the queue and updates local runtime state.

    This is the primary live-stream cache update path for:
    - LTP cache via state.update_price(...)
    - market depth cache via state.set_market_depth(...)
    - cross-process depth forwarding to snapshot_service via ZMQ
    """
    logger.info("📊 [MarketData Service] Market data queue processor started")

    while True:
        try:
            tick = await state.market_data_queue.get()

            if tick is None:
                logger.info("📊 [MarketData Service] Market data queue processor shutting down.")
                break

            token = tick.get('ExchangeInstrumentID')
            ltp = tick.get('LastTradedPrice') or tick.get('Touchline', {}).get('LastTradedPrice')
            normalized_depth = tick.get("_normalized_depth") or {}

            if token:
                try:
                    token = int(token)
                except Exception:
                    token = None

            if token:
                if normalized_depth:
                    try:
                        state.set_market_depth(token, normalized_depth)
                    except Exception as depth_e:
                        logger.debug(f"Failed to update market depth for {token}: {depth_e}")

                    try:
                        await _push_depth_update_to_snapshot_service(token, normalized_depth)
                    except Exception as zmq_e:
                        logger.debug(f"Failed to forward depth update for {token}: {zmq_e}")

                if ltp:
                    try:
                        state.update_price(token, float(ltp))
                    except Exception as price_e:
                        logger.debug(f"Failed to update price for {token}: {price_e}")
                elif normalized_depth:
                    try:
                        p = _extract_price_from_depth(normalized_depth)
                        if p > 0:
                            state.update_price(token, p)
                    except Exception as fallback_price_e:
                        logger.debug(f"Failed to derive fallback price from depth for {token}: {fallback_price_e}")

        except asyncio.CancelledError:
            logger.info("📊 [MarketData Service] Market data queue processor cancelled.")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] Error processing market data tick: {e}", exc_info=True)


async def rest_polling_loop():
    """
    Polls for market data via REST API when the WebSocket connection is down.
    This runs as a background task in the microservice.
    """
    logger.info("🔄 [MarketData Service] REST polling ready (fallback)")

    from trading.chain_provider import get_xts_market_api, get_bulk_market_depth

    while True:
        try:
            if state.socket_connected:
                await asyncio.sleep(config.REST_POLL_INTERVAL_CONNECTED)
                continue

            logger.warning("🔌 [MarketData Service] Socket is down. Using REST polling for price updates.")
            xt_market = get_xts_market_api()
            if not xt_market:
                logger.error("❌ [MarketData Service] XTS Market not initialized for REST polling.")
                await asyncio.sleep(config.REST_POLL_INTERVAL_ERROR)
                continue

            subscribed_tokens = list(state.subscribed_tokens)
            if not subscribed_tokens:
                logger.debug("[MarketData Service] No tokens subscribed for REST polling. Waiting...")
                await asyncio.sleep(config.REST_POLL_INTERVAL_NO_TOKENS)
                continue

            instruments_to_fetch = []
            token_segment_map = getattr(state, "token_segment_map", None) or getattr(state, "token_exchange_map", {})

            for token in subscribed_tokens:
                exchange_segment = token_segment_map.get(token, config.EXCHANGE_NSEFO)
                instruments_to_fetch.append({
                    'exchangeSegment': exchange_segment,
                    'exchangeInstrumentID': token
                })

            if instruments_to_fetch:
                depth_data = await get_bulk_market_depth(instruments_to_fetch)

                for token, data in depth_data.items():
                    try:
                        token = int(token)
                    except Exception:
                        continue

                    if not isinstance(data, dict):
                        continue

                    try:
                        state.set_market_depth(token, data)
                    except Exception as depth_e:
                        logger.debug(f"Failed to cache REST depth for {token}: {depth_e}")

                    try:
                        await _push_depth_update_to_snapshot_service(token, data)
                    except Exception as zmq_e:
                        logger.debug(f"Failed to forward REST depth for {token}: {zmq_e}")

                    if 'ltp' in data and data.get('ltp'):
                        try:
                            state.update_price(token, float(data['ltp']))
                        except Exception as price_e:
                            logger.debug(f"Failed to update REST LTP for {token}: {price_e}")
                    else:
                        try:
                            p = _extract_price_from_depth(data)
                            if p > 0:
                                state.update_price(token, p)
                        except Exception as mid_e:
                            logger.debug(f"Failed to derive REST fallback price for {token}: {mid_e}")

                logger.debug(f"[MarketData Service] REST polled {len(depth_data)} prices.")

            await asyncio.sleep(config.REST_POLL_INTERVAL_NORMAL)

        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] REST polling shutting down")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] REST polling error: {e}", exc_info=True)
            await asyncio.sleep(config.REST_POLL_INTERVAL_ERROR)


async def monitor_xts_socket_status():
    """Monitors the XTS socket connection state and broadcasts changes."""
    logger.info("🚦 [MarketData Service] XTS Socket Status Monitor started")
    last_status = None
    last_data_source = None

    while True:
        await asyncio.sleep(2)
        current_status = state.socket_connected
        current_data_source = getattr(state, 'data_source', 'UNKNOWN')

        if current_status != last_status or current_data_source != last_data_source:
            if current_status != last_status:
                logger.info(
                    f"🚦 [MarketData Service] XTS Socket status changed to: "
                    f"{'CONNECTED' if current_status else 'DISCONNECTED'}"
                )
            if current_data_source != last_data_source:
                logger.info(f"🚦 [MarketData Service] Market data source changed to: {current_data_source}")

            last_status = current_status
            last_data_source = current_data_source