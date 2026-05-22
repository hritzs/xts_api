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
from models.state import state

async def update_option_chain_cache_loop():
    """Periodically build and cache the option chain with Greeks."""
    await asyncio.sleep(2)  # Initial delay
    logger.info("🔄 [MarketData Service] Option chain cache updater started")
    # Local import to avoid circular dependency issues at startup
    from market_data.chain_provider import get_option_chain, get_xts_market_api
    loop = asyncio.get_event_loop()
    
    while True:
        try:
            # --- FIX: Add defensive check for dependencies during shutdown ---
            if not get_xts_market_api() or not state.db:
                logger.debug("[MarketData Service] Option chain loop paused: Dependencies not available (shutting down?).")
                await asyncio.sleep(10)
                continue
            # --- END FIX ---

            # Can be made dynamic later to support multiple symbols
            symbols_to_update = ["NIFTY", "SENSEX"] 
            
            for symbol in symbols_to_update:
                logger.debug(f"🔄 [MarketData Service] Starting option chain cache update for {symbol}...")
                # Run the synchronous get_option_chain in a thread pool executor
                # This function calculates all greeks and caches the result in state.option_chains
                await loop.run_in_executor(
                    None, 
                    get_option_chain, 
                    symbol, 
                    15 # strike_range
                )
                chain = state.option_chains.get(symbol)
                # After: state.option_chains[symbol] = chain
                if chain and hasattr(state, 'chain_shms'):
                    if symbol not in state.chain_shms:
                        from core.shared_memory import ChainSHM
                        state.chain_shms[symbol] = ChainSHM(symbol, create=True)
                    state.chain_shms[symbol].write(chain)

                if chain and hasattr(state, 'tick_publisher'):
                    asyncio.create_task(state.tick_publisher.publish(symbol)
                )
            
            # Wait for the next update cycle
            await asyncio.sleep(1) # Update every 1 second
            
        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] Option chain cache updater shutting down")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] Option chain cache loop error: {e}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(5) # Wait longer on error


# --- NEW: Moved from client-side market_data.data_processor ---
async def process_market_data_queue():
    """
    Processes incoming market data ticks from the queue and updates the state.
    This runs as a background task in the microservice.
    """
    logger.info("📊 [MarketData Service] Market data queue processor started")
    while True:
        try:
            tick = await state.market_data_queue.get()
            if tick is None: # Sentinel value to stop the loop
                logger.info("📊 [MarketData Service] Market data queue processor shutting down.")
                break

            token = tick.get('ExchangeInstrumentID')
            ltp = tick.get('LastTradedPrice') or tick.get('Touchline', {}).get('LastTradedPrice')

            if token and ltp:
                state.update_price(int(token), float(ltp))
                # logger.debug(f"Updated price for {token}: {ltp}") # Too verbose

        except asyncio.CancelledError:
            logger.info("📊 [MarketData Service] Market data queue processor cancelled.")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] Error processing market data tick: {e}", exc_info=True)


# --- NEW: Moved from client-side market_data.socket_callbacks and background.tasks ---
async def rest_polling_loop():
    """
    Polls for market data via REST API when the WebSocket connection is down.
    This runs as a background task in the microservice.
    """
    logger.info("🔄 [MarketData Service] REST polling ready (fallback)")
    # Local import to avoid circular dependency
    from market_data.chain_provider import get_xts_market_api, get_bulk_market_depth
    import config

    while True:
        try:
            if state.socket_connected:
                await asyncio.sleep(config.REST_POLL_INTERVAL_CONNECTED) # Poll less frequently if socket is up
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
            for token in subscribed_tokens:
                # Assuming state.token_exchange_map exists and is populated
                exchange_segment = state.token_exchange_map.get(token, config.EXCHANGE_NSEFO)
                instruments_to_fetch.append({
                    'exchangeSegment': exchange_segment,
                    'exchangeInstrumentID': token
                })

            if instruments_to_fetch:
                # Use get_bulk_market_depth for efficiency, it fetches LTP as well
                depth_data = await get_bulk_market_depth(instruments_to_fetch)
                for token, data in depth_data.items():
                    if 'ltp' in data: # Assuming get_bulk_market_depth returns ltp directly or can be derived
                        state.update_price(token, float(data['ltp']))
                    elif 'ask_price' in data and 'bid_price' in data:
                        # Fallback to mid-price if only depth is available
                        mid_price = (data['ask_price'] + data['bid_price']) / 2
                        state.update_price(token, mid_price)
                logger.debug(f"[MarketData Service] REST polled {len(depth_data)} prices.")

            await asyncio.sleep(config.REST_POLL_INTERVAL_NORMAL)

        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] REST polling shutting down")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] REST polling error: {e}", exc_info=True)
            await asyncio.sleep(config.REST_POLL_INTERVAL_ERROR)


# --- NEW: Moved from main app's background.tasks ---
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
                logger.info(f"🚦 [MarketData Service] XTS Socket status changed to: {'CONNECTED' if current_status else 'DISCONNECTED'}")
            if current_data_source != last_data_source:
                logger.info(f"🚦 [MarketData Service] Market data source changed to: {current_data_source}")

            # No broadcast needed from microservice, main app will poll /api/health
            last_status = current_status
            last_data_source = current_data_source