"""
Background tasks for the Market Data service module.
"""
import asyncio
import traceback
from utils.logger import logger
from models.state import state
import config

# This task is for the market data service, so it should use the service-side chain provider
from trading.chain_provider import get_option_chain, get_xts_market_api, get_ltp as fetch_ltp_sync

async def update_option_chain_cache_loop():
    """Periodically build and cache the option chain with Greeks."""
    await asyncio.sleep(2)  # Initial delay
    logger.info("🔄 [MarketData Service] Option chain cache updater started")
    loop = asyncio.get_event_loop()
    
    while True:
        try:
            # --- FIX: Remove dependency on state.db and check for XTS API instance ---
            if not get_xts_market_api():
                logger.debug("[MarketData Service] Option chain loop paused: XTS API not available (shutting down?).")
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
                    5 # strike_range
                )
            
            # Wait for the next update cycle
            await asyncio.sleep(10) # IV calculation is slow, run it less frequently.
            
        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] Option chain cache updater shutting down")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] Option chain cache loop error: {e}")
            logger.error(traceback.format_exc())
            await asyncio.sleep(30) # Wait longer on error

async def calculate_greeks_loop():
    """
    NEW: High-speed, real-time greeks calculation loop.
    This task consumes price-updated chains from the broadcast queue,
    calculates greeks, and broadcasts the final chain to the main app.
    """
    logger.info("🚀 [MarketData Service] Real-time Greeks calculation loop started.")
    while True:
        try:
            # This queue is populated by _update_chain in chain_provider.py
            message = await state.broadcast_queue.get()
            if message.get('type') != 'chain_for_greeks_calc':
                # If it's not for us, put it back for the other broadcast manager.
                # This is a simple way to share the queue.
                await state.broadcast_queue.put(message)
                await asyncio.sleep(0.01) # yield control
                continue

            chain_data = message.get('data')
            if not chain_data:
                continue

            symbol = chain_data.get('symbol')
            fut_ltp = chain_data.get('fut_ltp')
            dte = chain_data.get('dte')
            risk_free_rate = 0.0

            if not all([symbol, fut_ltp, dte]):
                logger.warning(f"Skipping greeks calculation for {symbol}, missing critical data.")
                continue

            logger.debug(f"Calculating real-time greeks for {symbol}...")
            for row in chain_data['chain']:
                ce_ltp = row.get('ce_ltp', 0.0)
                pe_ltp = row.get('pe_ltp', 0.0)

                ce_greeks = calculate_all_greeks("call", row['strike'], fut_ltp, dte, ce_ltp, risk_free_rate) if ce_ltp > 0 else {}
                pe_greeks = calculate_all_greeks("put", row['strike'], fut_ltp, dte, pe_ltp, risk_free_rate) if pe_ltp > 0 else {}

                is_ce_itm = row['strike'] < fut_ltp
                is_pe_itm = row['strike'] > fut_ltp

                if is_ce_itm and pe_greeks.get('iv', 0) > 0:
                    ce_greeks = calculate_greeks_from_iv("call", row['strike'], fut_ltp, dte, pe_greeks.get('iv'), risk_free_rate)
                elif is_pe_itm and ce_greeks.get('iv', 0) > 0:
                    pe_greeks = calculate_greeks_from_iv("put", row['strike'], fut_ltp, dte, ce_greeks.get('iv'), risk_free_rate)

                row.update({
                    "ce_iv": round(ce_greeks.get("iv", 0) * 100, 2), "ce_delta": ce_greeks.get("delta", 0),
                    "ce_gamma": ce_greeks.get("gamma", 0), "ce_vega": ce_greeks.get("vega", 0), "ce_theta": ce_greeks.get("theta", 0),
                    "pe_iv": round(pe_greeks.get("iv", 0) * 100, 2), "pe_delta": pe_greeks.get("delta", 0),
                    "pe_gamma": pe_greeks.get("gamma", 0), "pe_vega": pe_greeks.get("vega", 0), "pe_theta": pe_greeks.get("theta", 0),
                })

            # Broadcast the final, greeks-included chain to the main application
            final_message = {'type': 'option_chain_update', 'symbol': symbol, 'data': chain_data}
            await state.broadcast_queue.put(final_message)

        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] Greeks calculation loop shutting down.")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] Greeks calculation loop error: {e}", exc_info=True)

async def process_market_data_queue():
    """Processes incoming market data from the socket queue."""
    logger.info("🔄 [MarketData Service] Market data queue processor started.")
    while True:
        try:
            # Wait for an item from the queue
            data = await state.market_data_queue.get()
            
            token = data.get('ExchangeInstrumentID')
            ltp = data.get('LastTradedPrice') or data.get('Touchline', {}).get('LastTradedPrice')
            
            if token and ltp:
                # Update the shared state price cache
                state.update_price(int(token), float(ltp))

            # Mark the task as done
            state.market_data_queue.task_done()
            
        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] Market data queue processor shutting down.")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] Market data queue processor error: {e}", exc_info=True)

async def rest_polling_loop():
    """
    Fallback REST polling loop for prices when WebSocket is disconnected.
    """
    logger.info("🔄 [MarketData Service] REST polling loop started.")
    
    while True:
        try:
            # This loop only runs when the socket is disconnected
            if state.socket_connected:
                await asyncio.sleep(5)
                continue

            subscribed_tokens = list(state.subscribed_tokens)
            if not subscribed_tokens:
                await asyncio.sleep(5)
                continue
            
            logger.debug(f"[REST Poll] Fetching prices for {len(subscribed_tokens)} tokens...")
            loop = asyncio.get_event_loop()
            
            # Fetch prices in parallel using the thread pool executor
            tasks = [loop.run_in_executor(None, fetch_ltp_sync, token) for token in subscribed_tokens]
            prices = await asyncio.gather(*tasks)
            
            updates = 0
            for token, price in zip(subscribed_tokens, prices):
                if price > 0:
                    state.update_price(token, price)
                    updates += 1
            
            if updates > 0:
                logger.debug(f"[REST Poll] Synced {updates} prices.")
            
            await asyncio.sleep(getattr(config, 'REST_POLLING_INTERVAL', 1.0))

        except asyncio.CancelledError:
            logger.info("🔄 [MarketData Service] REST polling loop shutting down.")
            break
        except Exception as e:
            logger.error(f"❌ [MarketData Service] REST polling loop error: {e}", exc_info=True)
            await asyncio.sleep(10)

async def monitor_xts_socket_status():
    """Monitors the XTS socket connection state and just logs changes."""
    logger.info("🚦 [MarketData Service] XTS Socket Status Monitor started")
    last_status = None
    while True:
        await asyncio.sleep(2)
        current_status = state.socket_connected
        if current_status != last_status:
            logger.info(f"🚦 [MarketData Service] XTS Socket status changed to: {'CONNECTED' if current_status else 'DISCONNECTED'}")
            last_status = current_status