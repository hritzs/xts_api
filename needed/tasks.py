"""
Background Tasks - Market Data, Verification, and Monitoring
"""
import asyncio
import functools
from typing import Set, List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass
from utils.logger import logger
from models.state import state
from utils.greeks import calculate_all_greeks, calculate_straddle_greeks, implied_volatility, calculate_greeks_from_iv # Keep this
from utils.helpers import calculate_dte, get_ist_now
from market_data import get_option_chain, get_ltp, SYMBOL_CONFIG, get_spot_details, get_bulk_ltp
from trading.pnl_calculator import calculate_aggregate_pnl
from trading.order_manager import get_order_book
from trading.order_executor import get_order_executor
from utils.helpers import get_ist_now
from trading.data_client import get_option_chain_from_service


# WebSocket clients (imported from websocket module)
_websocket_clients: Set = set()

# --- NEW: Dataclasses moved from greeks_calculator.py ---
@dataclass
class PositionGreeks:
    """Greeks for a single position"""
    token: int
    strike: int
    option_type: str
    quantity: int  # Signed: +ve for BUY, -ve for SELL
    ltp: float
    delta: float
    gamma: float
    theta: float
    vega: float
    iv: float

    @property
    def net_delta(self) -> float:
        return self.delta * self.quantity

    @property
    def net_gamma(self) -> float:
        return self.gamma * self.quantity

    @property
    def net_theta(self) -> float:
        return self.theta * self.quantity

    @property
    def net_vega(self) -> float:
        return self.vega * self.quantity

@dataclass
class TradeGreeks:
    """Aggregated Greeks for entire trade"""
    trade_uid: str
    positions: List[PositionGreeks]

    @property
    def total_delta(self) -> float:
        return sum(p.net_delta for p in self.positions)

    @property
    def total_gamma(self) -> float:
        return sum(p.net_gamma for p in self.positions)

    @property
    def total_theta(self) -> float:
        return sum(p.net_theta for p in self.positions)

    @property
    def total_vega(self) -> float:
        return sum(p.net_vega for p in self.positions)

    @property
    def avg_iv(self) -> float:
        if not self.positions:
            return 0.0
        return sum(p.iv for p in self.positions) / len(self.positions)
# --- END NEW ---

def set_websocket_clients(clients: Set):
    """Set reference to WebSocket clients"""
    global _websocket_clients
    _websocket_clients = clients


async def broadcast_message(message: dict):
    """Broadcast to all WebSocket clients"""
    if not _websocket_clients:
        return
    
    disconnected = set()
    
    # FIX: Iterate over a copy (list) to prevent "Set changed size during iteration" error
    # if a client disconnects (and is removed) while we are awaiting send_json.
    for client in list(_websocket_clients):
        try:
            # Add a timeout to prevent the loop from hanging on a single bad client
            await asyncio.wait_for(client.send_json(message), timeout=2.0)
        except Exception as e: # Catches TimeoutError and other connection errors
            # --- FIX: Improve logging for websocket errors during broadcast ---
            # The specific exception type might not have a useful string representation.
            # Log the exception type and details for better debugging.
            logger.warning(f"Failed to send message to a websocket client, will disconnect it. Error: {type(e).__name__} - {e}")
            disconnected.add(client)
    
    for client in disconnected:
        _websocket_clients.discard(client)

async def broadcast_log(level: str, message: str):
    """Broadcasts a log message to the frontend terminal."""
    await broadcast_message({
        'type': 'log_message',
        'data': {
            'level': level.upper(),
            'message': message,
            'timestamp': get_ist_now().isoformat()
        }
    })


# ============================================================================
# VERIFICATION TASK
# ============================================================================

async def verify_orders_task(
    order_ids: List[str],
    batch_name: str = "BATCH"
) -> Dict:
    """
    ✅ BACKGROUND VERIFICATION TASK
    Runs independently after order execution

    Args:
        order_ids: List of order IDs to verify
        batch_name: Name for logging

    Returns:
        Verification result dict
    """
    try:
        if not order_ids:
            logger.warning(f"⚠️  No orders to verify for {batch_name}")
            return {'verified_success': [], 'verified_failed': []}

        executor = get_order_executor()
        if not executor:
            logger.error(f"❌ OrderExecutor not available for {batch_name}")
            return {'verified_success': [], 'verified_failed': []}
        
        # --- OPTIMIZATION: Removed redundant sleep. The verify_orders_bulk function has its own internal wait. ---
        # The 'delay' parameter is now unused but kept for signature compatibility.

        await broadcast_log('INFO', f"[{batch_name}] Verifying {len(order_ids)} orders...")

        # Call bulk verification (run in thread pool to avoid blocking)
        # --- REFACTOR: Call the async version directly instead of using an executor ---
        # The async version now correctly handles blocking calls and chasing internally.
        result = await executor.verify_orders_bulk(order_ids)
        # --- END REFACTOR ---

        verified_success = result.get('verified_success', [])
        verified_failed = result.get('verified_failed', [])

        # The async verify_orders_bulk handles chasing/cancellation internally, so we just report the final result.
        log_msg = f"[{batch_name}] Verification Complete: {len(verified_success)} OK, {len(verified_failed)} Failed."
        logger.info("="*100)
        logger.info(f"✅ [{batch_name}] VERIFICATION COMPLETE")
        logger.info(f"✅ Verified: {len(verified_success)}/{len(order_ids)} | ❌ Failed: {len(verified_failed)}")
        logger.info("="*100)
        await broadcast_log('SUCCESS' if not verified_failed else 'WARNING', log_msg)

        # Update state with verified orders
        if verified_success or verified_failed:
            verification_result = {
                'batch_name': batch_name,
                'timestamp': get_ist_now().isoformat(),
                'verified': verified_success,
                'failed': verified_failed,
                'total': len(order_ids)
            }

            # Store in state
            if not hasattr(state, 'verification_results'):
                state.verification_results = {}
            state.verification_results[batch_name] = verification_result

            # Broadcast to WebSocket clients
            await broadcast_message({
                'type': 'verification_complete',
                'data': {
                    'batch_name': batch_name,
                    'verified_count': len(verified_success),
                    'failed_count': len(verified_failed),
                    'total': len(order_ids)
                },
                'timestamp': get_ist_now().isoformat()
            })

        return result

    except Exception as e:
        logger.error(f"❌ Verification task failed for {batch_name}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'verified_success': [], 'verified_failed': []}


def start_verification_task(
    order_ids: List[str],
    batch_name: str = "BATCH"
) -> Optional[asyncio.Task]:
    """
    Start verification as a background task (non-blocking)
    
    Usage:
        start_verification_task(order_ids, "BUILD_trade123")
    
    Returns:
        asyncio.Task or None
    """
    try:
        loop = asyncio.get_event_loop()
        task = loop.create_task(
            verify_orders_task(order_ids, batch_name)
        )
        
        log_msg = f"Verification task started for {batch_name} ({len(order_ids)} orders)"
        logger.info(f"🚀 {log_msg}")
        
        # Store task reference
        if not hasattr(state, 'verification_tasks'):
            state.verification_tasks = {}
        state.verification_tasks[batch_name] = task
        
        # Add callback when task completes
        def on_complete(future):
            try:
                result = future.result()
                verified = len(result.get('verified_success', []))
                failed = len(result.get('verified_failed', []))
                log_msg = f"Task complete for {batch_name}: {verified} verified, {failed} failed."
                logger.info(f"✅ {log_msg}")
                
                # Remove from active tasks
                if hasattr(state, 'verification_tasks') and batch_name in state.verification_tasks:
                    del state.verification_tasks[batch_name]
                    
            except Exception as e:
                asyncio.run(broadcast_log('ERROR', f"Verification task for {batch_name} failed internally."))
                logger.error(f"❌ Verification task error for {batch_name}: {e}")
        
        task.add_done_callback(on_complete)
        
        return task
        
    except Exception as e:
        logger.error(f"❌ Failed to start verification task: {e}")
        return None


# ============================================================================
# MARKET DATA PROCESSING
# ============================================================================

async def monitor_xts_socket_status():
    """Monitors the XTS socket connection state and broadcasts changes."""
    logger.info("🚦 XTS Socket Status Monitor started")
    last_status = None
    last_data_source = None
    while True:
        await asyncio.sleep(2)
        current_status = state.socket_connected
        current_data_source = getattr(state, 'data_source', 'UNKNOWN')

        if current_status != last_status or current_data_source != last_data_source:
            if current_status != last_status:
                logger.info(f"🚦 XTS Socket status changed to: {'CONNECTED' if current_status else 'DISCONNECTED'}")
            if current_data_source != last_data_source:
                logger.info(f"🚦 Market data source changed to: {current_data_source}")

            await broadcast_message({
                'type': 'xts_socket_status',
                'data': {'connected': True, 'dataSource': "MICROSERVICE"} # Hardcoded for new architecture
            })
            last_status = current_status
            last_data_source = current_data_source


# ============================================================================
# ORDER BOOK & PNL UPDATES
# ============================================================================

def get_live_pnl_data() -> dict:
    """
    Calculate live PnL for all positions for on-demand API calls.
    
    Uses calculate_aggregate_pnl from pnl_calculator.
    """
    try:
        if not state.db:
            return {}
        
        # Get active straddles
        straddles = state.db.get_active_straddles()
        
        if not straddles:
            return {
                'total_pnl': 0.0,
                'realized_pnl': 0.0,
                'unrealized_pnl': 0.0,
                'active_trades': 0
            }
        
        # Calculate aggregate P&L
        pnl_data = calculate_aggregate_pnl(straddles, state.prices)
        
        return pnl_data
        
    except Exception as e:
        logger.error(f"❌ Live PnL calculation error for API: {e}")
        return {
            'total_pnl': 0.0,
            'realized_pnl': 0.0,
            'unrealized_pnl': 0.0,
            'active_trades': 0
        }


async def update_order_book_loop():
    """Periodically update order book from broker"""
    await asyncio.sleep(10)
    logger.info("📋 Order book sync started")
    
    try:
        loop = asyncio.get_event_loop() # Get loop once
        while True:
            try:
                await asyncio.sleep(10)
                
                # Run blocking calls in an executor to not block the event loop
                orders = await loop.run_in_executor(None, get_order_book)
                
                if orders and state.db:
                    # Use the new bulk insert method, also in an executor
                    await loop.run_in_executor(None, state.db.insert_orders_bulk, orders)
                        
            except asyncio.CancelledError:
                logger.info("📋 Order book sync shutting down")
                break
            except Exception as e:
                logger.error(f"❌ Order book sync error: {e}")
                await asyncio.sleep(10)
    except asyncio.CancelledError:
        logger.info("📋 Order book sync cancelled")


# ============================================================================
# CLEANUP TASK
# ============================================================================

async def cleanup_old_data():
    """Clean up old data from database periodically"""
    logger.info("🧹 Cleanup task started")
    
    try:
        while True:
            try:
                # Run cleanup every 6 hours
                await asyncio.sleep(21600)
                
                if state.db:
                    logger.info("🧹 Running database cleanup...")
                    
                    # Clean up old orders (older than 30 days)
                    state.db.cleanup_old_orders(days=30)
                    
                    # Clean up old logs (if applicable)
                    # state.db.cleanup_old_logs(days=30)
                    
                    logger.info("✅ Database cleanup complete")
                    
            except asyncio.CancelledError:
                logger.info("🧹 Cleanup task shutting down")
                break
            except Exception as e:
                logger.error(f"❌ Cleanup error: {e}")
                await asyncio.sleep(3600)  # Wait 1 hour on error
                
    except asyncio.CancelledError:
        logger.info("🧹 Cleanup task cancelled")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_verification_status(batch_name: str) -> Optional[Dict]:
    """
    Get verification status for a specific batch
    
    Args:
        batch_name: Name of the batch (e.g., "BUILD_trade123")
    
    Returns:
        Verification result dict or None
    """
    if hasattr(state, 'verification_results'):
        return state.verification_results.get(batch_name)
    return None


def is_verification_running(batch_name: str) -> bool:
    """
    Check if verification is still running for a batch
    
    Args:
        batch_name: Name of the batch
    
    Returns:
        True if verification task is still running
    """
    if hasattr(state, 'verification_tasks'):
        task = state.verification_tasks.get(batch_name)
        return task is not None and not task.done()
    return False


async def wait_for_verification(batch_name: str, timeout: float = 30.0) -> Optional[Dict]:
    """
    Wait for verification to complete (with timeout)
    
    Args:
        batch_name: Name of the batch
        timeout: Maximum time to wait in seconds
    
    Returns:
        Verification result or None if timeout
    """
    try:
        if not hasattr(state, 'verification_tasks'):
            return None
        
        task = state.verification_tasks.get(batch_name)
        if not task:
            # Already completed, check results
            return get_verification_status(batch_name)
        
        # Wait for task with timeout
        result = await asyncio.wait_for(task, timeout=timeout)
        return result
        
    except asyncio.TimeoutError:
        logger.warning(f"⚠️  Verification timeout for {batch_name}")
        return None
    except Exception as e:
        logger.error(f"❌ Error waiting for verification: {e}")
        return None


# Removed sync_prices_loop and subscribe_active_straddles as they are now in market_data.data_client

async def _create_snapshot_for_trade(trade: dict, spot_details_map: dict, trade_data_override: dict = None):
    """Internal helper to create a snapshot for a single trade."""
    trade_uid = trade.get('trade_uid')
    if not trade_uid:
        return # Cannot create snapshot without trade_uid
    
    try:
        loop = asyncio.get_event_loop()

        # --- ROBUSTNESS FIX: Fetch DB data and reconcile with Cache ---
        db_trade_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
        
        if trade_data_override:
            full_trade_data = trade_data_override
            # Update cache with override as it is the most authoritative source
            if hasattr(state, 'trade_data_cache'):
                state.trade_data_cache[trade_uid] = {'data': trade_data_override, 'timestamp': datetime.now().timestamp()}
        else:
            # Check cache for critical data (Realized PnL) that might be missing in DB due to lag/overwrite
            full_trade_data = db_trade_data
            
            if hasattr(state, 'trade_data_cache'):
                cached_entry = state.trade_data_cache.get(trade_uid)
                if cached_entry:
                    cached_trade = cached_entry.get('data')
                    if cached_trade and full_trade_data:
                        cached_pnl = cached_trade.get('realized_pnl', 0.0)
                        db_pnl = full_trade_data.get('realized_pnl', 0.0)
                        
                        # If DB has lost the PnL (0) but cache has it (>0), restore it.
                        if cached_pnl != 0 and db_pnl == 0:
                            logger.warning(f"⚠️ DB Data Loss Detected for {trade_uid}: DB Realized PnL=0, Cache={cached_pnl}. Restoring from Cache and repairing DB.")
                            full_trade_data['realized_pnl'] = cached_pnl
                            if cached_trade.get('psqf_percentage'):
                                full_trade_data['psqf_percentage'] = cached_trade.get('psqf_percentage')
                            
                            # Trigger async repair to store permanently
                            # We use a copy to avoid modifying the object while it's being used
                            repair_data = full_trade_data.copy()
                            # --- FIX: loop.run_in_executor returns a Future, not a coroutine.
                            # It cannot be passed to asyncio.create_task.
                            # Calling it without 'await' schedules it as a fire-and-forget background job.
                            loop.run_in_executor(None, state.db.insert_straddle, repair_data)
            
        if not full_trade_data:
            logger.error(f"Could not re-fetch full trade data for {trade_uid} in snapshot creation. Using potentially stale data.")
            full_trade_data = trade # Fallback to the passed-in data
        # --- FIX: Ensure 'straddle_id' is always present in full_trade_data ---
        if 'straddle_id' not in full_trade_data:
            full_trade_data['straddle_id'] = trade_uid
        # --- END FIX ---

        # --- ROBUSTNESS FIX: Re-derive exchange segment from symbol ---
        # This ensures that even if the DB has a stale/incorrect segment,
        # we use the correct one for fetching live data (LTP, greeks).
        symbol_upper = full_trade_data.get('symbol', 'NIFTY').upper()
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        
        # --- FIX: Get option chain from map at the beginning to resolve UnboundLocalError ---
        # This ensures `option_chain` is available for the lot_size check below.
        option_chain = spot_details_map.get(symbol_upper)
        # --- END FIX ---
        
        derived_segment = SYMBOL_CONFIG.get(base_symbol, {}).get('segment') if base_symbol else None

        if derived_segment:
            correct_exchange_segment = derived_segment
        else:
            # Fallback to what's in the data, or default to NSEFO
            correct_exchange_segment = full_trade_data.get('exchange_segment', 2)
            logger.warning(f"Could not derive segment for {symbol_upper} from SYMBOL_CONFIG. Falling back to DB value: {correct_exchange_segment}.")
        # --- END FIX ---

        # --- FIX: Get lot size robustly at the beginning of the snapshot ---
        lot_size = full_trade_data.get('lot_size')
        if not lot_size or lot_size <= 0:
            # If lot size is missing from the trade record, we must derive it.
            # The most reliable way is from the live option chain data for that symbol.
            if option_chain and option_chain.get('lot_size'):
                lot_size = option_chain.get('lot_size')
                # --- FIX: Do NOT update DB here. It causes race conditions with PnL updates. ---
                # Just use the derived lot_size for this snapshot calculation in memory.
                logger.info(f"⚠️ Using derived lot_size {lot_size} for {trade_uid} snapshot (DB value missing/invalid).")
            else:
                lot_size = 65 # Last resort fallback
                logger.error(f"Could not determine lot size for {trade_uid}. Falling back to default {lot_size}. Hedging may be incorrect.")
        # `option_chain` is now defined at the top of the function.

        # --- FIX: Add a strict check to ensure spot details are available from the cache ---
        # If the details are not here, it means the option chain cache is not ready.
        # Aborting the snapshot for this cycle prevents any downstream blocking calls.
        if not option_chain or not option_chain.get('fut_ltp'):
            logger.warning(f"📸 Snapshot for {trade_uid} aborted this cycle: Spot/chain details not available in cache.")
            return
        
        # --- NEW: Calculate Live Synthetic Spot from ATM prices ---
        # Instead of relying on the potentially stale 'fut_ltp' from the chain cache,
        # we calculate the synthetic spot using the latest LTPs from state.prices.
        # This ensures Greeks update instantly even if the full chain structure update is slower.
        live_spot_price = option_chain['fut_ltp'] # Default fallback
        try:
            atm_strike = option_chain.get('atm')
            if atm_strike:
                # Find ATM row
                atm_row = next((row for row in option_chain.get('chain', []) if row.get('strike') == atm_strike), None)
                if atm_row:
                    ce_token = atm_row.get('ce_token')
                    pe_token = atm_row.get('pe_token')
                    if ce_token and pe_token:
                        # Get fresh prices from state
                        ce_ltp = state.get_price(int(ce_token))
                        pe_ltp = state.get_price(int(pe_token))
                        
                        if ce_ltp and pe_ltp and ce_ltp > 0 and pe_ltp > 0:
                            # Synthetic Future = Strike + CE - PE
                            live_spot_price = atm_strike + ce_ltp - pe_ltp
        except Exception:
            pass # Keep fallback
        # --- END NEW ---
        
        # --- FIX: Use the fresh option chain data passed into this function for all calculations ---
        # The `option_chain` object is now defined at the top of the function.
        # --- END FIX ---

        # --- UNIFIED POSITION & PNL CALCULATION ---
        # Reconstruct the entire trade state (all positions and PnL) from the order history.
        total_pnl = 0.0
        total_realized_pnl = 0.0
        total_unrealized_pnl = 0.0
        pnl_by_token = {}
        live_positions = []
        positions_for_greeks = []
        trade_orders = []
        pnl_and_position_details = {}
        net_positions = {}

        try:
            # --- ROBUST ORDER RECONSTRUCTION ---
            # This is the core of the snapshot. We reconstruct the entire state
            # from the full order history, which is the single source of truth.

            # 1. Fetch all orders for this specific trade in a non-blocking way.
            # This is much more efficient than fetching all of today's orders.
            all_db_orders = await loop.run_in_executor(
                None, state.db.get_orders_by_trade_id, trade_uid
            )
            trade_orders = [
                o for o in all_db_orders if str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in 
                                            ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']
            ]
            # --- FIX: Add strict filtering by OrderUniqueIdentifier to ensure orders belong to this trade_uid ---
            # The OrderUniqueIdentifier should start with "BUILD_<trade_uid>", "HEDGE_<trade_uid>", "SQF_<trade_uid>", etc.
            # This prevents accidental inclusion of orders from other trades if the DB query is too broad.
            filtered_trade_orders = []
            for order in trade_orders:
                order_uid_from_broker = order.get('OrderUniqueIdentifier') or order.get('order_unique_id')
                # Check if the order_uid_from_broker starts with the trade_uid (e.g., "BUILD_ny123" starts with "ny123")
                if order_uid_from_broker and trade_uid in order_uid_from_broker:
                    filtered_trade_orders.append(order)
                else:
                    logger.warning(f"Skipping order {order.get('AppOrderID')} in snapshot for {trade_uid}: OrderUniqueIdentifier '{order_uid_from_broker}' does not match trade_uid. (This is expected for non-trade-specific orders or if DB filter is loose)")
            trade_orders = filtered_trade_orders
            
            logger.info(f"Found {len(trade_orders)} filled orders for {trade_uid} in DB via get_orders_by_trade_id.")

            # 2. Check the temporary cache for any orders that haven't been persisted or propagated yet.
            # This merge strategy prevents race conditions where the DB is not yet updated when a snapshot is created.
            if hasattr(state, 'temp_order_cache') and state.temp_order_cache:
                # --- UNIFIED CACHE LOGIC ---
                # Read from the single cache key for this trade.
                cached_fills = state.temp_order_cache.get(trade_uid, [])
                if cached_fills:
                    logger.info(f"Found {len(cached_fills)} orders in temp cache for {trade_uid}. Merging with DB orders.")
                    # Get the AppOrderIDs of orders already fetched from the DB to avoid duplicates.
                    db_order_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')) for o in trade_orders}

                    new_fills_from_cache = 0
                    for fill in cached_fills:
                        app_order_id = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid'))
                        if app_order_id and app_order_id not in db_order_ids:
                            trade_orders.append(fill)
                            new_fills_from_cache += 1
                    if new_fills_from_cache > 0:
                        logger.info(f"Added {new_fills_from_cache} new orders from cache for {trade_uid}.")

            if trade_orders:
                # --- REFACTORED PNL LOGIC V3 (Persistent Realized PnL) ---
                
                # 1. Calculate the Total P&L Pool (the current MTM PnL of the entire trade)
                total_pnl_pool = 0.0
                all_tokens = set()
                aggregated_orders = {}

                for order in trade_orders:
                    # --- FIX: Add robust token parsing ---
                    token_val = order.get('exchange_instrument_id') or order.get('ExchangeInstrumentID')
                    if not token_val: continue
                    token = int(token_val)
                    # --- END FIX ---
                    qty = int(order.get('cumulative_quantity') or order.get('CumulativeQuantity', 0))
                    price = float(order.get('order_avg_price') or order.get('OrderAverageTradedPrice', 0))
                    side = str(order.get('order_side') or order.get('OrderSide', '')).upper()
                    all_tokens.add(token)
                    if token not in aggregated_orders:
                        aggregated_orders[token] = {'buy_qty': 0, 'buy_value': 0.0, 'sell_qty': 0, 'sell_value': 0.0}
                    if side == 'BUY': aggregated_orders[token]['buy_qty'] += qty; aggregated_orders[token]['buy_value'] += qty * price
                    elif side == 'SELL': aggregated_orders[token]['sell_qty'] += qty; aggregated_orders[token]['sell_value'] += qty * price

                # --- REFACTOR: Fetch all LTPs in a single bulk call from the market data service ---
                # This is the correct microservice pattern, avoiding reliance on a local cache.
                price_map = {}
                if all_tokens:
                    # We assume all tokens for a single trade belong to the same exchange segment.
                    price_map = await get_bulk_ltp(list(all_tokens), correct_exchange_segment)
                    missing_tokens = [t for t in all_tokens if price_map.get(t, 0) <= 0]
                    if missing_tokens:
                        logger.warning(f"Could not fetch LTP for tokens {missing_tokens} in bulk call.")
                # --- END REFACTOR ---

                for token in all_tokens:
                    aggs = aggregated_orders.get(token, {'buy_qty': 0, 'buy_value': 0.0, 'sell_qty': 0, 'sell_value': 0.0})
                    total_buy_qty, total_buy_value = aggs['buy_qty'], aggs['buy_value']
                    total_sell_qty, total_sell_value = aggs['sell_qty'], aggs['sell_value']
                    net_qty = total_sell_qty - total_buy_qty
                    ltp = price_map.get(token, 0.0)
                    mtm_of_net_pos = net_qty * ltp if ltp > 0 else 0.0
                    # This token_pnl is the Total PnL (Realized + Unrealized) for this specific token
                    token_pnl = (total_sell_value - total_buy_value) - mtm_of_net_pos
                    total_pnl_pool += token_pnl
                
                # 2. Get the PERSISTED realized PnL from the trade document.
                total_realized_pnl = full_trade_data.get('realized_pnl', 0.0)

                # 3. Set the snapshot's realized and unrealized PnL.
                total_unrealized_pnl = total_pnl_pool - total_realized_pnl

                # 4. Determine final open positions and their individual MTM PnL for display apportionment
                total_mtm_of_live_positions = 0.0

                for token in all_tokens:
                    aggs = aggregated_orders.get(token, {'buy_qty': 0, 'buy_value': 0.0, 'sell_qty': 0, 'sell_value': 0.0})
                    net_open_qty = aggs['sell_qty'] - aggs['buy_qty']
                    ltp = price_map.get(token, 0.0)
                    entry_price = 0
                    leg_mtm_pnl = 0.0

                    if net_open_qty != 0:
                        if net_open_qty > 0: # Net short position
                            entry_price = aggs['sell_value'] / aggs['sell_qty'] if aggs['sell_qty'] > 0 else 0
                            if ltp > 0: leg_mtm_pnl = (entry_price - ltp) * net_open_qty
                        elif net_open_qty < 0: # Net long position
                            entry_price = aggs['buy_value'] / aggs['buy_qty'] if aggs['buy_qty'] > 0 else 0
                            if ltp > 0: leg_mtm_pnl = (ltp - entry_price) * abs(net_open_qty)

                    total_mtm_of_live_positions += leg_mtm_pnl

                    strike, option_type = None, None
                    if option_chain and option_chain.get('chain'):
                        for row in option_chain['chain']:
                            if row.get('ce_token') == token:
                                strike, option_type = row['strike'], 'CE'; break
                            if row.get('pe_token') == token:
                                strike, option_type = row['strike'], 'PE'; break

                    pnl_and_position_details[token] = {
                        'net_open_qty': net_open_qty, 'entry_price': entry_price, 'ltp': ltp,
                        'leg_mtm_pnl': leg_mtm_pnl, 'strike': strike, 'option_type': option_type, 'token_pnl': (aggs['sell_value'] - aggs['buy_value']) - (net_open_qty * ltp)
                    }

                    if net_open_qty != 0 and strike is not None: # This is the net position
                        positions_for_greeks.append({
                            "token": token, "strike": strike, "option_type": option_type,
                            "quantity": abs(net_open_qty), "action": 'BUY' if net_open_qty < 0 else 'SELL',
                            "symbol": symbol_upper
                        })
                        # --- FIX: Populate live_positions for the snapshot ---
                        live_positions.append({
                            'token': token, 'strike': strike, 'option_type': option_type,
                            'quantity': abs(net_open_qty), 'action': 'SELL' if net_open_qty > 0 else 'BUY',
                            'entry_price': entry_price, 'ltp': ltp,
                            'pnl': leg_mtm_pnl,
                            # Greeks will be added later after calculation
                        })

                # --- NEW: Add logging to clarify PnL components ---
                # This logging will only trigger if there's a mix of open and closed positions, or if a partial square-off has occurred.
                if (total_realized_pnl > 0 or any(pos['net_open_qty'] == 0 for pos in pnl_and_position_details.values())) and total_mtm_of_live_positions != 0:
                    pnl_from_closed_legs_in_pool = total_pnl_pool - total_mtm_of_live_positions
                    logger.info("="*100)
                    logger.info(f"💰 SNAPSHOT P&L CALCULATION for {trade_uid}")
                    logger.info(f"   - Total PnL Pool (Live MTM of all transactions): ₹{total_pnl_pool:,.2f}")
                    logger.info(f"   - MTM of Open Positions:                         ₹{total_mtm_of_live_positions:,.2f}")
                    logger.info(f"   - Implied PnL from Closed Legs (in pool):        ₹{pnl_from_closed_legs_in_pool:,.2f}")
                    logger.info(f"   - Persisted Realized PnL (from PSQF/SQF):      ₹{total_realized_pnl:,.2f}")
                    logger.info(f"   -> Final Unrealized PnL (Pool - Realized):      ₹{total_unrealized_pnl:,.2f}")
                    logger.info("="*100)
                # --- END NEW ---

                # --- REFACTOR: Calculate Greeks ON THE FLY using cached IV and Live Spot ---
                # This ensures Delta is responsive even if the option chain cache is slightly stale.
                chain_rows = option_chain.get('chain', [])
                iv_lookup = {}
                for row in chain_rows:
                    if row.get('ce_token'): iv_lookup[int(row['ce_token'])] = row.get('ce_iv', 0.0)
                    if row.get('pe_token'): iv_lookup[int(row['pe_token'])] = row.get('pe_iv', 0.0)
                
                dte = option_chain.get('dte', 0)
                risk_free_rate = 0.0

                position_greeks_list = []
                for pos in positions_for_greeks:
                    token = int(pos['token'])
                    cached_iv_pct = iv_lookup.get(token, 0.0)
                    live_ltp = state.get_price(token) or 0.0
                    
                    # --- NEW: Calculate Real-Time IV & Greeks ---
                    # Try to calculate fresh IV from live price. If that fails (e.g. deep ITM/OTM), fallback to cached IV.
                    greeks = {}
                    if live_ltp > 0:
                        greeks = calculate_all_greeks(pos['option_type'].lower(), pos['strike'], live_spot_price, dte, live_ltp, risk_free_rate)
                    
                    if greeks.get('iv', 0) > 0:
                        # Use fresh real-time Greeks
                        greeks['iv'] = greeks['iv'] * 100.0 # Convert decimal to % for consistency
                    elif cached_iv_pct > 0:
                        # Fallback: Use cached IV but recalculate Delta/Gamma with live spot
                        greeks = calculate_greeks_from_iv(pos['option_type'].lower(), pos['strike'], live_spot_price, dte, cached_iv_pct / 100.0, risk_free_rate)
                        greeks['iv'] = cached_iv_pct
                    else:
                        greeks = {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0, 'iv': 0.0}

                    signed_quantity = pos['quantity'] if pos.get('action') == 'BUY' else -pos.get('quantity')
                    pg = PositionGreeks(token=token, strike=pos['strike'], option_type=pos['option_type'], quantity=signed_quantity, ltp=live_ltp, delta=greeks.get('delta', 0.0), gamma=greeks.get('gamma', 0.0), theta=greeks.get('theta', 0.0), vega=greeks.get('vega', 0.0), iv=greeks.get('iv', 0.0))
                    position_greeks_list.append(pg)
                    
                    # --- FIX: Enrich live_positions with Greeks ---
                    for lp in live_positions:
                        if lp['token'] == token:
                            lp['iv'] = greeks.get('iv', 0.0)
                            lp['delta'] = pg.net_delta
                            lp['gamma'] = pg.net_gamma
                            lp['theta'] = pg.net_theta
                            lp['vega'] = pg.net_vega
                trade_greeks = TradeGreeks(trade_uid=trade_uid, positions=position_greeks_list)
                # --- END REFACTOR ---

            else:
                logger.warning(f"No trade orders found for {trade_uid} to build snapshot.")
                

        except Exception as e:
            logger.error(f"Error reconstructing position/pnl for snapshot of {trade_uid}: {e}", exc_info=True)

        total_pnl = total_realized_pnl + total_unrealized_pnl

        # --- FIX: Fallback logic should only trigger if NO orders were found at all. ---
        # If orders were found but the net position is zero (i.e., closed), 
        # `positions_for_greeks` will be empty, and we should NOT fall back.
        if not trade_orders:
            logger.warning(f"Position reconstruction for {trade_uid} yielded no positions. Falling back to main two legs for greeks.")
            ce_token = int(full_trade_data["ce_token"]) if full_trade_data.get("ce_token") else None

            if ce_token: positions_for_greeks.append({"token": ce_token, "strike": full_trade_data["strike"], "option_type": "CE", "quantity": full_trade_data.get("ce_quantity", 0), "action": "SELL", "symbol": full_trade_data.get("symbol", "NIFTY")})
            pe_token = int(full_trade_data["pe_token"]) if full_trade_data.get("pe_token") else None
            if pe_token: positions_for_greeks.append({"token": pe_token, "strike": full_trade_data["strike"], "option_type": "PE", "quantity": full_trade_data.get("pe_quantity", 0), "action": "SELL", "symbol": full_trade_data.get("symbol", "NIFTY")})

            # --- REFACTOR: Calculate Greeks from cached option chain for fallback ---
            # This now reads the complete, real-time greeks from the chain.
            chain_rows = option_chain.get('chain', [])
            greeks_lookup = {}
            for row in chain_rows:
                if row.get('ce_token'): greeks_lookup[row['ce_token']] = {'iv': row.get('ce_iv', 0.0), 'delta': row.get('ce_delta', 0.0), 'gamma': row.get('ce_gamma', 0.0), 'theta': row.get('ce_theta', 0.0), 'vega': row.get('ce_vega', 0.0)}
                if row.get('pe_token'): greeks_lookup[row['pe_token']] = {'iv': row.get('pe_iv', 0.0), 'delta': row.get('pe_delta', 0.0), 'gamma': row.get('pe_gamma', 0.0), 'theta': row.get('pe_theta', 0.0), 'vega': row.get('pe_vega', 0.0)}
            position_greeks_list = []
            for pos in positions_for_greeks:
                greeks_data = greeks_lookup.get(pos['token'], {})
                signed_quantity = pos['quantity'] if pos.get('action') == 'BUY' else -pos.get('quantity')
                pg = PositionGreeks(token=pos['token'], strike=pos['strike'], option_type=pos['option_type'], quantity=signed_quantity, ltp=state.get_price(pos['token']) or 0.0, delta=greeks_data.get('delta', 0.0), gamma=greeks_data.get('gamma', 0.0), theta=greeks_data.get('theta', 0.0), vega=greeks_data.get('vega', 0.0), iv=greeks_data.get('iv', 0.0))
                position_greeks_list.append(pg)
            trade_greeks = TradeGreeks(trade_uid=trade_uid, positions=position_greeks_list)
            # --- END REFACTOR ---

            greeks_map = {p.token: p for p in trade_greeks.positions} if trade_greeks and trade_greeks.positions else {}

            # --- PNL & Live Positions Fallback ---
            ce_qty = full_trade_data.get('ce_quantity', 0)
            pe_qty = full_trade_data.get('pe_quantity', 0)
            ce_entry = full_trade_data.get('ce_entry_price', 0)
            pe_entry = full_trade_data.get('pe_entry_price', 0)
            
            # Use robust LTP fetch to avoid race condition with price updates
            # get_ltp is now async, so await it directly
            ce_ltp = await get_ltp(ce_token, correct_exchange_segment) if ce_token else 0
            pe_ltp = await get_ltp(pe_token, correct_exchange_segment) if pe_token else 0

            ce_pnl_fallback = (ce_entry - ce_ltp) * ce_qty if ce_entry > 0 and ce_ltp > 0 else 0
            pe_pnl_fallback = (pe_entry - pe_ltp) * pe_qty if pe_entry > 0 and pe_ltp > 0 else 0
            total_pnl = ce_pnl_fallback + pe_pnl_fallback
            total_unrealized_pnl = total_pnl # All PnL is unrealized in this fallback case
            total_realized_pnl = 0.0

            if ce_token:
                ce_greeks: Optional[PositionGreeks] = greeks_map.get(ce_token)
                live_positions.append({
                    'token': ce_token, 'strike': full_trade_data['strike'], 'option_type': 'CE', 'quantity': ce_qty, 'action': 'SELL',
                    'entry_price': ce_entry, 'ltp': ce_ltp, 'pnl': ce_pnl_fallback,
                    'iv': ce_greeks.iv / 100 if ce_greeks else 0.0, 'delta': ce_greeks.net_delta if ce_greeks else 0.0, 'gamma': ce_greeks.net_gamma if ce_greeks else 0.0,
                    'theta': ce_greeks.net_theta if ce_greeks else 0.0, 'vega': ce_greeks.net_vega if ce_greeks else 0.0,
                })
            if pe_token:
                pe_greeks: Optional[PositionGreeks] = greeks_map.get(pe_token)
                live_positions.append({
                    'token': pe_token, 'strike': full_trade_data['strike'], 'option_type': 'PE', 'quantity': pe_qty, 'action': 'SELL',
                    'entry_price': pe_entry, 'ltp': pe_ltp, 'pnl': pe_pnl_fallback,
                    'iv': pe_greeks.iv / 100 if pe_greeks else 0.0, 'delta': pe_greeks.net_delta if pe_greeks else 0.0, 'gamma': pe_greeks.net_gamma if pe_greeks else 0.0,
                    'theta': pe_greeks.net_theta if pe_greeks else 0.0, 'vega': pe_greeks.net_vega if pe_greeks else 0.0,
                })

        elif 'trade_greeks' not in locals(): # Ensure greeks are calculated if the main block was skipped but orders were found
            # --- REFACTOR: Calculate Greeks ON THE FLY for fallback case too ---
            chain_rows = option_chain.get('chain', [])
            iv_lookup = {}
            for row in chain_rows:
                if row.get('ce_token'): iv_lookup[int(row['ce_token'])] = row.get('ce_iv', 0.0)
                if row.get('pe_token'): iv_lookup[int(row['pe_token'])] = row.get('pe_iv', 0.0)
            
            dte = option_chain.get('dte', 0)
            risk_free_rate = 0.0

            position_greeks_list = []
            for pos in positions_for_greeks:
                token = int(pos['token'])
                cached_iv_pct = iv_lookup.get(token, 0.0)
                live_ltp = state.get_price(token) or 0.0
                
                # --- NEW: Calculate Real-Time IV & Greeks (Fallback Block) ---
                greeks = {}
                if live_ltp > 0:
                    greeks = calculate_all_greeks(pos['option_type'].lower(), pos['strike'], live_spot_price, dte, live_ltp, risk_free_rate)
                
                if greeks.get('iv', 0) > 0:
                    greeks['iv'] = greeks['iv'] * 100.0
                elif cached_iv_pct > 0:
                    greeks = calculate_greeks_from_iv(pos['option_type'].lower(), pos['strike'], live_spot_price, dte, cached_iv_pct / 100.0, risk_free_rate)
                    greeks['iv'] = cached_iv_pct
                else:
                    greeks = {'delta': 0.0, 'gamma': 0.0, 'theta': 0.0, 'vega': 0.0, 'iv': 0.0}

                signed_quantity = pos['quantity'] if pos.get('action') == 'BUY' else -pos.get('quantity')
                pg = PositionGreeks(token=token, strike=pos['strike'], option_type=pos['option_type'], quantity=signed_quantity, ltp=live_ltp, delta=greeks.get('delta', 0.0), gamma=greeks.get('gamma', 0.0), theta=greeks.get('theta', 0.0), vega=greeks.get('vega', 0.0), iv=greeks.get('iv', 0.0))
                position_greeks_list.append(pg)
            trade_greeks = TradeGreeks(trade_uid=trade_uid, positions=position_greeks_list)
            # --- END REFACTOR ---
        
        net_delta = trade_greeks.total_delta if trade_greeks else 0.0
        net_gamma = trade_greeks.total_gamma if trade_greeks else 0.0
        
        # --- FIX: Force Live ATM IV (Strictly from ATM options, not position) ---
        avg_iv = 0.0
        live_atm_iv = 0.0
        current_atm_strike = 0
        try:
            gap = option_chain.get('gap') or 50
            # Calculate current ATM strike based on live spot
            current_atm_strike = int(round(live_spot_price / gap) * gap)
            
            # Find the row for this strike in the cached chain
            chain_data = option_chain.get('chain', [])
            atm_row = next((row for row in chain_data if row.get('strike') == current_atm_strike), None)
            
            if atm_row:
                # 1. Try cached IVs from Market Data Service (Most reliable source for Live IV)
                ce_iv_chain = float(atm_row.get('ce_iv') or 0)
                pe_iv_chain = float(atm_row.get('pe_iv') or 0)
                
                valid_ivs = [x for x in [ce_iv_chain, pe_iv_chain] if x > 0]
                if valid_ivs:
                    live_atm_iv = sum(valid_ivs) / len(valid_ivs)
                
                # 2. If cached IVs missing, try calculating on-the-fly
                if live_atm_iv == 0:
                    ce_token = int(atm_row.get('ce_token') or 0)
                    pe_token = int(atm_row.get('pe_token') or 0)
                    ce_ltp = state.get_price(ce_token) or float(atm_row.get('ce_ltp') or 0)
                    pe_ltp = state.get_price(pe_token) or float(atm_row.get('pe_ltp') or 0)
                    dte = option_chain.get('dte', 0)
                    
                    calc_ivs = []
                    if ce_ltp > 0:
                        g = calculate_all_greeks("call", current_atm_strike, live_spot_price, dte, ce_ltp, 0.0)
                        iv = g.get('iv', 0)
                        if iv > 0: calc_ivs.append(iv * 100 if iv < 1.0 else iv) # Handle decimal vs %
                    if pe_ltp > 0:
                        g = calculate_all_greeks("put", current_atm_strike, live_spot_price, dte, pe_ltp, 0.0)
                        iv = g.get('iv', 0)
                        if iv > 0: calc_ivs.append(iv * 100 if iv < 1.0 else iv)
                    
                    if calc_ivs:
                        live_atm_iv = sum(calc_ivs) / len(calc_ivs)
            else:
                logger.warning(f"ATM Row not found for strike {current_atm_strike} in chain.")
                
        except Exception as e:
            logger.error(f"Error determining Live ATM IV: {e}")

        if live_atm_iv > 0:
            avg_iv = live_atm_iv
        else:
            # Fallback to position average only if absolutely necessary
            avg_iv = trade_greeks.avg_iv if trade_greeks else 0.0
            if avg_iv > 0:
                logger.warning(f"⚠️ Live ATM IV unavailable for {trade_uid} (ATM: {current_atm_strike}). Using Pos Avg: {avg_iv:.2f}%")

        # --- Extract per-leg PnL ---
        # Ensure tokens are integers for comparison, as they may come from DB as strings
        ce_token_int = int(full_trade_data.get('ce_token')) if full_trade_data.get('ce_token') else None
        pe_token_int = int(full_trade_data.get('pe_token')) if full_trade_data.get('pe_token') else None
        ce_pnl = pnl_and_position_details.get(ce_token_int, {}).get('token_pnl', 0.0)
        pe_pnl = pnl_and_position_details.get(pe_token_int, {}).get('token_pnl', 0.0)
        # --- Extract per-leg Greeks ---
        ce_iv, pe_iv, ce_delta, pe_delta = 0.0, 0.0, 0.0, 0.0

        if trade_greeks and trade_greeks.positions:
            for pos_greeks in trade_greeks.positions:
                if pos_greeks.token == ce_token_int:
                    ce_iv, ce_delta = pos_greeks.iv, pos_greeks.net_delta
                elif pos_greeks.token == pe_token_int:
                    pe_iv, pe_delta = pos_greeks.iv, pos_greeks.net_delta

        # --- 3. DYNAMIC PARAMETER CALCULATION (Hedge, Roll) ---
        pts_out = abs(net_delta) / abs(net_gamma) if abs(net_gamma) > 1e-6 else 0.0
        config = full_trade_data.get('config') or {}
        
        points_allowed = float("inf") # Default value
        roll_trigger_price = 0.0 # Default value

        if option_chain:
            try:
                hedge_div = config.get("hedge_div", 19)
                straddle_div = config.get("straddle_div", 3)
                roll_straddle_div = config.get('roll_straddle_div', 2)

                atm_strike = option_chain.get('atm')
                atm_row = next((row for row in option_chain['chain'] if row['strike'] == atm_strike), None)
                if atm_row:
                    ce_ltp = state.get_price(atm_row['ce_token']) or atm_row['ce_ltp']
                    pe_ltp = state.get_price(atm_row['pe_token']) or atm_row['pe_ltp']
                    atm_straddle_price_live = ce_ltp + pe_ltp

                    # Calculate dynamic points_allowed
                    # avg_iv is already calculated above (preferring Live ATM IV)
                    avg_iv_decimal = avg_iv / 100.0
                    straddle_based = (atm_straddle_price_live / straddle_div) if straddle_div > 0 and atm_straddle_price_live > 0 else float("inf")
                    spot_iv_based = ((live_spot_price * avg_iv_decimal) / hedge_div) if hedge_div > 0 and live_spot_price > 0 and avg_iv_decimal > 0 else float("inf")
                    points_allowed = min(straddle_based, spot_iv_based)

                    # Calculate dynamic roll_trigger_price
                    roll_trigger_price = atm_straddle_price_live / roll_straddle_div if roll_straddle_div > 0 and atm_straddle_price_live > 0 else 0.0
            except Exception as e:
                logger.warning(f"Could not calculate dynamic params for {trade_uid}: {e}")

        # --- PnL per Straddle Calculation ---
        # --- FIX: Re-fetch the trade data right before this calculation to ensure it's fresh ---
        # --- FIX: This calculation is now based on the CURRENT live position for consistency. ---
        
        # --- REFACTOR (USER FEEDBACK): Calculate PnL per straddle based on NET open quantities ---
        # A short position (SELL) is positive quantity, a long position (BUY) is negative.
        # This correctly calculates the net exposure for each option type.
        net_ce_qty = sum(p['quantity'] if p['action'] == 'SELL' else -p['quantity'] for p in live_positions if p['option_type'] == 'CE')
        net_pe_qty = sum(p['quantity'] if p['action'] == 'SELL' else -p['quantity'] for p in live_positions if p['option_type'] == 'PE')
        
        # The number of "straddle units" is the average of the absolute net quantities.
        # This provides a more intuitive denominator for PnL/straddle in complex positions.
        num_straddle_units = (abs(net_ce_qty) + abs(net_pe_qty)) / 2.0
        
        pnl_per_straddle = 0.0
        if num_straddle_units > 0:
            # --- MODIFIED: Calculate based on UNREALIZED PnL and CURRENT Qty ---
            # This reflects the performance of the currently open position.
            pnl_per_straddle = total_unrealized_pnl / num_straddle_units
        elif total_pnl != 0: # Position is fully closed
            # For closed trades, it's based on total PnL and the main position size.
            # The 'initial_...' fields are deprecated.
            ce_qty = full_trade_data.get('ce_quantity', 0)
            pe_qty = full_trade_data.get('pe_quantity', 0)
            straddle_units = (ce_qty + pe_qty) / 2.0
            if straddle_units > 0:
                pnl_per_straddle = total_pnl / straddle_units

        # --- 4. SL Threshold Calculation ---
        sl_bps = config.get('sl_bps', 14)
        sl_points_per_straddle = (live_spot_price * sl_bps) / 10000 if live_spot_price > 0 else 0.0
        
        # --- MODIFIED: Calculate SL threshold based on CURRENT open quantity ---
        # This scales the SL down as the position is partially squared off.
        current_straddles = num_straddle_units if num_straddle_units > 0 else 0.0
        sl_threshold = -1 * sl_points_per_straddle * current_straddles

        # --- 5. DTE and Lot Size ---
        days_to_expiry = calculate_dte(full_trade_data.get('expiry', '')) if full_trade_data.get('expiry') else -1

        # --- 6. Assemble and Store Snapshot ---
        snapshot_data = {
            'timestamp': get_ist_now().isoformat(), 'total_pnl': total_pnl,
            'realized_pnl': total_realized_pnl, # This now only includes PnL from PSQF
            'unrealized_pnl': total_unrealized_pnl,
            'pnl_per_straddle': pnl_per_straddle, 'net_delta': net_delta, 'net_gamma': net_gamma,
            'net_theta': trade_greeks.total_theta if trade_greeks else 0.0,
            'net_vega': trade_greeks.total_vega if trade_greeks else 0.0,
            'avg_iv': avg_iv, 'ce_pnl': ce_pnl, 'pe_pnl': pe_pnl,
            'ce_iv': ce_iv, 'pe_iv': pe_iv, 'ce_delta': ce_delta, 'pe_delta': pe_delta,
            'pts_out': pts_out, 'points_allowed': points_allowed, 'roll_trigger_price': roll_trigger_price,
            'sl_threshold': sl_threshold, 'sl_points': sl_points_per_straddle,
            'days_to_expiry': days_to_expiry, 'lot_size': lot_size,
            'spot_price': live_spot_price,
            'total_contracts': full_trade_data.get('ce_quantity', 0) + full_trade_data.get('pe_quantity', 0),
            'live_positions': live_positions # Add the detailed positions list
        }
        state.trade_snapshots[trade_uid] = snapshot_data

        # --- 7. Format Positions for Logging ---
        positions_log_str = ""
        if live_positions:
            positions_log_str += "\n   - Positions:"
            # Sort positions for consistent display: PE SELL, PE BUY, CE SELL, CE BUY
            sorted_positions = sorted(live_positions, key=lambda p: (p['option_type'], p['action']), reverse=True)
            for pos in sorted_positions:
                positions_log_str += (
                    f"\n     - {pos['action']} {pos['quantity']} {symbol_upper} {pos['strike']} {pos['option_type']}"
                    f" | Entry: {pos['entry_price']:.2f} | LTP: {pos['ltp']:.2f} | PnL: ₹{pos['pnl']:.2f}"
                )

        # Formatted log message for readability
        log_message = (
            f"📸 Snapshot for {trade_uid}:\n"
            f"   - PnL: ₹{total_pnl:.2f} (R: ₹{total_realized_pnl:.2f}, U: ₹{total_unrealized_pnl:.2f}) | PnL/Straddle: ₹{pnl_per_straddle:.2f} | Spot: ₹{live_spot_price:.2f}\n"
            f"   - Greeks (Δ|Γ|Θ|V): {snapshot_data.get('net_delta', 0):.2f} | {snapshot_data.get('net_gamma', 0):.4f} | {snapshot_data.get('net_theta', 0):.2f} | {snapshot_data.get('net_vega', 0):.2f}\n"
            f"   - Hedge (Pts Out|Allowed): {snapshot_data.get('pts_out', 0):.2f} | {snapshot_data.get('points_allowed', float('inf')):.2f}\n"
            f"   - Roll (Trigger Price): ₹{snapshot_data.get('roll_trigger_price', 0):.2f}\n"
            f"   - SL (Threshold|Points): ₹{snapshot_data.get('sl_threshold', 0):.2f} | {snapshot_data.get('sl_points', 0):.2f}\n"
            f"   - IV (Avg|CE|PE): {snapshot_data.get('avg_iv', 0):.2f}% | {snapshot_data.get('ce_iv', 0):.2f}% | {snapshot_data.get('pe_iv', 0):.2f}%\n"
            f"   - DTE: {snapshot_data.get('days_to_expiry', -1):.2f}"
            f"{positions_log_str}"
        )
        logger.info(log_message)
    except Exception as e:
        logger.exception(f"❌ Snapshot creation failed for {trade_uid}: {e}")


async def create_snapshot_for_trade(trade_uid: str, trade_data: dict = None):
    """Creates a snapshot for a single trade, on-demand."""
    # Ensure snapshot dictionary exists to prevent race conditions on startup
    if not hasattr(state, 'trade_snapshots'):
        state.trade_snapshots = {}

    loop = asyncio.get_event_loop()
    
    if not trade_data:
        # Run synchronous DB call in an executor
        trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
    else:
        trade = trade_data
        
    if not trade:
        logger.warning(f"Cannot create snapshot for non-existent trade {trade_uid}")
        return

    spot_details_map = {}
    symbol = trade.get('symbol', 'NIFTY').upper()
    
    # --- FIX: Fetch chain from cache or service, do NOT build it here ---
    # This function runs in the main app/worker process, which should not build data.
    # --- FIX: Add validation to protect against reading a corrupted chain object from shared state ---
    # This can happen due to a race condition if the market data service is updating the chain non-atomically.
    chain = state.get_option_chain(symbol)
    is_chain_valid = isinstance(chain, dict) and 'fut_ltp' in chain and 'chain' in chain and isinstance(chain.get('chain'), list)

    if not is_chain_valid:
        if chain is not None: # It existed but was invalid
            logger.warning(f"Snapshot: Invalid or incomplete chain for {symbol} found in cache. Re-fetching from service...")
        else: # It was a clean cache miss
            logger.info(f"Snapshot: Cache miss for {symbol}. Fetching from service...")
        chain = await get_option_chain_from_service(symbol)
    # --- END FIX ---

    if chain:
        spot_details_map[symbol] = chain

    await _create_snapshot_for_trade(trade, spot_details_map, trade_data_override=trade_data)

async def trigger_snapshot_and_broadcast(trade_uid: str, trade_data: dict = None):
    """
    On-demand function to create a snapshot for a trade and immediately
    broadcast it to all UI clients. Useful for instant feedback after an action.
    """
    logger.info(f"⚡ Triggering immediate snapshot and broadcast for {trade_uid}...")
    
    # 1. Create the snapshot
    await create_snapshot_for_trade(trade_uid, trade_data)

    # 2. Assemble and broadcast
    await _assemble_and_broadcast_snapshot(trade_uid)
    logger.info(f"✅ Immediate snapshot for {trade_uid} broadcasted.")


async def _assemble_and_broadcast_snapshot(trade_uid: str):
    """Helper to assemble the full snapshot payload and broadcast it."""
    from trading.trade_manager import get_trade_manager
    from trading.event_bus import get_event_bus, EventPriority

    snapshot = state.trade_snapshots.get(trade_uid)
    if not snapshot:
        logger.warning(f"Could not get snapshot for {trade_uid} to broadcast.")
        return

    loop = asyncio.get_event_loop()
    trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
    if not trade:
        logger.warning(f"Could not get trade data for {trade_uid} to broadcast.")
        return

    manager = get_trade_manager(trade_uid)
    event_bus = get_event_bus()

    # --- Assemble the full payload ---
    payload = {
        'trade_uid': trade_uid,
        'symbol': trade.get('symbol'),
        'strike': trade.get('strike'),
        'status': trade.get('status'),
        'ce_quantity': trade.get('ce_quantity', 0), # Keep for summary view
        'pe_quantity': trade.get('pe_quantity', 0), # Keep for summary view
        'live_pnl': snapshot.get('total_pnl'),
        'realized_pnl': snapshot.get('realized_pnl', 0.0),
        'unrealized_pnl': snapshot.get('unrealized_pnl', 0.0),
        'live_net_delta': snapshot.get('net_delta'),
        'net_gamma': snapshot.get('net_gamma'),
        'net_theta': snapshot.get('net_theta'),
        'net_vega': snapshot.get('net_vega'),
        'live_positions': snapshot.get('live_positions', []), # The new detailed list
        # Pass monitor-relevant data for details view
        'pts_out': snapshot.get('pts_out'),
        'points_allowed': snapshot.get('points_allowed'),
        'roll_trigger_price': snapshot.get('roll_trigger_price'),
        'entry_spot': trade.get('entry_spot'),
    }

    # Add Monitor Status if manager exists
    if manager:
        live_config = trade.get('config', {})
        payload['monitors'] = {
            'sl': { 'running': manager.sl_monitor.running, 'sl_points': manager.sl_monitor.sl_points, 'sl_bps': manager.sl_monitor.sl_bps, 'interval': manager.sl_monitor.sl_monitor_interval, 'start_time': live_config.get('sl_start_time') or "Trade Start", },
            'hedge': { 'running': manager.hedge_monitor.running, 'hedge_div': manager.hedge_monitor.hedge_div, 'straddle_div': manager.hedge_monitor.straddle_div, 'interval': manager.hedge_monitor.hedge_monitor_interval, 'start_time': live_config.get('hedge_start_time') or "Trade Start", },
            'roll': { 'running': manager.roll_monitor.running, 'roll_straddle_div': manager.roll_monitor.roll_straddle_div, 'interval': manager.roll_monitor.roll_monitor_interval, 'start_time': live_config.get('roll_start_time') or "Trade Start", },
            'square_off': { 'running': manager.square_off_monitor.running, 'exit_time': manager.square_off_monitor.exit_time_str or "Not Set" }
        }

    # Add Recent Events if event bus exists
    if event_bus:
        trade_events = event_bus.get_trade_events(trade_uid)
        payload['events'] = [
            {'timestamp': evt.timestamp.strftime('%H:%M:%S'), 'type': evt.event_type, 'priority': EventPriority(evt.priority).name}
            for evt in reversed(trade_events[-5:])  # latest 5
        ]

    # Broadcast individual straddle update
    # --- FIRE AND FORGET BROADCAST ---
    # This prevents a slow or stuck websocket client from blocking the critical snapshot loop.
    asyncio.create_task(broadcast_message({
        'type': 'straddle_full_update',
        'data': payload
    }))

async def create_trade_snapshots_loop(interval_seconds: float = 0.5):
    """
    [MULTIPROCESSING REFACTOR]
    This loop no longer creates snapshots. Instead, it reads snapshots from the
    queues of each trade process, aggregates them, and broadcasts them to the UI.
    """
    logger.info(f"📸 Snapshotter task started. Interval: {interval_seconds}s")

    if not hasattr(state, 'trade_snapshots'):
        state.trade_snapshots = {}

    while True:
        try:
            # Check for any new trade processes that have been spawned
            active_trade_uids = list(state.trade_processes.keys())

            if not active_trade_uids:
                await asyncio.sleep(2) # Sleep longer if no trades
                continue

            lightweight_updates = []

            for trade_uid in active_trade_uids:
                process_info = state.trade_processes.get(trade_uid)
                if not process_info or not process_info['process'].is_alive():
                    logger.warning(f"Process for trade {trade_uid} is dead or missing. Removing from registry.")
                    if trade_uid in state.trade_processes:
                        del state.trade_processes[trade_uid]
                    continue
                
                # Read from the queue without blocking
                snapshot_q = process_info['snapshot_q']
                while not snapshot_q.empty():
                    snapshot = snapshot_q.get()
                    state.trade_snapshots[trade_uid] = snapshot # Update main process state
            
            # Now, build the lightweight updates from the main process's state.trade_snapshots
            for trade_uid, snapshot in state.trade_snapshots.items():
                lightweight_updates.append({
                    'trade_uid': trade_uid,
                    'live_pnl': snapshot.get('total_pnl'),
                    'unrealized_pnl': snapshot.get('unrealized_pnl'),
                    'live_net_delta': snapshot.get('net_delta'),
                    'pts_out': snapshot.get('pts_out'),
                    'points_allowed': None if snapshot.get('points_allowed') == float('inf') else snapshot.get('points_allowed'),
                    'pnl_per_lot': snapshot.get('pnl_per_straddle'),
                    'position_ltps': {p['token']: p['ltp'] for p in snapshot.get('live_positions', [])},
                    'position_pnls': {p['token']: p['pnl'] for p in snapshot.get('live_positions', [])}
                })

            if lightweight_updates:
                asyncio.create_task(broadcast_message({
                    'type': 'pnl_batch_update', # A new, efficient message type for the UI
                    'data': lightweight_updates
                }))

            # Sleep at the end of the loop after processing all trades
            await asyncio.sleep(interval_seconds)

        except asyncio.CancelledError:
            logger.info("📸 Snapshotter task shutting down.")
            break
        except Exception as e:
            logger.exception(f"❌ Snapshotter loop error: {e}")
            await asyncio.sleep(60) # Wait longer on error
