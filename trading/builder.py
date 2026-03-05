"""
Position Builder - Build ATM Straddles (Delta-Neutral)
Supports: NSE F&O, BSE F&O, and other segments dynamically
Uses same UID for entire trade lifecycle
Verification runs as background task
"""
import asyncio
import multiprocessing
import functools
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
from utils.logger import logger
from models.state import state
from trading.order_batching_utils import generate_chunked_orders # NEW IMPORT
from utils.helpers import get_ist_now
from market_data import get_option_chain, SYMBOL_CONFIG
from trading.order_executor import get_order_executor
from trading.delta_neutral_utils import calculate_delta_neutral_quantities
from background.tasks import verify_orders_task, broadcast_log, create_snapshot_for_trade, trigger_snapshot_and_broadcast
from trading.data_client import get_option_chain_from_service
import config


async def build_straddle(
    symbol: str,
    lots: int,
    trade_uid: str = None,
    delta_neutral: bool = True,  # Enable delta-neutral by default
    product_type: str = "MIS",   # ✅ Product type (MIS/NRML)
    strike_range: int = 5,       # ✅ Strike range for option chain
    trade_config: Dict = None,   # ✅ Optional config for advanced settings
    target_expiry: str = None    # ✅ Optional: Force a specific expiry
) -> Optional[Dict]:
    """
    ⚡ BUILD ATM STRADDLE (DELTA-NEUTRAL)
    
    Args:
        symbol: Index symbol (NIFTY, SENSEX, BANKNIFTY, etc.)
        lots: Number of lots (baseline for delta-neutral calc)
        trade_uid: Trade UID (auto-generated if None)
        delta_neutral: If True, calculates unequal PE/CE for delta neutrality
        product_type: Product type (MIS/NRML, default: MIS)
        strike_range: Strike range for option chain (default: 5),
        trade_config: Full configuration dictionary for the trade,
        target_expiry: Specific expiry to build for (e.g., "25JUL2024"), used for rolling.
    
    Returns:
        Straddle data dict or None if failed
    """
    start_time = get_ist_now()
    
    try:
        # --- NEW LOGIC: Set default monitor start times ---
        if trade_config is None:
            trade_config = {}

        now = get_ist_now()
        # Calculate the start of the next minute
        next_minute_start = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        next_minute_str = next_minute_start.strftime('%H:%M:%S')

        # Set default entry time if not provided (for UI consistency)
        if 'entry_time' not in trade_config or not trade_config.get('entry_time'):
            trade_config['entry_time'] = next_minute_str

        # Set default start times for monitors if not provided in the config
        if 'sl_start_time' not in trade_config or not trade_config.get('sl_start_time'):
            trade_config['sl_start_time'] = next_minute_str
        if 'hedge_start_time' not in trade_config or not trade_config.get('hedge_start_time'):
            trade_config['hedge_start_time'] = next_minute_str
        if 'roll_start_time' not in trade_config or not trade_config.get('roll_start_time'):
            trade_config['roll_start_time'] = next_minute_str
        
        # Set default exit time if not provided
        if 'exit_time' not in trade_config or not trade_config.get('exit_time'):
            trade_config['exit_time'] = '15:27:00'
        # --- END NEW LOGIC ---

        # Generate trade UID if not provided
        if not trade_uid:
            now = get_ist_now()
            
            # Use entry_time for timestamp if available in config
            entry_time_str = trade_config.get('entry_time')
            if entry_time_str:
                try:
                    et = datetime.strptime(entry_time_str, "%H:%M:%S").time()
                    timestamp = datetime.combine(now.date(), et).strftime("%d%m%y%H%M%S")
                except ValueError:
                    try:
                        et = datetime.strptime(entry_time_str, "%H:%M").time()
                        timestamp = datetime.combine(now.date(), et).strftime("%d%m%y%H%M%S")
                    except ValueError:
                        timestamp = now.strftime("%d%m%y%H%M%S")
            else:
                timestamp = now.strftime("%d%m%y%H%M%S")  # ddmmyyhhmmss

            # --- Refactored: Use a dictionary for prefixes ---
            SYMBOL_PREFIXES = {
                "NIFTY": "ny",
                "SENSEX": "sx",
                "BANKNIFTY": "bn",
                "FINNIFTY": "fn",
                "MIDCPNIFTY": "mc",
            }
            symbol_upper = symbol.upper()
            # Match longer names first
            sorted_keys = sorted(SYMBOL_PREFIXES.keys(), key=len, reverse=True)
            prefix = next((SYMBOL_PREFIXES[key] for key in sorted_keys if key in symbol_upper), symbol[:2].lower())

            base_trade_uid = f"{prefix}{timestamp}"
            
            # --- NEW: Handle concurrent trade creation within the same second ---
            # Always append suffix starting from 'a' to ensure uniqueness and prevent prefix matching issues
            suffix_counter = 0
            loop = asyncio.get_event_loop()
            
            trade_uid = f"{base_trade_uid}{chr(ord('a') + suffix_counter)}"
            
            while await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid):
                suffix_counter += 1
                trade_uid = f"{base_trade_uid}{chr(ord('a') + suffix_counter)}"
            
            logger.info(f"Generated unique trade_uid: {trade_uid}")
        
        executor = get_order_executor()
        if not executor:
            logger.error("❌ OrderExecutor not initialized")
            return None
        
        logger.info("="*100)
        logger.info(f"🏗️  BUILD STRADDLE | Trade UID: {trade_uid}")
        if delta_neutral:
            logger.info("⚖️  DELTA-NEUTRAL MODE: Calculating unequal PE/CE quantities")
        logger.info("="*100)
        
        loop = asyncio.get_event_loop()
        # --- FIX: Read from cache for normal builds, only build on-demand for rolls ---
        # The previous implementation was always rebuilding the chain, causing significant lag.
        if target_expiry:
            # For rolls, we must build a specific chain on-the-fly. This is an exception.
            # We run the blocking build function in an executor to avoid freezing the application.
            logger.info(f"Building a specific chain for target expiry: {target_expiry}")
            chain_data = await loop.run_in_executor(
                None, get_option_chain, symbol, strike_range, target_expiry
            )
        else:
            # For normal builds, read directly from the state cache. This is instantaneous.
            # The cache is kept warm by the background tasks in the market data service.
            chain_data = state.get_option_chain(symbol.upper())
            
            # --- FIX: Fallback to REST API if cache is empty ---
            if not chain_data:
                logger.info(f"🏗️ Cache miss for {symbol}. Fetching from service...")
                chain_data = await get_option_chain_from_service(symbol.upper())
                if chain_data:
                    state.update_option_chain(symbol.upper(), chain_data)
            # --- END FIX ---

        if not chain_data:
            logger.error(f"❌ Option chain for {symbol.upper()} not found in cache. The background task may not have run yet or failed. Aborting build.")
            return None
        # --- END REFACTOR ---
        
        # ✅ Extract exchange segment from chain data
        exchange_segment = chain_data.get('exchange_segment', config.EXCHANGE_NSEFO)
        exchange_name = {2: "NSE", 12: "BSE", 1: "NSECM", 11: "BSECM"}.get(
            exchange_segment, f"SEG{exchange_segment}"
        )
        
        logger.info(f"📍 Exchange: {exchange_name} (Segment: {exchange_segment})")
        
        # Extract ATM data
        atm = chain_data['atm']
        atm_row = next((row for row in chain_data['chain'] if row['is_atm']), None)
        if not atm_row:
            logger.error("❌ ATM strike not found")
            return None
        
        lot_size = chain_data.get("lot_size")
        if not lot_size or lot_size <= 0:
            logger.error(f"❌ Invalid or missing lot size in option chain for {symbol}. Aborting build.")
            return None
        
        ce_token = atm_row.get('ce_token')
        pe_token = atm_row.get('pe_token')
        ce_symbol = atm_row.get('ce_symbol')
        pe_symbol = atm_row.get('pe_symbol')
        ce_ltp = atm_row.get('ce_ltp', 0.0)
        pe_ltp = atm_row.get('pe_ltp', 0.0)
        
        # Validate prices
        if ce_ltp <= 0 or pe_ltp <= 0:
            logger.error(f"❌ Invalid LTP: CE={ce_ltp}, PE={pe_ltp}")
            return None
        
        # Get Greeks for delta calculation
        ce_delta = atm_row.get('ce_delta', 0.5)  # Default 0.5 if not available
        pe_delta = atm_row.get('pe_delta', -0.5)  # Default -0.5 if not available
        
        # --- ROBUSTNESS FIX: Get symbol-specific max_order_qty ---
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol.upper()), None)
        sym_config = SYMBOL_CONFIG.get(base_symbol, {})
        # Use the symbol-specific max_order_qty, with a fallback to the global config constant.
        max_order_qty = sym_config.get('max_order_qty', config.MAX_ORDER_QTY)
        logger.info(f"Using MaxOrderQty: {max_order_qty} for {symbol}")

        if delta_neutral and ce_delta != 0 and pe_delta != 0:
            logger.info("="*100)
            logger.info("⚖️  DELTA-NEUTRAL CALCULATION")
            logger.info("="*100)
            logger.info(f"📊 CE Delta: {ce_delta:.6f}")
            logger.info(f"📊 PE Delta: {pe_delta:.6f}")
            logger.info(f"📊 Baseline lots: {lots}")
            
            # Calculate unequal quantities
            target_contracts = lots * lot_size  # Baseline per leg
            
            pe_lots, ce_lots, pe_contracts, ce_contracts, net_delta = calculate_delta_neutral_quantities(
                ce_option_delta=ce_delta,
                pe_option_delta=pe_delta,
                target_contracts=target_contracts,
                lotsize=lot_size
            )
            
            logger.info("="*100)
            logger.info("⚖️  DELTA-NEUTRAL ALLOCATION:")
            logger.info(f"   PE: {pe_contracts} contracts ({pe_lots} lots)")
            logger.info(f"   CE: {ce_contracts} contracts ({ce_lots} lots)")
            logger.info(f"   Net Delta: {net_delta:.4f}")
            logger.info("="*100)
            
        else:
            # Equal split (traditional straddle)
            pe_lots = lots
            ce_lots = lots
            pe_contracts = lots * lot_size
            ce_contracts = lots * lot_size
            net_delta = 0.0
            
            logger.info("="*100)
            logger.info("📊 EQUAL ALLOCATION:")
            logger.info(f"   PE: {pe_contracts} contracts ({pe_lots} lots)")
            logger.info(f"   CE: {ce_contracts} contracts ({ce_lots} lots)")
            logger.info("="*100)
        
        total_quantity = pe_contracts + ce_contracts
        
        logger.info(f"📊 [{exchange_name}] {symbol} ATM {atm} | Total Qty: {total_quantity}")
        logger.info(f"📊 CE: {ce_token} @ ₹{ce_ltp:.2f} x {ce_contracts}")
        logger.info(f"📊 PE: {pe_token} @ ₹{pe_ltp:.2f} x {pe_contracts}")
        
        # --- NEW BATCHING LOGIC ---
        legs_data_for_batching = []
        if ce_lots > 0:
            legs_data_for_batching.append({
                'token': ce_token, 'option_type': 'CE', 'action': 'SELL',
                'total_lots': ce_lots, 'lot_size': lot_size,
                'expected_price': ce_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
            }
            )
        if pe_lots > 0:
            legs_data_for_batching.append({
                'token': pe_token, 'option_type': 'PE', 'action': 'SELL',
                'total_lots': pe_lots, 'lot_size': lot_size,
                'expected_price': pe_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
            }
            )

        all_chunks = generate_chunked_orders(
            trade_uid_prefix=f"BUI_{trade_uid}",
            legs_data=legs_data_for_batching,
            base_lots_for_trade=lots, # Use the initial 'lots' parameter
            # Process in ~15% chunks (100/7) to reduce verification cycles and speed up execution.
            chunk_divisor=7,
            max_order_qty=max_order_qty
        )
        
        # --- Inject limit order buffer from config into each order ---
        # --- FIX: Use separate buy/sell buffers from config ---
        buy_buffer = float(trade_config.get('buy_buffer', 2.0)) if trade_config else 2.0
        sell_buffer = float(trade_config.get('sell_buffer', 2.0)) if trade_config else 2.0
        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else: # Default to sell buffer for SELL or unspecified actions
                    order['limit_order_buffer'] = sell_buffer
        # --- END INJECTION ---

        logger.info(f"Generated {len(all_chunks)} chunks for execution.")
        
        all_successful_orders = []
        all_failed_orders = []
        all_verified_fills = []
        build_aborted = False
        is_first_fill_processed = False
        total_execution_time = 0.0
        
        if not hasattr(state, 'temp_order_cache'):
            state.temp_order_cache = {}

        for chunk_idx, chunk_orders in enumerate(all_chunks):
            if not chunk_orders:
                continue
            
            # --- CANCELLATION CHECK ---
            if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                logger.warning(f"🛑 Build for {trade_uid} cancelled by user during chunk execution.")
                # Update status to PARTIAL, as the build was intentionally stopped.
                if is_first_fill_processed: # Only update if a trade record already exists
                    await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'PARTIAL')
                if trade_uid in state.cancellation_flags:
                    del state.cancellation_flags[trade_uid]
                build_aborted = True # Use the same flag to break and finalize
                break

            logger.info(f"⚡ Executing BUILD chunk {chunk_idx + 1}/{len(all_chunks)} with {len(chunk_orders)} orders.")
            
            chunk_result = await executor.execute_batch(chunk_orders, f"BUI_{trade_uid}_CHUNK{chunk_idx+1}")
            total_execution_time += chunk_result.get('execution_time', 0.0)
            
            successful_in_chunk = chunk_result.get('successful_orders', [])
            failed_in_chunk = chunk_result.get('failed_orders', [])
            
            all_successful_orders.extend(successful_in_chunk)
            all_failed_orders.extend(failed_in_chunk)
            
            # Extract order IDs for verification for this chunk
            chunk_order_ids = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_chunk if o.get('order_id') or o.get('app_order_id')]

            app_order_id_to_uid_map = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_chunk}

            # --- PERSISTENT VERIFICATION LOOP FOR THE CHUNK ---
            verified_fills_for_chunk = []
            unverified_order_ids = list(chunk_order_ids)
            max_verification_attempts = 3 # Reduced retries

            orders_to_reexecute_in_next_chunk = []

            for attempt in range(max_verification_attempts):
                if not unverified_order_ids:
                    break # Success, all orders in chunk are accounted for.

                logger.info(f"📊 Verifying BUILD chunk {chunk_idx + 1}, attempt {attempt + 1}/{max_verification_attempts} for {len(unverified_order_ids)} orders...")
                verification_result = await verify_orders_task(unverified_order_ids, f"BUI_{trade_uid}_CHUNK{chunk_idx+1}_ATTEMPT{attempt+1}")
                
                if verification_result:
                    newly_verified = verification_result.get('verified_success', [])
                    newly_failed = verification_result.get('verified_failed', [])
                    verified_fills_for_chunk.extend(newly_verified)
                    
                    verified_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_verified}
                    
                    # FIX: Retry verification for non-terminal failures (like 'Order not found' initially)
                    if attempt < max_verification_attempts - 1:
                        terminal_statuses = {'REJECTED', 'CANCELLED', 'CANCELED', 'REEXECUTE_NEEDED', 'NOT_FOUND_ON_RETRY'}
                        failed_ids = {
                            str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) 
                            for o in newly_failed 
                            if str(o.get('status')).upper() in terminal_statuses
                        }
                    else:
                        failed_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_failed}
                    
                    resolved_ids = verified_ids.union(failed_ids)
                    
                    unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]
                
                if unverified_order_ids:
                    logger.warning(f"⚠️ {len(unverified_order_ids)} orders still pending in BUILD chunk {chunk_idx + 1}. Retrying in 1.0s...")
                    await asyncio.sleep(1.0)

            # Collect orders that need re-execution after all verification attempts for this chunk
            ids_to_remove_from_successful = set()
            if 'newly_failed' in locals():
                for failed_order_info in newly_failed: # newly_failed from the last verification attempt
                    if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                        order_id = str(failed_order_info.get('order_id'))
                        ids_to_remove_from_successful.add(order_id)
                        original_order_uid = app_order_id_to_uid_map.get(order_id)
                        if original_order_uid:
                            original_order_data = next((o for o in chunk_orders if o['uid'] == original_order_uid), None)
                            if original_order_data:
                                # --- FIX: Clear the stale limit price to force recalculation ---
                                order_for_re_execution = original_order_data.copy()
                                order_for_re_execution['limit_price'] = 0.0
                                orders_to_reexecute_in_next_chunk.append(order_for_re_execution)
                                # --- END FIX ---
                                logger.info(f"🔄 Order {original_order_uid} marked for re-execution in next chunk.")

            # Remove orders that were cancelled for re-execution from the list of successfully placed orders
            # to prevent them from being counted as "unfilled" at the end.
            if ids_to_remove_from_successful:
                all_successful_orders = [
                    o for o in all_successful_orders
                    if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful
                ]
                logger.info(f"Corrected success tracking: Removed {len(ids_to_remove_from_successful)} orders that were cancelled for re-execution.")

            all_verified_fills.extend(verified_fills_for_chunk)

            # --- REFACTOR: Populate cache and insert to DB BEFORE snapshot ---
            fills_to_process_for_chunk = verified_fills_for_chunk
            if fills_to_process_for_chunk:
                # 1. Populate cache
                state.temp_order_cache.setdefault(trade_uid, []).extend(fills_to_process_for_chunk)
                logger.info(f"Cached {len(fills_to_process_for_chunk)} build orders under key '{trade_uid}' from chunk {chunk_idx + 1}.")
            
                # 2. Insert to DB
                if hasattr(state.db, 'insert_order'):
                    logger.info(f"Inserting {len(fills_to_process_for_chunk)} orders into DB for {trade_uid} from chunk {chunk_idx + 1}...")
                    app_order_id_to_uid_map = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_chunk}
                    orders_inserted_count = 0
                    for fill_data in fills_to_process_for_chunk:
                        app_order_id = str(fill_data.get('AppOrderID') or fill_data.get('app_order_id') or fill_data.get('apporderid'))
                        # Always prefer the full local UID over the potentially truncated broker UID
                        if app_order_id in app_order_id_to_uid_map:
                            fill_data['OrderUniqueIdentifier'] = app_order_id_to_uid_map[app_order_id]
                        
                        # --- FIX: Map OrderUniqueIdentifier to order_unique_id for DB ---
                        if fill_data.get('OrderUniqueIdentifier'):
                            if 'order_unique_id' not in fill_data:
                                fill_data['order_unique_id'] = fill_data.get('OrderUniqueIdentifier')
                            
                            state.db.insert_order(fill_data)
                            orders_inserted_count += 1
                    if orders_inserted_count > 0:
                        logger.info(f"✅ Inserted {orders_inserted_count} orders into DB from chunk {chunk_idx + 1}.")
            # --- END REFACTOR ---

            # --- LIVE SL CHECK ON PARTIALLY BUILT POSITION ---
            if all_verified_fills:
                # 1. Create/Update the trade record in the DB to make it "live"
                ce_total_value, ce_filled_qty, pe_total_value, pe_filled_qty = 0.0, 0, 0.0, 0
                for fill in all_verified_fills:
                    avg_price = float(fill.get('OrderAverageTradedPrice') or fill.get('fill_price') or 0.0)
                    qty = int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or 0)
                    # --- ROBUSTNESS FIX: Prevent crash if token is missing ---
                    token_val = fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id')
                    if not token_val:
                        logger.error(f"Build Error: Fill data missing instrument token during live check: {fill}")
                        continue
                    token = int(token_val)
                    # --- END FIX ---
                    if token == ce_token:
                        ce_total_value += avg_price * qty
                        ce_filled_qty += qty
                    elif token == pe_token:
                        pe_total_value += avg_price * qty
                        pe_filled_qty += qty
                
                avg_ce_fill = (ce_total_value / ce_filled_qty) if ce_filled_qty > 0 else ce_ltp
                avg_pe_fill = (pe_total_value / pe_filled_qty) if pe_filled_qty > 0 else pe_ltp

                current_straddle_data = {
                    'straddle_id': trade_uid, 'trade_uid': trade_uid, 'symbol': symbol, 'strike': atm,
                    'expiry': chain_data['expiry'], 'expiry_date': chain_data.get('expiry_date'),
                    'exchange_segment': exchange_segment, 'exchange_name': exchange_name, 'product_type': product_type,
                    'lot_size': lot_size, 'lots': lots, 'initial_pe_quantity': pe_contracts, 'initial_ce_quantity': ce_contracts,
                    'pe_lots': pe_filled_qty // lot_size, 'ce_lots': ce_filled_qty // lot_size,
                    'pe_quantity': pe_filled_qty, 'ce_quantity': ce_filled_qty,
                    'total_quantity': ce_filled_qty + pe_filled_qty,
                    'ce_token': ce_token, 'ce_symbol': ce_symbol, 'ce_entry_price': avg_ce_fill,
                    'pe_token': pe_token, 'pe_symbol': pe_symbol, 'pe_entry_price': avg_pe_fill,
                    'status': 'BUILDING', 'config': trade_config or {}, 'entry_spot': chain_data['fut_ltp'],
                    'ce_delta': ce_delta, 'pe_delta': pe_delta, 'net_delta': net_delta, 'delta_neutral': delta_neutral,
                }
                await loop.run_in_executor(None, state.db.insert_straddle, current_straddle_data)
                if not is_first_fill_processed:
                    logger.info(f"✅ First chunk verified for {trade_uid}. Trade is now live with status 'BUILDING'.")
                    is_first_fill_processed = True
                
                # --- REFACTOR: Create snapshot for checks, but don't wait for broadcast ---
                # This ensures the data is ready for SL/Hedge checks without the delay of UI broadcasting.
                # Pass current_straddle_data to avoid DB race condition
                await create_snapshot_for_trade(trade_uid, trade_data=current_straddle_data)

                # --- LIVE HEDGE CHECK (RUNS BEFORE SL CHECK) ---
                try:
                    logger.info(f"🛡️  Performing in-build HEDGE check for {trade_uid}...")
                    # The snapshot was just created by trigger_snapshot_and_broadcast
                    snapshot = state.trade_snapshots.get(trade_uid)
                    
                    if snapshot:
                        pts_out = snapshot.get('pts_out', 0.0)
                        points_allowed = snapshot.get('points_allowed', float('inf'))
                        net_delta = snapshot.get('net_delta', 0.0)
                        
                        # Check if hedge is needed
                        if pts_out > points_allowed:
                            log_msg = f"🛡️ HEDGE NEEDED DURING BUILD for {trade_uid}! Pts Out: {pts_out:.2f} > Allowed: {points_allowed:.2f}. Triggering hedge."
                            logger.warning(log_msg)
                            await broadcast_log('WARNING', log_msg)
                            
                            from trading.hedger import execute_synthetic_hedge
                            hedge_result = await execute_synthetic_hedge(
                                trade_uid=trade_uid,
                                net_delta=net_delta,
                                target_delta_reduction=-net_delta, # Neutralize full delta
                                hedge_type="BUI_HEDGE",
                                uid_prefix_override=f"BUI_{trade_uid}" # Ensure in-build hedge orders are tagged as part of the build
                            )
                            
                            if hedge_result and hedge_result.get('success'):
                                logger.info(f"✅ In-build hedge for {trade_uid} completed successfully.")
                                # After hedging, we need a fresh snapshot before the SL check
                                await create_snapshot_for_trade(trade_uid, trade_data=current_straddle_data)
                            else:
                                logger.error(f"❌ In-build hedge for {trade_uid} FAILED. Continuing build with unhedged position.")
                        else:
                            logger.info(f"✅ In-build Hedge Check OK for {trade_uid}. Pts Out: {pts_out:.2f} <= Allowed: {points_allowed:.2f}")
                except Exception as hedge_check_e:
                    logger.error(f"❌ Error during HEDGE check in build process for {trade_uid}: {hedge_check_e}")

                # --- LIVE SL CHECK (RUNS AFTER HEDGE CHECK) ---
                try:
                    logger.info(f"🛡️  Performing in-build SL check for {trade_uid}...")
                    # Get the latest snapshot again, as it might have been updated by the hedge
                    snapshot = state.trade_snapshots.get(trade_uid)
                    if not snapshot:
                        logger.warning(f"Could not get snapshot for SL check on {trade_uid}. Skipping SL check for this chunk.")
                    else:
                        # --- FIX: Use Total PnL vs Total Threshold based on Gross Built Lots ---
                        # This avoids issues where hedging reduces net lots (denominator) causing PnL/Lot to spike.
                        total_pnl = snapshot.get('total_pnl', 0.0)
                        sl_value_per_lot = snapshot.get('sl_points', 0.0)
                        
                        # Calculate gross lots built so far (average of CE and PE filled quantities)
                        # We use the local variables ce_filled_qty/pe_filled_qty which track the build progress
                        current_gross_lots = (ce_filled_qty + pe_filled_qty) / (2.0 * lot_size) if lot_size > 0 else 0
                        
                        # Threshold is negative (Loss Limit)
                        total_sl_threshold = -1 * sl_value_per_lot * current_gross_lots
                        
                        if total_sl_threshold < 0 and total_pnl <= total_sl_threshold:
                            log_msg = f"🚨 STOP-LOSS HIT DURING BUILD for {trade_uid}! Total PnL: ₹{total_pnl:.2f}, Threshold: ₹{total_sl_threshold:.2f} (Gross Lots: {current_gross_lots:.2f}). Aborting build and squaring off."
                            logger.critical(log_msg)
                            await broadcast_log('CRITICAL', log_msg)
                            
                            # --- NEW: INITIATE SQUARE-OFF ---
                            from trading.square_off import square_off # Local import to avoid circular dependency
                            logger.info(f"Initiating immediate square-off for partially built trade {trade_uid}...")
                            sqf_result = await square_off(trade_uid=trade_uid, straddle_data=current_straddle_data)
                            if sqf_result and sqf_result.get('success'):
                                logger.info(f"✅ Square-off for {trade_uid} completed successfully after in-build SL hit.")
                            else:
                                logger.error(f"❌ Square-off for {trade_uid} FAILED after in-build SL hit. Position may be open.")
                            # --- END NEW ---
                            
                            build_aborted = True
                            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'CLOSED_SL_BUILD')
                            break # Exit the chunk execution loop
                        else:
                            logger.info(f"✅ In-build SL Check OK for {trade_uid}. PnL/Unit: ₹{total_pnl/current_gross_lots if current_gross_lots > 0 else 0:.2f} > Threshold/Unit: ₹{-sl_value_per_lot:.2f}")

                except Exception as sl_check_e:
                    logger.error(f"❌ Error during SL check in build process for {trade_uid}: {sl_check_e}")

            # If there are orders to re-execute, prepend them to the next chunk
            if orders_to_reexecute_in_next_chunk:
                if chunk_idx + 1 < len(all_chunks):
                    all_chunks[chunk_idx + 1] = orders_to_reexecute_in_next_chunk + all_chunks[chunk_idx + 1]
                    logger.info(f"Prepended {len(orders_to_reexecute_in_next_chunk)} orders to next chunk {chunk_idx + 2}.")
                else:
                    # If this is the last chunk, create a new chunk for re-execution
                    all_chunks.append(orders_to_reexecute_in_next_chunk)
                    logger.info(f"Created new chunk for {len(orders_to_reexecute_in_next_chunk)} orders for re-execution.")

            if unverified_order_ids: # Handle unverified orders after all retries
                logger.error(f"❌ FAILED to verify all orders in BUILD chunk {chunk_idx + 1} after {max_verification_attempts} attempts. Stopping further build chunks.")
                # Add these to all_failed_orders so the final status becomes PARTIAL
                for oid in unverified_order_ids:
                    all_failed_orders.append({'uid': 'unknown', 'app_order_id': oid, 'error': 'Verification timed out'})

                build_aborted = True # Set flag to stop processing more chunks, but proceed to finalize with partial data
                break # Exit the chunk loop and proceed to finalization

            # Small delay between chunks
            if chunk_idx < len(all_chunks) - 1:
                await asyncio.sleep(0.05) # Tiny delay

        # --- AFTER THE LOOP ---
        if build_aborted:
            # The build was stopped by SL or cancellation. The status is already set.
            # Check the reason for the abort.
            final_straddle_data_check = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            current_status = final_straddle_data_check.get('status') if final_straddle_data_check else 'UNKNOWN'

            # If aborted due to SL, the trade is already closed. Return and do not start monitors.
            if current_status == 'CLOSED_SL_BUILD':
                logger.warning(f"Build for {trade_uid} was aborted due to SL hit. Final status: {current_status}")
                return final_straddle_data_check
            
            # If aborted by user or verification failure, we proceed to finalize as PARTIAL.
            logger.warning(f"Build for {trade_uid} was aborted mid-way. Proceeding to finalize with partial data.")

        # If not a single order was successfully placed, it's a complete failure.
        if not all_successful_orders:
            logger.error(f"❌ Build failed for {trade_uid}. No orders were successful.")
            return None

        # --- NEW: Pre-Sweep Verification ---
        # Re-verify any placed orders that aren't confirmed fills yet to avoid unnecessary sweeping.
        placed_ids = {str(o.get('app_order_id') or o.get('order_id')) for o in all_successful_orders}
        filled_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in all_verified_fills}
        ids_to_check = list(placed_ids - filled_ids)
        
        if ids_to_check:
            logger.info(f"🔍 Pre-Sweep Check: Re-verifying {len(ids_to_check)} pending orders before calculating unfilled quantity...")
            pre_sweep_result = await verify_orders_task(ids_to_check, f"BUI_{trade_uid}_PRE_SWEEP")
            
            new_fills = pre_sweep_result.get('verified_success', [])
            if new_fills:
                logger.info(f"✅ Pre-Sweep Check: Found {len(new_fills)} new fills. Updating state.")
                all_verified_fills.extend(new_fills)
                state.temp_order_cache.setdefault(trade_uid, []).extend(new_fills)
                
                # Rebuild map for DB insert
                app_order_id_to_uid_map = {str(o.get('app_order_id') or o.get('order_id')): o.get('uid') for o in all_successful_orders}
                
                if hasattr(state.db, 'insert_order'):
                    for fill in new_fills:
                        app_oid = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid'))
                        # Always prefer the full local UID
                        if app_oid in app_order_id_to_uid_map:
                             fill['OrderUniqueIdentifier'] = app_order_id_to_uid_map[app_oid]
                        
                        # --- FIX: Map OrderUniqueIdentifier to order_unique_id for DB ---
                        if fill.get('OrderUniqueIdentifier'):
                            if 'order_unique_id' not in fill:
                                fill['order_unique_id'] = fill.get('OrderUniqueIdentifier')
                            state.db.insert_order(fill)

        # If no orders were filled after verification, it's a failure.
        if not all_verified_fills:
            logger.error(f"❌ Build failed for {trade_uid}. All placed orders failed verification (no fills).")
            
            # --- FIX: If orders were placed but verification failed, mark as PARTIAL to prevent FAILED_FILTER ---
            if all_successful_orders:
                logger.warning(f"⚠️ Orders were placed for {trade_uid} but verification failed. Setting status to PARTIAL.")
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'PARTIAL')

            if trade_uid in state.temp_order_cache:
                del state.temp_order_cache[trade_uid]
            return None

        # Determine final status based on fills vs placed orders
        # --- REFACTOR: Use Quantity-based status check ---
        total_ce_filled = sum(int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0) for f in all_verified_fills if f.get('option_type') == 'CE')
        total_pe_filled = sum(int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0) for f in all_verified_fills if f.get('option_type') == 'PE')
        
        unfilled_ce = max(0, ce_contracts - total_ce_filled)
        unfilled_pe = max(0, pe_contracts - total_pe_filled)
        total_unfilled_qty = unfilled_ce + unfilled_pe

        # --- NEW: Final Sweep for Unfilled Quantities ---
        # Allow up to 3 sweep attempts to fill remaining quantity
        max_sweep_attempts = 3
        sweep_attempt = 0
        
        while total_unfilled_qty > 0 and not build_aborted and sweep_attempt < max_sweep_attempts:
            sweep_attempt += 1
            logger.info(f"🧹 Final Sweep (Attempt {sweep_attempt}/{max_sweep_attempts}): Detected unfilled quantity (CE: {unfilled_ce}, PE: {unfilled_pe}). Attempting to fill...")
            
            sweep_legs = []
            if unfilled_ce > 0:
                sweep_legs.append({
                    'token': ce_token, 'option_type': 'CE', 'action': 'SELL',
                    'total_lots': int(unfilled_ce / lot_size), 'lot_size': lot_size,
                    'expected_price': ce_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
                })
            if unfilled_pe > 0:
                sweep_legs.append({
                    'token': pe_token, 'option_type': 'PE', 'action': 'SELL',
                    'total_lots': int(unfilled_pe / lot_size), 'lot_size': lot_size,
                    'expected_price': pe_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
                })
            
            if sweep_legs:
                # Generate chunks for the sweep (likely just 1 chunk)
                sweep_chunks = generate_chunked_orders(f"BUI_{trade_uid}_SWEEP{sweep_attempt}", sweep_legs, lots, chunk_divisor=1, max_order_qty=max_order_qty)
                
                for chunk in sweep_chunks:
                    # Boost buffer for sweep
                    for order in chunk:
                        order['limit_order_buffer'] = sell_buffer + 3.0 + (sweep_attempt * 1.0) # More aggressive each attempt
                        order['limit_price'] = 0.0 # Force recalc
                    
                    logger.info(f"🔄 Executing Sweep Chunk with {len(chunk)} orders...")
                    sweep_result = await executor.execute_batch(chunk, f"BUI_{trade_uid}_SWEEP{sweep_attempt}")
                    
                    # Create map for this sweep chunk to restore UIDs
                    sweep_app_order_id_to_uid_map = {str(o.get('app_order_id')): o.get('uid') for o in sweep_result.get('successful_orders', [])}
                    
                    # Verify sweep orders
                    sweep_success_ids = [str(o.get('app_order_id')) for o in sweep_result.get('successful_orders', [])]
                    if sweep_success_ids:
                        sweep_verify = await verify_orders_task(sweep_success_ids, f"BUI_{trade_uid}_SWEEP{sweep_attempt}_VERIFY")
                        sweep_verified_fills = sweep_verify.get('verified_success', [])
                        
                        # Restore full UIDs for sweep orders
                        for fill in sweep_verified_fills:
                            app_oid = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid'))
                            if app_oid in sweep_app_order_id_to_uid_map:
                                fill['OrderUniqueIdentifier'] = sweep_app_order_id_to_uid_map[app_oid]
                                fill['order_unique_id'] = fill['OrderUniqueIdentifier']

                        all_verified_fills.extend(sweep_verified_fills)
                        
                        # Cache sweep orders so they are picked up by the final calculation
                        if sweep_verified_fills:
                             state.temp_order_cache.setdefault(trade_uid, []).extend(sweep_verified_fills)
            
            # Wait briefly before next sweep check
            await asyncio.sleep(1.0)

            # Recalculate after sweep attempt
            total_ce_filled = sum(int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0) for f in all_verified_fills if f.get('option_type') == 'CE')
            total_pe_filled = sum(int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0) for f in all_verified_fills if f.get('option_type') == 'PE')
            
            unfilled_ce = max(0, ce_contracts - total_ce_filled)
            unfilled_pe = max(0, pe_contracts - total_pe_filled)
            total_unfilled_qty = unfilled_ce + unfilled_pe
            
        unfilled_count = total_unfilled_qty
        # --- END REFACTOR ---

        if all_failed_orders or unfilled_count > 0:
            logger.warning(f"⚠️ Partial build for {trade_uid}: {len(all_verified_fills)} fills, {unfilled_count} qty unfilled, {len(all_failed_orders)} failed to place.")
            for f_order in all_failed_orders:
                logger.warning(f"  - Failed UID: {f_order.get('uid')}, Reason: {f_order.get('error')}")
            final_status = 'PARTIAL' # Set status to PARTIAL if there were any failures or unfilled orders
        else:
            logger.info(f"✅ All {len(all_successful_orders)} placed build orders were successfully filled for {trade_uid}.")
            final_status = 'ACTIVE'

        # Update status from BUILDING to the final status
        if is_first_fill_processed:
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, final_status)

        # Use the locally accumulated verified fills for the final calculation.
        # This is more robust than the global cache which might be cleared by concurrent processes (e.g. in-build hedge).
        all_fills_to_process = all_verified_fills
        if not all_fills_to_process and all_successful_orders:
            logger.warning(f"⚠️ No verified fills found in cache for {trade_uid}. Final calculations will be based on zero filled quantity.")
            all_fills_to_process = []

        ce_total_value = 0.0
        ce_filled_qty = 0
        pe_total_value = 0.0
        pe_filled_qty = 0

        # Recalculate filled quantities and average prices from all_fills_to_process
        for fill in all_fills_to_process:
            avg_price = float(fill.get('OrderAverageTradedPrice') or fill.get('fill_price') or fill.get('expected_price') or 0.0)
            qty = int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or fill.get('quantity') or 0)
            # --- ROBUSTNESS FIX: Prevent crash if token is missing ---
            token_val = fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id')
            if not token_val:
                logger.error(f"Final Calc Error: Fill data missing instrument token: {fill}")
                continue
            token = int(token_val)
            # --- END FIX ---

            if token == ce_token and avg_price > 0 and qty > 0:
                ce_total_value += avg_price * qty
                ce_filled_qty += qty
            elif token == pe_token and avg_price > 0 and qty > 0:
                pe_total_value += avg_price * qty
                pe_filled_qty += qty

        # Calculate the final weighted average prices. Fallback to initial LTP if verification fails.
        avg_ce_fill = (ce_total_value / ce_filled_qty) if ce_filled_qty > 0 else ce_ltp
        avg_pe_fill = (pe_total_value / pe_filled_qty) if pe_filled_qty > 0 else pe_ltp
        
        # ✅ Calculate ACTUAL executed quantities and lots from successful orders
        # Use the verified filled quantities for accuracy.
        executed_ce_qty = ce_filled_qty # Now correctly reflects total filled quantity from all chunks
        executed_pe_qty = pe_filled_qty # Now correctly reflects total filled quantity from all chunks
        executed_total_qty = executed_ce_qty + executed_pe_qty
        executed_ce_lots = executed_ce_qty // lot_size if lot_size > 0 else 0
        executed_pe_lots = executed_pe_qty // lot_size if lot_size > 0 else 0
        
        # Build straddle data
        straddle_data = {
            'straddle_id': trade_uid,
            'trade_uid': trade_uid,  # SAME UID for all operations
            'symbol': symbol,
            'strike': atm,
            'expiry': chain_data['expiry'],
            'expiry_date': chain_data.get('expiry_date'),  # ✅ Store expiry date
            'exchange_segment': exchange_segment,      # ✅ Store exchange segment
            'exchange_name': exchange_name,            # ✅ Store exchange name
            'product_type': product_type,              # ✅ Store product type
            'lot_size': lot_size,                      # ✅ Store lot size
            'lots': lots,                              # Store initial target lots
            'initial_pe_quantity': executed_pe_qty,    # ✅ Store initial executed quantity
            'initial_ce_quantity': executed_ce_qty,    # ✅ Store initial executed quantity
            'pe_lots': executed_pe_lots,
            'ce_lots': executed_ce_lots,
            'pe_quantity': executed_pe_qty,
            'ce_quantity': executed_ce_qty,
            'total_quantity': executed_total_qty,
            'ce_token': ce_token,
            'ce_symbol': ce_symbol,
            'ce_entry_price': avg_ce_fill,
            'ce_delta': ce_delta,
            'ce_gamma': atm_row.get('ce_gamma', 0),
            'ce_theta': atm_row.get('ce_theta', 0),
            'ce_vega': atm_row.get('ce_vega', 0),      # ✅ Store vega
            'ce_iv': atm_row.get('ce_iv', 0),          # ✅ Store IV
            'pe_token': pe_token,
            'pe_symbol': pe_symbol,
            'pe_entry_price': avg_pe_fill,
            'pe_delta': pe_delta,
            'pe_gamma': atm_row.get('pe_gamma', 0),
            'pe_theta': atm_row.get('pe_theta', 0),
            'pe_vega': atm_row.get('pe_vega', 0),      # ✅ Store vega
            'pe_iv': atm_row.get('pe_iv', 0),          # ✅ Store IV
            'net_delta': net_delta, 'config': trade_config or {},                    # ✅ Store the config
            'delta_neutral': delta_neutral,
            'total_premium': (avg_ce_fill * executed_ce_qty) + (avg_pe_fill * executed_pe_qty),
            'status': final_status,
            'execution_time': total_execution_time,
            'entry_spot': chain_data['fut_ltp'],
            'spot_price': chain_data['fut_ltp'],
            'fut_token': chain_data.get('fut_token'),
            'entry_timestamp': get_ist_now().isoformat(),
            'ce_orders': [o for o in all_fills_to_process if o.get('ExchangeInstrumentID') == ce_token], # Use verified fills
            'pe_orders': [o for o in all_fills_to_process if o.get('ExchangeInstrumentID') == pe_token] # Use verified fills
        }
        
        # Extract order IDs from successful orders
        ce_order_ids = [str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')) for o in straddle_data['ce_orders']]
        pe_order_ids = [str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')) for o in straddle_data['pe_orders']]
        
        straddle_data['ce_order_id'] = ','.join(ce_order_ids) if ce_order_ids else ''
        straddle_data['ce_app_order_id'] = ','.join(ce_order_ids) if ce_order_ids else ''
        straddle_data['pe_order_id'] = ','.join(pe_order_ids) if pe_order_ids else ''
        straddle_data['pe_app_order_id'] = ','.join(pe_order_ids) if pe_order_ids else ''
        
        # Map all order IDs to trade
        for order in all_fills_to_process: # Map all processed fills
            order_id = order.get('app_order_id') or order.get('order_id')
            if order_id:
                state.map_order_to_trade(str(order_id), trade_uid)
        
        # Save to DB (run in thread pool to avoid blocking)
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
            logger.info(f"💾 Straddle saved: {trade_uid}")
        except Exception as e:
            logger.error(f"❌ Failed to save straddle to DB: {e}")
        
        # ✅ Subscribe to instruments (fire-and-forget, non-blocking)
        # --- FIX: Clear the temporary cache for this trade after the build is complete ---
        if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
            del state.temp_order_cache[trade_uid]
            logger.info(f"🧹 Cleared temp order cache for {trade_uid} after build.")
        # --- END FIX ---
        # This is now handled automatically and more robustly within get_option_chain,
        # which subscribes to all instruments in the generated chain.
        logger.info("📡 Instrument subscription handled by option chain builder.")
        # Calculate total time
        end_time = get_ist_now()
        total_time = (end_time - start_time).total_seconds()
        
        logger.info("="*100)
        logger.info(f"✅ [{exchange_name}] BUILD_{trade_uid} COMPLETE | Status: {final_status} | Time: {total_time:.2f}s")
        logger.info(f"⚖️  Executed: PE={executed_pe_qty}, CE={executed_ce_qty} | Target NetΔ={net_delta:.4f}")
        logger.info(f"💰 Total Premium: ₹{straddle_data['total_premium']:,.2f}")
        logger.info(f"   CE: {executed_ce_qty} @ ₹{avg_ce_fill:.2f}")
        logger.info(f"   PE: {executed_pe_qty} @ ₹{avg_pe_fill:.2f}")
        logger.info("="*100)
        
        # --- NEW: Spawn a dedicated process for this trade ---
        if final_status in ['ACTIVE', 'PARTIAL']:
            logger.info(f"🚀 Spawning dedicated process for trade {trade_uid}...")
            command_q = multiprocessing.Queue()
            snapshot_q = multiprocessing.Queue()
            
            from trading.trade_process import trade_process_worker_entry
            
            process = multiprocessing.Process(
                target=trade_process_worker_entry,
                args=(trade_uid, trade_config or {}, command_q, snapshot_q, state.option_chains, all_verified_fills)
            )
            process.start()
            
            state.trade_processes[trade_uid] = {
                'process': process,
                'command_q': command_q,
                'snapshot_q': snapshot_q
            }
            logger.info(f"✅ Process for {trade_uid} started and registered.")

        # Return a dictionary indicating success status and the straddle data
        return {
            "success": final_status == 'ACTIVE', # Only fully successful builds are 'ACTIVE'
            "straddle_data": straddle_data,
            "message": f"Straddle build completed with status: {final_status}"
        }
        
    except Exception as e:
        logger.error(f"❌ Build failed: {e}", exc_info=True)
        # If an unexpected error occurs after the trade has been created,
        # try to revert its status to ACTIVE to allow monitoring of any partial position.
        if 'trade_uid' in locals() and trade_uid:
            try:
                loop = asyncio.get_event_loop()
                trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                if trade and trade.get('status') == 'BUILDING':
                    await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                    logger.warning(f"⚠️ Build failed with exception. Reverted status to ACTIVE for {trade_uid} as it may be partially built.")
            except Exception as db_e:
                logger.error(f"CRITICAL: Failed to revert status for {trade_uid} after build exception. DB Error: {db_e}")
    finally: # This finally block must be at the same indentation level as the try and except blocks.
        # --- CRITICAL FIX: Ensure the temporary cache is always cleared for this trade ---
        if 'trade_uid' in locals() and trade_uid:
            if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
                del state.temp_order_cache[trade_uid]
                logger.info(f"🧹 Final cleanup: Cleared temp order cache for {trade_uid} after build attempt.")
        # --- END FIX ---


async def build_multi_straddle(
    symbol: str,
    lots: int,
    count: int = 1,
    delta_neutral: bool = True,
    product_type: str = "MIS",
    strike_range: int = 5,
    delay_seconds: float = 1.0,
    trade_config: Dict = None
) -> List[Dict]:
    """
    Build multiple straddles in sequence
    
    Args:
        symbol: Index/stock symbol
        lots: Lots per straddle
        count: Number of straddles to build
        delta_neutral: Enable delta-neutral mode
        product_type: MIS/NRML
        strike_range: Strike range for option chain
        delay_seconds: Delay between straddles (to avoid rate limits),
        trade_config: The configuration to apply to all straddles
    
    Returns:
        List of straddle data dicts
    """
    straddles = []
    
    logger.info("="*100)
    logger.info(f"🏗️  BUILDING {count} STRADDLES")
    logger.info("="*100)
    
    for i in range(count):
        logger.info(f"🏗️  Building straddle {i+1}/{count}")
        
        straddle = await build_straddle(
            symbol=symbol,
            lots=lots,
            delta_neutral=delta_neutral,
            product_type=product_type,
            strike_range=strike_range,
            trade_config=trade_config
        )
        
        if straddle:
            straddles.append(straddle)
            logger.info(f"✅ Straddle {i+1}/{count} built: {straddle['trade_uid']}")
        else:
            logger.error(f"❌ Straddle {i+1}/{count} failed")
        
        # Delay before next straddle
        if i < count - 1 and delay_seconds > 0:
            logger.info(f"⏳ Waiting {delay_seconds}s before next straddle...")
            await asyncio.sleep(delay_seconds)
    
    logger.info("="*100)
    logger.info(f"✅ Built {len(straddles)}/{count} straddles successfully")
    logger.info("="*100)
    
    return straddles


def validate_straddle_data(straddle_data: Dict) -> bool:
    """
    Validate straddle data before saving
    
    Args:
        straddle_data: Straddle data dict
    
    Returns:
        True if valid, False otherwise
    """
    try:
        required_fields = [
            'straddle_id', 'trade_uid', 'symbol', 'strike', 'expiry',
            'ce_token', 'pe_token', 'ce_quantity', 'pe_quantity'
        ]
        
        for field in required_fields:
            if field not in straddle_data or straddle_data[field] is None:
                logger.error(f"❌ Missing required field: {field}")
                return False
        
        # Validate quantities
        if straddle_data['ce_quantity'] <= 0 or straddle_data['pe_quantity'] <= 0:
            logger.error(f"❌ Invalid quantities: CE={straddle_data['ce_quantity']}, PE={straddle_data['pe_quantity']}")
            return False
        
        # Validate prices
        if straddle_data.get('ce_entry_price', 0) <= 0 or straddle_data.get('pe_entry_price', 0) <= 0:
            logger.error(f"❌ Invalid entry prices: CE={straddle_data.get('ce_entry_price')}, PE={straddle_data.get('pe_entry_price')}")
            return False
        
        logger.debug(f"✅ Straddle data validation passed for {straddle_data['trade_uid']}")
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation error: {e}")
        return False


async def manual_sync_trade_orders(trade_uid: str) -> Dict:
    """
    Manually synchronizes the state of a trade with the broker's order book.
    This is a two-way sync:
    1. Finds orders at the broker that are missing or have a different status in the local DB.
    2. Finds orders in the local DB that are missing from the broker (ghost orders).
    It then updates the local database to match the broker's reality without placing any new orders.
    """
    logger.info(f"🛠️ MANUAL SYNC initiated for trade {trade_uid}")
    loop = asyncio.get_event_loop()
    executor = get_order_executor()
    if not executor:
        return {"success": False, "error": "OrderExecutor not initialized"}

    # 1. Get all orders for this trade from our local DB
    db_orders_for_trade = await loop.run_in_executor(
        None, state.db.get_orders_by_trade_id, trade_uid
    )
    db_order_map = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')): o for o in db_orders_for_trade}
    logger.info(f"Found {len(db_order_map)} orders for {trade_uid} in the database.")

    # 2. Get all of today's orders from the broker
    try:
        if executor.client_id:
            order_book_func = functools.partial(executor.xt_i.get_order_book, clientID=executor.client_id)
        else:
            order_book_func = executor.xt_i.get_order_book
        broker_order_book = await loop.run_in_executor(None, order_book_func)
        if not broker_order_book or broker_order_book.get('type') != 'success':
            error_msg = broker_order_book.get('description', 'Unknown error')
            logger.error(f"❌ Manual Sync: Order book fetch failed: {error_msg}")
            return {"success": False, "error": f"Broker order book fetch failed: {error_msg}"}
        
        broker_orders = broker_order_book.get('result', [])
        logger.info(f"Fetched {len(broker_orders)} total orders from broker.")
        
        # Create a map of broker orders for efficient lookup
        broker_order_map = {str(o.get('AppOrderID')): o for o in broker_orders if o.get('AppOrderID')}

    except Exception as e:
        logger.error(f"❌ Manual Sync: Exception during order book fetch: {e}", exc_info=True)
        return {"success": False, "error": f"Exception during order book fetch: {e}"}

    # 3. Find and process discrepancies
    newly_found_orders = []
    orders_to_update = [] # Unified list for updates

    # --- Part A: Sync from Broker to DB (Finds orders that exist at broker) ---
    for broker_order in broker_orders:
        app_order_id = str(broker_order.get('AppOrderID'))
        if not app_order_id:
            continue

        # --- ROBUSTNESS FIX: Check if the order belongs to the trade by AppOrderID OR OrderUniqueIdentifier ---
        is_trade_order = False
        uid_from_broker = broker_order.get('OrderUniqueIdentifier', '')
        if trade_uid in uid_from_broker:
            is_trade_order = True
        elif app_order_id in db_order_map:
            is_trade_order = True
        
        if not is_trade_order:
            continue
        # --- END FIX ---

        broker_status = str(broker_order.get('OrderStatus', 'UNKNOWN')).upper()

        if app_order_id not in db_order_map:
            # This order exists at the broker but not in our DB. It's a newly discovered order for this trade.
            if broker_status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED', 'CANCELLED', 'CANCELED', 'REJECTED']:
                logger.info(f"SYNC: Found new order for {trade_uid} at broker: ID {app_order_id}, Status: {broker_status}")
                newly_found_orders.append(broker_order)
        else:
            # This order exists in both. Check for status mismatch.
            db_order = db_order_map[app_order_id]
            db_status = str(db_order.get('order_status', 'UNKNOWN')).upper()
            if broker_status != db_status and broker_status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED', 'CANCELLED', 'CANCELED', 'REJECTED']:
                 logger.info(f"SYNC: Status mismatch for {trade_uid} ID {app_order_id}. DB: {db_status}, Broker: {broker_status}. Marking for update.")
                 orders_to_update.append(broker_order)

    # --- Part B: Sync from DB to Broker (Finds orders in our DB that are missing from broker) ---
    NON_TERMINAL_STATUSES = ['PENDINGNEW', 'NEW', 'OPEN', 'REPLACED', 'PENDINGCANCEL', 'PENDINGREPLACE', 'PARTIALLYFILLED']
    for app_order_id, db_order in db_order_map.items():
        if app_order_id not in broker_order_map:
            db_status = str(db_order.get('order_status', 'UNKNOWN')).upper()
            # If the order is in a non-terminal state in our DB but doesn't exist at the broker, it's a ghost order.
            if db_status in NON_TERMINAL_STATUSES:
                logger.warning(f"SYNC: Ghost order found for {trade_uid}. ID {app_order_id} is in DB as '{db_status}' but NOT in broker's order book. Marking as REJECTED.")
                # Create a mock broker order object to update the status in our DB.
                # We need to preserve the original order details.
                updated_ghost_order = dict(db_order) # Make a copy
                updated_ghost_order['OrderStatus'] = 'REJECTED' # The new status
                updated_ghost_order['CancelRejectReason'] = 'Not found in broker order book during manual sync'
                orders_to_update.append(updated_ghost_order)

    # 4. Persist changes
    if newly_found_orders:
        logger.info(f"Inserting {len(newly_found_orders)} newly found orders into DB for {trade_uid}.")
        for order_data in newly_found_orders:
            order_data['order_unique_id'] = order_data.get('OrderUniqueIdentifier')
            await loop.run_in_executor(None, state.db.insert_order, order_data)

    if orders_to_update:
        logger.info(f"Updating {len(orders_to_update)} orders with status changes in DB for {trade_uid}.")
        for order_data in orders_to_update:
            # The insert_order method should handle upserts (insert or update).
            # Ensure the UID is present for the upsert logic.
            if 'order_unique_id' not in order_data:
                 order_data['order_unique_id'] = order_data.get('OrderUniqueIdentifier')
            await loop.run_in_executor(None, state.db.insert_order, order_data)

    if not newly_found_orders and not orders_to_update:
        logger.info(f"✅ Manual Sync for {trade_uid}: No discrepancies found. Database is up to date.")
        return {"success": True, "message": "No discrepancies found."}

    # 5. Trigger snapshot to update UI
    logger.info(f"Triggering snapshot for {trade_uid} after manual sync.")
    await trigger_snapshot_and_broadcast(trade_uid)

    return {
        "success": True,
        "message": f"Sync complete. Found {len(newly_found_orders)} new orders, updated {len(orders_to_update)} orders."
    }


async def get_straddle_pnl(trade_uid: str) -> Optional[Dict]:
    """
    Calculate current P&L for a straddle
    
    Args:
        trade_uid: Trade UID
    
    Returns:
        Dict with P&L details or None
    """
    try:
        # Get straddle from database
        straddle = state.db.get_straddle(trade_uid)
        if not straddle:
            logger.error(f"❌ Straddle {trade_uid} not found")
            return None
        
        # Get current prices
        ce_token = straddle['ce_token']
        pe_token = straddle['pe_token']
        
        ce_ltp = state.get_price(ce_token) or 0.0
        pe_ltp = state.get_price(pe_token) or 0.0
        
        if ce_ltp <= 0 or pe_ltp <= 0:
            logger.warning(f"⚠️  Invalid current prices for {trade_uid}: CE={ce_ltp}, PE={pe_ltp}")
            return None
        
        # Calculate P&L
        ce_entry = straddle['ce_entry_price']
        pe_entry = straddle['pe_entry_price']
        ce_qty = straddle['ce_quantity']
        pe_qty = straddle['pe_quantity']
        
        # P&L = (Entry - Current) * Quantity (for SELL positions)
        ce_pnl = (ce_entry - ce_ltp) * ce_qty
        pe_pnl = (pe_entry - pe_ltp) * pe_qty
        total_pnl = ce_pnl + pe_pnl
        
        # Calculate P&L percentage
        total_premium = straddle['total_premium']
        pnl_percent = (total_pnl / total_premium * 100) if total_premium > 0 else 0
        
        return {
            'trade_uid': trade_uid,
            'ce_ltp': ce_ltp,
            'pe_ltp': pe_ltp,
            'ce_pnl': ce_pnl,
            'pe_pnl': pe_pnl,
            'total_pnl': total_pnl,
            'pnl_percent': pnl_percent,
            'total_premium': total_premium
        }
        
    except Exception as e:
        logger.error(f"❌ Error calculating P&L for {trade_uid}: {e}")
        return None
