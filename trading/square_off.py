"""
Square Off - Close positions with 1-lot-per-leg batching
Evenly distributes legs across batches
Verification happens as background task
"""
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from utils.logger import logger
from models.state import state
from trading.order_executor import get_order_executor # Removed create_batch_orders
from market_data import SYMBOL_CONFIG
from trading.order_batching_utils import generate_chunked_orders
from background.tasks import verify_orders_task, trigger_snapshot_and_broadcast
from utils.helpers import get_correct_lot_size
from trading.hedger import execute_synthetic_hedge


async def _neutralize_delta_before_square_off(trade_uid: str):
    """Executes a delta-neutralizing hedge right before a square-off."""
    # Defensive check: only run if status is SQUARING-OFF, set by the main square_off function.
    loop = asyncio.get_event_loop()
    trade = await loop.run_in_executor(
        None, state.db.get_straddle_by_id, trade_uid
    )
    if not trade or trade.get('status') != 'SQUARING-OFF':
        logger.warning(f"⚠️ Pre-square-off hedge for {trade_uid} skipped: status is not 'SQUARING-OFF'.")
        return

    logger.info(f"🛡️  Checking for delta neutralization before squaring off {trade_uid}...")
    
    from background.tasks import create_snapshot_for_trade
    await create_snapshot_for_trade(trade_uid)
    
    snapshot = state.trade_snapshots.get(trade_uid)
    if not snapshot:
        logger.warning(f"⚠️  No snapshot for {trade_uid}, cannot neutralize delta.")
        return

    net_delta = snapshot.get('net_delta', 0.0)
    lot_size = snapshot.get('lot_size', 65)
    delta_tolerance = float(lot_size)

    if abs(net_delta) <= delta_tolerance:
        logger.info(f"✅ Delta ({net_delta:.2f}) is within tolerance ({delta_tolerance}). No pre-square-off hedge needed.")
        return

    # --- NEW: Calculate ATM from snapshot's spot price for consistency ---
    spot_price = snapshot.get('spot_price', 0.0)
    symbol = trade.get("symbol", "NIFTY")
    symbol_upper = symbol.upper()
    base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
    gap = SYMBOL_CONFIG[base_symbol]['gap'] if base_symbol else 50
    atm_strike_from_snapshot = int(round(spot_price / gap) * gap) if spot_price > 0 and gap > 0 else None
    # --- END NEW ---

    logger.warning(f"🛡️  Net delta is {net_delta:.2f}. Executing neutralizing hedge before square-off.")
    result = await execute_synthetic_hedge(
        trade_uid=trade_uid, net_delta=net_delta, target_delta_reduction=-net_delta, hedge_type="PRE_SQF_NEUTRAL", atm_strike_override=atm_strike_from_snapshot
    )

    if result and result.get('success'):
        logger.info(f"✅ Successfully neutralized delta for {trade_uid} before square-off.")
        await asyncio.sleep(1.0) # Reduced delay
    else:
        logger.error(f"❌ FAILED to neutralize delta for {trade_uid} before square-off. Proceeding anyway.")

def calculate_even_batch_pattern(lots_list: List[int]) -> List[List[int]]:
    raise NotImplementedError("calculate_even_batch_pattern is deprecated and replaced by chunked order generation.")


async def square_off(
    trade_uid: str,
    positions: List[Dict] = None,
    straddle_data: Dict = None
) -> Optional[Dict]:
    """
    ⚡ SQUARE OFF - Close positions with chunked order execution.
    Verification happens after each chunk.
    """
    start_time = datetime.now()

    # Initialize the set if it doesn't exist
    if not hasattr(state, 'closing_trades'):
        state.closing_trades = set()
    
    # --- FIX: Mark trade as 'closing' to prevent race conditions with other actions like hedging ---
    state.closing_trades.add(trade_uid)
    logger.info(f"ℹ️  Trade {trade_uid} marked as 'closing' to prevent concurrent actions.")

    try:
        loop = asyncio.get_event_loop()

        # --- MOVED STATUS CHECK TO THE TOP TO PREVENT RACE CONDITIONS ---
        # If straddle_data is not provided, fetch it fresh to check the status.
        if not straddle_data:
            # Run synchronous DB call in an executor
            straddle_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            if not straddle_data:
                logger.error(f"❌ Could not find straddle data for {trade_uid} in DB. Aborting square_off.")
                return {'success': False, 'error': 'Trade data not found in DB'}

        current_status = straddle_data.get('status')
        if current_status == 'SQUARING-OFF':
            logger.warning(f"⚠️ Square-off for {trade_uid} is already in progress. Ignoring duplicate request.")
            return {'success': False, 'error': 'Square-off already in progress.'}
        
        # --- MODIFICATION: Allow square-off from BUILDING or ACTIVE status ---
        # Added ROLLING to allow the roll_position function to call square_off
        allowed_statuses = ['ACTIVE', 'BUILDING', 'ROLLING']
        if current_status not in allowed_statuses:
            logger.warning(f"⚠️ Cannot square off trade {trade_uid} with status '{current_status}'.")
            return {'success': False, 'error': f'Trade status is {current_status}, not in {allowed_statuses}.'}

        # --- FIX: Immediately stop all monitors for this trade before proceeding ---
        from trading.trade_manager import get_trade_manager
        manager = get_trade_manager(trade_uid)
        if manager:
            logger.info(f"🛑 Stopping all monitors for {trade_uid} before square-off execution.")
            await manager.stop_monitoring()
        else:
            logger.warning(f"⚠️ Could not find TradeManager for {trade_uid} to stop monitors, but proceeding with square-off.")
        # --- END FIX ---

        # If we reach here, status is ACTIVE. Now change it to prevent other actions.
        # Run synchronous DB call in an executor
        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'SQUARING-OFF')
        logger.info(f"🔄 Status updated: {trade_uid} -> SQUARING-OFF (monitors will now pause)")

        await _neutralize_delta_before_square_off(trade_uid)

        # --- CANCELLATION CHECK after pre-sqf hedge ---
        if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
            logger.warning(f"🛑 Square-off for {trade_uid} cancelled by user after pre-sqf hedge.")
            await loop.run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            ) # Revert status
            if trade_uid in state.cancellation_flags:
                del state.cancellation_flags[trade_uid]
            if manager:
                await manager.start_monitoring() # Restart monitors
            return {'success': False, 'error': 'Cancelled by user'}
        # --- END CANCELLATION CHECK ---

        executor = get_order_executor()
        if not executor:
            logger.error("❌ OrderExecutor not initialized")
            return None
        
        logger.info("="*100)
        logger.info(f"⏹️  SQUARE OFF | Trade UID: {trade_uid}")
        logger.info("="*100)

        # Get positions from straddle_data if not provided
        if not positions:
            positions = await extract_positions_from_straddle(straddle_data)
        
        if not positions:
            logger.error("❌ No positions to square off")
            # Revert status back to ACTIVE since we aborted before doing anything.
            try:                
                await loop.run_in_executor(
                    None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
                )
                logger.info(f"🔄 Reverted status for {trade_uid} to 'ACTIVE' as no positions were found.")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to revert status for {trade_uid}. Trade may be stuck in SQUARING-OFF. Error: {e}")
            return {'success': False, 'error': 'No positions to square off'}
        
        # ✅ Get correct lot_size from option chain
        correct_lot_size = await get_correct_lot_size(straddle_data)
        logger.info(f"✅ Verified lot_size: {correct_lot_size}")
        
        # Analyze position structure with correct lot_size
        position_summary = analyze_positions(positions, correct_lot_size)
        logger.info(f"📊 Position summary: {len(position_summary)} legs")
        
        lots_list = []
        leg_names = []
        total_lots_across_all_legs = 0 # For base_lots_for_trade
        for leg in position_summary:
            leg_name = f"{leg['strike']} {leg['option_type']} {leg['action']}"
            logger.info(
                f"   {leg_name}: "
                f"{leg['total_quantity']} ({leg['lots']} lots x {leg['lot_size']})"
            )
            lots_list.append(leg['lots'])
            leg_names.append(leg_name)
            total_lots_across_all_legs += leg['lots']
        
        if not position_summary:
            logger.error("❌ No valid legs to square off after analysis.")
            try:
                await loop.run_in_executor(
                    None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
                )
                logger.info(f"🔄 Reverted status for {trade_uid} to 'ACTIVE' as no valid legs were found.")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to revert status for {trade_uid}. Trade may be stuck in SQUARING-OFF. Error: {e}")
            return {'success': False, 'error': 'No valid legs to square off'}

        # Determine base_lots_for_trade for min_lots_per_order calculation.
        # Using the maximum lots of any leg as a proxy for base_lots_for_trade.
        base_lots_for_trade = max(leg['lots'] for leg in position_summary) if position_summary else 0
        if base_lots_for_trade == 0:
            logger.warning("⚠️ Base lots for trade is 0, defaulting min_lots_per_order to 1 for square-off.")
            
        # Prepare legs_data for the new chunking strategy
        legs_data_for_batching = []
        for leg in position_summary:
            legs_data_for_batching.append({
                'token': leg['token'],
                'option_type': leg['option_type'],
                'action': 'BUY' if leg['action'] == 'SELL' else 'SELL', # Reverse action for square-off
                'total_lots': leg['lots'],
                'lot_size': leg['lot_size'],
                'expected_price': leg['current_price'],
                'exchange_segment': leg['exchange_segment'],
                'product_type': leg['product_type']
            })

        # ✅ Calculate chunked orders
        all_chunks = generate_chunked_orders(
            trade_uid_prefix=f"SQF_{trade_uid}",
            legs_data=legs_data_for_batching,
            base_lots_for_trade=base_lots_for_trade,
            # Process in ~15% chunks (100/7) to reduce verification cycles and speed up execution.
            chunk_divisor=7
        )
        
        # --- Inject limit order buffer from config into each order ---
        config = straddle_data.get('config', {})
        buy_buffer = float(config.get('buy_buffer', 2.0))
        sell_buffer = float(config.get('sell_buffer', 2.0))
        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer
        # --- END INJECTION ---

        logger.info("="*100)
        logger.info(f"🔄 CHUNKED EXECUTION PLAN (min_lots_per_order based on max leg lots)")
        logger.info(f"   Total chunks: {len(all_chunks)}")
        logger.info(f"   Legs: {', '.join(leg_names)}")
        logger.info("")
        
        # Show chunk preview
        for chunk_idx, chunk in enumerate(all_chunks, 1):
            chunk_summary = []
            for order in chunk:
                chunk_summary.append(f"{order['action']} {order['quantity'] // order['lot_size']} lots {order['option_type']} {order['token']}")
            logger.info(f"   Chunk {chunk_idx:2d}: {', '.join(chunk_summary)}")
        
        logger.info("="*100)
        
        # ✅ Execute all chunks WITH inline verification
        all_successful_orders = []
        all_failed_orders = []
        all_verified_fills = []
        all_verification_failures = []
        sqf_aborted = False

        if not hasattr(state, 'temp_order_cache'):
            state.temp_order_cache = {}

        batch_execution_start = datetime.now()
        
        for chunk_idx, chunk_orders in enumerate(all_chunks, 1):
            if not chunk_orders:
                continue

            # --- NEW: Intra-chunk retry loop ---
            orders_to_process_in_chunk = list(chunk_orders)
            chunk_attempt = 0
            max_chunk_retries = 1  # One immediate retry

            while orders_to_process_in_chunk and chunk_attempt <= max_chunk_retries:
                if chunk_attempt > 0:
                    logger.info(f"🔄 Re-executing {len(orders_to_process_in_chunk)} orders within chunk {chunk_idx} (Attempt {chunk_attempt + 1})...")
                    await asyncio.sleep(2.0)  # Wait before retrying

                    # Increase buffer and clear price for re-calculation
                    for order in orders_to_process_in_chunk:
                        original_buffer = order.get('limit_order_buffer', 2.0)
                        order['limit_order_buffer'] = original_buffer + 4.0
                        order['limit_price'] = 0.0  # Force recalculation
                        logger.info(f"   -> For UID {order['uid']}, new buffer is {order['limit_order_buffer']:.1f}")

                logger.info(f"⚡ Executing SQUARE OFF chunk {chunk_idx}/{len(all_chunks)} (Attempt {chunk_attempt + 1}) with {len(orders_to_process_in_chunk)} orders.")

                chunk_result = await executor.execute_batch(
                    orders_to_process_in_chunk, f"SQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt + 1}"
                )

                successful_in_attempt = chunk_result.get('successful_orders', [])
                failed_in_attempt = chunk_result.get('failed_orders', [])

                all_successful_orders.extend(successful_in_attempt)
                all_failed_orders.extend(failed_in_attempt)

                attempt_order_ids = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_attempt if o.get('order_id') or o.get('app_order_id')]
                app_order_id_to_uid_map_attempt = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_attempt}

                # --- VERIFICATION FOR THIS ATTEMPT ---
                verified_fills_for_attempt = []
                unverified_order_ids = list(attempt_order_ids)
                max_verification_attempts = 3
                orders_to_reexecute_in_this_chunk = []
                ids_to_remove_from_successful_attempt = set()

                for verification_attempt in range(max_verification_attempts):
                    if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                        logger.warning(f"🛑 Square-off for {trade_uid} cancelled by user during verification.")
                        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                        if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
                        if manager: await manager.start_monitoring()
                        return {'success': False, 'error': 'Cancelled by user'}

                    if not unverified_order_ids: break

                    logger.info(f"📊 Verifying chunk {chunk_idx} (Attempt {chunk_attempt+1}), verification {verification_attempt + 1}/{max_verification_attempts} for {len(unverified_order_ids)} orders...")
                    verification_result = await verify_orders_task(unverified_order_ids, f"SQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt+1}_VERIFY{verification_attempt+1}")

                    if verification_result:
                        newly_verified = verification_result.get('verified_success', [])
                        newly_failed = verification_result.get('verified_failed', [])
                        verified_fills_for_attempt.extend(newly_verified)

                        verified_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_verified}
                        reexecute_ids = {str(o.get('order_id')) for o in newly_failed if o.get('status') == 'REEXECUTE_NEEDED'}
                        terminal_failure_statuses = {'REJECTED', 'CANCELLED', 'CANCELED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}
                        failed_ids = {str(o.get('order_id')) for o in newly_failed if o.get('status') in terminal_failure_statuses}
                        resolved_ids = verified_ids.union(failed_ids).union(reexecute_ids)

                        unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]

                    if unverified_order_ids:
                        logger.warning(f"⚠️ {len(unverified_order_ids)} orders still pending in chunk {chunk_idx}. Retrying verification in 1.0s...")
                        await asyncio.sleep(1.0)

                # After verification, find orders to re-execute in the same chunk
                if 'newly_failed' in locals():
                    for failed_order_info in newly_failed:
                        if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                            ids_to_remove_from_successful_attempt.add(str(failed_order_info.get('order_id')))
                            order_id = str(failed_order_info.get('order_id'))
                            original_order_uid = app_order_id_to_uid_map_attempt.get(order_id)
                            if original_order_uid:
                                original_order_data = next((o for o in orders_to_process_in_chunk if o['uid'] == original_order_uid), None)
                                if original_order_data:
                                    orders_to_reexecute_in_this_chunk.append(original_order_data.copy())
                                    logger.info(f"🔄 Order {original_order_uid} marked for re-execution in this chunk.")
                        
                        terminal_failure_statuses = {'REJECTED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}
                        if failed_order_info.get('status') in terminal_failure_statuses:
                            logger.error(f"❌ Verification Failure for Order {failed_order_info.get('order_id')}: {failed_order_info.get('status')} - {failed_order_info.get('reason', 'N/A')}")
                            all_verification_failures.append(failed_order_info)

                # Correct the success tracking for this attempt
                if ids_to_remove_from_successful_attempt:
                    all_successful_orders = [
                        o for o in all_successful_orders
                        if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful_attempt
                    ]
                    logger.info(f"Corrected success tracking: Removed {len(ids_to_remove_from_successful_attempt)} orders that were cancelled for re-execution in this attempt.")


                # Update the list for the next `while` loop iteration
                orders_to_process_in_chunk = orders_to_reexecute_in_this_chunk

                # Restore full UIDs before caching
                for fill in verified_fills_for_attempt:
                    app_oid = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid') or fill.get('order_id'))
                    if app_oid in app_order_id_to_uid_map_attempt:
                        fill['OrderUniqueIdentifier'] = app_order_id_to_uid_map_attempt[app_oid]
                        fill['order_unique_id'] = fill['OrderUniqueIdentifier']

                # Aggregate verified fills and cache them
                all_verified_fills.extend(verified_fills_for_attempt)
                if verified_fills_for_attempt:
                    state.temp_order_cache.setdefault(trade_uid, []).extend(verified_fills_for_attempt)
                    logger.info(f"Cached {len(verified_fills_for_attempt)} square-off orders from chunk {chunk_idx} attempt {chunk_attempt+1}.")

                chunk_attempt += 1

            # After all attempts for a chunk, if any orders are still unverified or failed re-execution, abort.
            if unverified_order_ids or orders_to_process_in_chunk:
                logger.error(f"❌ FAILED to execute/verify all orders in chunk {chunk_idx} after all retries. Stopping further square-off chunks.")
                sqf_aborted = True
                break

            # Proceed to the next chunk
        
        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()
        
        # --- OPTIMIZATION: Insert all verified orders and trigger snapshot ONCE after all chunks ---
        all_verified_fills_from_cache = state.temp_order_cache.get(trade_uid, [])
        if all_verified_fills_from_cache:
            if hasattr(state.db, 'insert_order'):
                logger.info(f"Inserting {len(all_verified_fills_from_cache)} total verified square-off orders into DB for {trade_uid}...")
            
            # Trigger a single, final snapshot for the UI.
            await trigger_snapshot_and_broadcast(trade_uid)
            # --- FIX: Clear the temporary cache for this trade after the square-off is complete ---
            if trade_uid in state.temp_order_cache:
                del state.temp_order_cache[trade_uid]
                logger.info(f"🧹 Cleared temp order cache for {trade_uid} after square-off.")
        # --- END OPTIMIZATION ---
        # --- REFACTORED: DB INSERT MOVED TO FINALLY BLOCK FOR ROBUSTNESS ---
        # This ensures that even if PnL calculation fails, the orders are still saved.
        # The actual insertion logic is now in the `finally` block.
        # --- END REFACTOR ---
        
        # Summary
        total_orders = len(all_successful_orders) + len(all_failed_orders)
        success_count = len(all_successful_orders)
        failed_count = len(all_failed_orders)
        
        logger.info("="*100)
        logger.info(f"⚡ EXECUTION COMPLETE: {success_count}/{total_orders} | Time: {batch_execution_time:.2f}s")
        logger.info("="*100)
        
        # Calculate total time
        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        # Final status
        final_success = (failed_count == 0 and not sqf_aborted and not all_verification_failures)
        
        logger.info("="*100)
        logger.info(f"✅ SQUARE-OFF_{trade_uid} COMPLETE | Time: {total_time:.2f}s")
        logger.info(f"✅ Success: {success_count}/{total_orders} | ❌ Failed: {failed_count}")
        logger.info("="*100)
        
        # The verified (or fallback) fills for all chunks have already been
        # collected and stored in the temporary cache during the execution loop.
        if not all_verified_fills:
             logger.warning(f"⚠️ No fills found for {trade_uid} to calculate final stats.")

        if all_verified_fills:
            leg_quantities = aggregate_verified_quantities(all_verified_fills, position_summary) # This function needs to be updated to use the new fill structure
            for leg_info in leg_quantities:
                logger.info(f"   {leg_info['name']}: {leg_info['quantity']} @ ₹{leg_info['avg_price']:.2f}")

        # Calculate P&L and update status
        pnl_result = None # Initialize to None
        if final_success:
            pnl_result = await calculate_realized_pnl(trade_uid, recent_fills=all_verified_fills)
            if pnl_result:
                realized_pnl = pnl_result.get('realized_pnl', 0.0)
                logger.info("="*100)
                logger.info(f"💰 REALIZED P&L CALCULATION for {trade_uid}")
                logger.info(f"   Total Sell Value: ₹{pnl_result.get('total_sell_value', 0):,.2f}")
                logger.info(f"   Total Buy Value:  ₹{pnl_result.get('total_buy_value', 0):,.2f}")
                logger.info(f"   Realized P&L:     ₹{realized_pnl:,.2f}")
                logger.info("="*100)
            else:
                logger.warning(f"⚠️ Could not calculate realized PnL for {trade_uid}.")

        # --- PERSIST FINAL REALIZED PNL AND STATUS ---
        # Fetch the latest trade data to ensure we don't overwrite any concurrent updates.
        # Use the straddle_data that was passed in or fetched at the beginning, as it's the most up-to-date.
        if straddle_data:
            if pnl_result:
                straddle_data['realized_pnl'] = pnl_result.get('realized_pnl', 0.0)
            straddle_data['status'] = 'CLOSED_SQF' # Set final status to CLOSED after full square-off
            
            # --- FIX: Robust DB Update with Retry for Full Square-off ---
            saved_successfully = False
            for attempt in range(3):
                try:
                    await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
                    # Verify read-back
                    saved_doc = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                    if saved_doc and saved_doc.get('status') == 'CLOSED_SQF':
                        saved_successfully = True
                        logger.info(f"✅ Saved realized PnL (₹{straddle_data.get('realized_pnl', 0.0):,.2f}) and set status to CLOSED_SQF for {trade_uid} (Verified).")
                        break
                except Exception as e:
                    logger.error(f"❌ DB persistence error for {trade_uid} (Attempt {attempt+1}): {e}")
                    await asyncio.sleep(0.5)
            
            if not saved_successfully:
                logger.critical(f"🚨 CRITICAL: Failed to persist final status for {trade_uid} after 3 attempts.")
            # --- END FIX ---
        else:
            logger.error(f"❌ Could not retrieve trade data for {trade_uid} to persist final PnL and status after full square-off.")
        # --- END PERSIST ---

        # --- UI UPDATE: Send a single, final snapshot to the UI after the entire square-off is complete. ---
        await trigger_snapshot_and_broadcast(trade_uid, trade_data=straddle_data)
        logger.info(f"✅ Final snapshot for {trade_uid} broadcasted to UI after full square-off.")
        
        return {
            'success': final_success,
            'total_orders': total_orders,
            'successful_count': success_count,
            'failed_count': failed_count,
            'successful_orders': all_successful_orders,
            'failed_orders': all_failed_orders,
            'execution_time': batch_execution_time,
            'total_time': total_time,
            'batches': len(all_chunks), # Now refers to chunks
            'trade_uid': trade_uid,
            'pnl': pnl_result
        }

    except asyncio.CancelledError:
        logger.critical(f"🛑 Square-off for {trade_uid} was cancelled abruptly. Reverting status to ACTIVE to prevent a stale state.")
        # This is critical because a partially squared-off position must be monitored.
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
        except Exception as db_e:
            logger.error(f"CRITICAL: Failed to revert status to ACTIVE for {trade_uid} after cancellation. DB Error: {db_e}")
        # Re-raise the cancellation error so the task actually stops.
        raise
    except Exception as e:
        logger.error(f"❌ Square-off failed: {e}")
        # Attempt to revert status to ERROR on unexpected failure
        try:
            # Revert to ACTIVE so the remaining open position is monitored.
            await asyncio.get_event_loop().run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
            logger.warning(f"⚠️ Status updated: {trade_uid} -> ACTIVE due to unexpected exception in square_off.")
        except Exception as db_e:
            logger.error(f"CRITICAL: Failed to revert status to ACTIVE for {trade_uid} after another error. DB Error: {db_e}")
        import traceback
        logger.error(traceback.format_exc())
        return None
    finally:
        # --- REFACTORED FINALIZATION ---
        # This block runs regardless of success or failure, ensuring cleanup.
        # 1. Unmark the trade as 'closing'
        if hasattr(state, 'closing_trades') and trade_uid in state.closing_trades:
            state.closing_trades.remove(trade_uid)
            logger.info(f"ℹ️  Trade {trade_uid} unmarked as 'closing'.")
        
        # 2. Persist all verified orders from the cache to the DB.
        all_verified_fills_from_cache = state.temp_order_cache.get(trade_uid, [])
        if all_verified_fills_from_cache and hasattr(state.db, 'insert_order'):
            logger.info(f"Finalizing: Inserting {len(all_verified_fills_from_cache)} verified orders into DB for {trade_uid}...")
            for fill_data in all_verified_fills_from_cache:
                if fill_data.get('OrderUniqueIdentifier'):
                    await asyncio.get_event_loop().run_in_executor(None, state.db.insert_order, fill_data)
        
        # 3. Clean up the cache.
        if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
            del state.temp_order_cache[trade_uid]



def analyze_positions(positions: List[Dict], correct_lot_size: int = None) -> List[Dict]:
    """Analyze position structure and ensure quantities are multiples of lot_size"""
    summary = []
    
    for pos in positions:
        quantity = pos.get('quantity', 0)
        lot_size = correct_lot_size or pos.get('lot_size', 65)
        
        # ✅ Calculate lots and round down to ensure multiple
        lots = quantity // lot_size if lot_size > 0 else 0
        
        # ✅ Recalculate quantity to ensure exact multiple
        corrected_quantity = lots * lot_size
        
        # ✅ Skip if no lots (prevents sending 0 or fractional quantities)
        if lots == 0:
            logger.warning(f"⚠️  Skipping {pos.get('option_type')} - quantity {quantity} less than lot size {lot_size}")
            continue
        
        if corrected_quantity != quantity:
            logger.warning(f"⚠️  Adjusted {pos.get('option_type')} quantity: {quantity} → {corrected_quantity}")
        
        summary.append({
            'token': pos.get('token'),
            'strike': pos.get('strike', 0),
            'option_type': pos.get('option_type', 'UNKNOWN'),
            'action': pos.get('action', 'SELL'),
            'total_quantity': corrected_quantity,  # ✅ Corrected quantity
            'lots': lots,
            'lot_size': lot_size,
            'current_price': pos.get('current_price', 0),
            'entry_price': pos.get('entry_price', 0),
            'exchange_segment': pos.get('exchange_segment', 2),
            'product_type': pos.get('product_type', 'MIS')
        })
    
    return summary


async def extract_positions_from_straddle(straddle_data: Dict) -> List[Dict]:
    """
    Extracts ALL open positions for a trade by calculating the net position
    from the order history stored in the database. This correctly includes
    the initial build and all subsequent hedges.
    """
    trade_uid = straddle_data.get('trade_uid')
    if not trade_uid:
        logger.error("❌ Cannot extract positions: trade_uid missing from straddle_data.")
        return []
    
    loop = asyncio.get_event_loop()
    logger.info(f"🔍 Calculating net open positions for {trade_uid} from order history...")

    try:
        # Use the efficient DB query to get all orders for this specific trade.
        all_db_orders = await loop.run_in_executor(
            None, state.db.get_orders_by_trade_id, trade_uid
        )
        
        # Filter for filled orders.
        trade_orders = [
            o for o in all_db_orders if (str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in 
                                        ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED'])
        ]

        # --- ROBUSTNESS FIX: Merge orders from temporary cache to handle race conditions ---
        if hasattr(state, 'temp_order_cache') and state.temp_order_cache:
            cached_fills = state.temp_order_cache.get(trade_uid, [])
            if cached_fills:
                logger.info(f"Found {len(cached_fills)} orders in temp cache for {trade_uid}. Merging for square-off.")
                db_order_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')) for o in trade_orders}
                
                new_fills_from_cache = 0
                for fill in cached_fills:
                    app_order_id = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid'))
                    if app_order_id and app_order_id not in db_order_ids:
                        trade_orders.append(fill)
                        new_fills_from_cache += 1
                if new_fills_from_cache > 0:
                    logger.info(f"Added {new_fills_from_cache} new orders from cache for square-off calculation.")

        if not trade_orders:
            logger.warning(f"⚠️ No filled orders found for trade {trade_uid} in DB or cache. Cannot determine position.")
            return []

        logger.info(f"Found {len(trade_orders)} filled orders for {trade_uid}.")

        # Calculate net quantity for each instrument (token).
        net_positions = {}
        for order in trade_orders:
            # --- ROBUSTNESS FIX: Check for missing token before int() conversion ---
            token_val = order.get('exchange_instrument_id') or order.get('ExchangeInstrumentID')
            if not token_val:
                order_id_for_log = order.get('AppOrderID') or order.get('app_order_id') or order.get('order_id', 'UNKNOWN_ID')
                logger.warning(f"Skipping order {order_id_for_log} in position extraction: missing instrument token.")
                continue
            token = int(token_val)
            # --- END FIX ---
            
            qty = int(order.get('cumulative_quantity') or order.get('CumulativeQuantity', 0))
            price = float(order.get('order_avg_price') or order.get('OrderAverageTradedPrice', 0))
            side = str(order.get('order_side') or order.get('OrderSide', '')).upper()
            
            if token not in net_positions:
                net_positions[token] = {
                    'quantity': 0, 'token': token,
                    'buy_val': 0.0, 'buy_qty': 0,
                    'sell_val': 0.0, 'sell_qty': 0
                }

            # Update net quantity: SELL adds to our short position (+), BUY reduces it (-)
            if side == 'SELL':
                net_positions[token]['quantity'] += qty
                net_positions[token]['sell_qty'] += qty
                net_positions[token]['sell_val'] += (qty * price)
            elif side == 'BUY':
                net_positions[token]['quantity'] -= qty
                net_positions[token]['buy_qty'] += qty
                net_positions[token]['buy_val'] += (qty * price)

        # Convert the net positions map into a list of positions to be closed.
        final_positions = []
        symbol_upper = straddle_data.get('symbol', 'NIFTY').upper()

        # --- ROBUSTNESS FIX: Re-derive exchange segment from symbol ---
        # This prevents using a stale segment from an old DB record.
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        derived_segment = SYMBOL_CONFIG.get(base_symbol, {}).get('segment') if base_symbol else None

        if derived_segment:
            correct_exchange_segment = derived_segment
        else:
            # Fallback to what's in the data, or default to NSEFO
            correct_exchange_segment = straddle_data.get('exchange_segment', 2)
            logger.warning(f"Could not derive segment for {symbol_upper} from SYMBOL_CONFIG. Falling back to DB value: {correct_exchange_segment}.")
        # --- END FIX ---

        option_chain = state.option_chains.get(symbol_upper)

        for token, pos_data in net_positions.items():
            net_qty = pos_data['quantity']
            if net_qty == 0: continue

            # Find strike and option_type from the live option chain to handle rolled positions
            strike, option_type = None, None
            if option_chain and option_chain.get('chain'):
                for row in option_chain['chain']:
                    if row.get('ce_token') == token:
                        strike, option_type = row['strike'], 'CE'; break
                    if row.get('pe_token') == token:
                        strike, option_type = row['strike'], 'PE'; break
            
            if not strike:
                logger.warning(f"Could not find token {token} in live option chain for {trade_uid}. This can happen after a roll. Square-off may fail for this leg if details are not resolved.")
                strike = 0
                option_type = 'UNKNOWN'

            # --- FIX: Ensure current_price is valid before creating orders ---
            # If the price is missing from the cache, fetch it directly.
            current_price = state.get_price(token) or 0.0
            if not current_price or current_price <= 0:
                logger.warning(f"⚠️ Missing current_price for token {token} in extract_positions_from_straddle. Fetching fresh LTP.")
                from market_data import get_ltp
                current_price = await get_ltp(token, correct_exchange_segment)
                if not current_price or current_price <= 0:
                    logger.error(f"❌ Could not fetch valid LTP for token {token} in extract_positions_from_straddle. Price will be 0.")
                    current_price = 0.0 # Ensure it's zero if fetch fails
            # --- END FIX ---
            
            # --- Calculate Weighted Average Entry Price ---
            entry_price = 0.0
            if net_qty > 0: # Net Short Position
                if pos_data['sell_qty'] > 0:
                    entry_price = pos_data['sell_val'] / pos_data['sell_qty']
            elif net_qty < 0: # Net Long Position
                if pos_data['buy_qty'] > 0:
                    entry_price = pos_data['buy_val'] / pos_data['buy_qty']
            # ----------------------------------------------

            pos_data['strike'] = strike
            pos_data['option_type'] = option_type
            pos_data['action'] = 'SELL' if net_qty > 0 else 'BUY'
            pos_data['quantity'] = abs(net_qty)
            pos_data['entry_price'] = entry_price
            pos_data['current_price'] = current_price
            pos_data['exchange_segment'] = correct_exchange_segment
            pos_data['product_type'] = straddle_data.get('product_type', 'MIS')
            final_positions.append(pos_data)
        
        logger.info(f"✅ Net positions calculated: {len(final_positions)} legs.")
        return final_positions

    except Exception as e:
        logger.error(f"❌ Error calculating net positions for {trade_uid}: {e}", exc_info=True)
        return []


def aggregate_verified_quantities(orders: List[Dict], position_summary: List[Dict]) -> List[Dict]:
    """Aggregate quantities by leg"""
    leg_data = {}

    # Create a map from token to leg name (e.g., '25600 PE SELL') for easy lookup
    token_to_leg_name = {leg['token']: f"{leg['strike']} {leg['option_type']} {leg['action']}" for leg in position_summary}

    for order in orders:
        # The verified fill data from the broker is the source of truth.
        token = int(order.get('ExchangeInstrumentID') or order.get('exchange_instrument_id', 0))
        if not token:
            continue

        # Use the token to find the correct leg name from our initial position summary.
        leg_name = token_to_leg_name.get(token)
        if not leg_name:
            leg_name = f"UNKNOWN_TOKEN_{token}"

        qty = int(order.get('CumulativeQuantity') or order.get('filled_qty', 0))
        price = float(order.get('OrderAverageTradedPrice') or order.get('avg_price', 0) or order.get('fill_price', 0))

        if leg_name not in leg_data:
            leg_data[leg_name] = {'quantity': 0, 'total_value': 0}
        
        leg_data[leg_name]['quantity'] += qty
        leg_data[leg_name]['total_value'] += (price * qty)
    result = []
    for leg_key, data in leg_data.items():
        avg_price = data['total_value'] / data['quantity'] if data['quantity'] > 0 else 0
        result.append({
            'name': leg_key,
            'quantity': data['quantity'],
            'avg_price': avg_price
        })
    
    return result


async def partial_square_off(
    trade_uid: str,
    percentage_of_original: float
) -> Optional[Dict]:
    """
    ⚡ PARTIAL SQUARE OFF - Close a percentage of the *original* trade size.
    Calculates the required percentage of the *current* position to achieve this.
    Verification happens after each chunk. The trade remains ACTIVE.
    """
    start_time = datetime.now()

    loop = asyncio.get_event_loop()
    try:
        straddle_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
        if not straddle_data:
            logger.error(f"❌ Could not find straddle data for {trade_uid} in DB. Aborting partial square_off.")
            return {'success': False, 'error': 'Trade data not found in DB'}

        current_status = straddle_data.get('status')
        if current_status != 'ACTIVE':
            logger.warning(f"⚠️ Cannot partially square off trade {trade_uid} with status '{current_status}'.")
            return {'success': False, 'error': f'Trade status is {current_status}, not ACTIVE.'}

        # --- MULTIPROCESSING CHECK ---
        # If a dedicated process exists for this trade, dispatch the command to it.
        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            logger.info(f"🚀 Dispatching PARTIAL_SQUARE_OFF command to dedicated process for {trade_uid}.")
            process_info = state.trade_processes[trade_uid]
            process_info['command_q'].put({
                'command': 'PARTIAL_SQUARE_OFF',
                'percentage': percentage_of_original
            })
            return {'success': True, 'message': 'Partial Square-off command dispatched to trade process.'}

        # Set a temporary status to indicate the action is in progress
        # This allows the UI to show a cancel button.
        await loop.run_in_executor(
            None, state.db.update_straddle_status, trade_uid, 'PARTIAL-SQF'
        )
        logger.info(f"🔄 Status updated: {trade_uid} -> PARTIAL-SQF")
        # --- FIX: Immediately broadcast the PARTIAL-SQF status to the UI ---
        await trigger_snapshot_and_broadcast(trade_uid)
        # --- END FIX ---

        executor = get_order_executor()
        if not executor:
            logger.error("❌ OrderExecutor not initialized")
            await loop.run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ERROR'
            )
            return None
        
        logger.info("="*100)
        logger.info(f"🪓  PARTIAL SQUARE OFF ({percentage_of_original}%) | Trade UID: {trade_uid}")
        logger.info("="*100)

        positions = await extract_positions_from_straddle(straddle_data)
        
        if not positions:
            logger.error("❌ No positions to partially square off")
            await loop.run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
            return {'success': False, 'error': 'No positions to square off'}
        
        correct_lot_size = await get_correct_lot_size(straddle_data)
        logger.info(f"✅ Verified lot_size: {correct_lot_size}")
        
        # --- Calculate quantities for partial square off ---
        current_total_qty = sum(p['quantity'] for p in positions)

        if current_total_qty <= 0:
            logger.warning(f"Current total quantity for {trade_uid} is zero. Nothing to square off.")
            await loop.run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
            return {'success': True, 'message': 'Current position is zero.'}
        
        # --- MODIFIED: Calculate initial size from 'BUILD' orders for robustness ---
        all_trade_orders = await loop.run_in_executor(
            None, state.db.get_orders_by_trade_id, trade_uid
        )
        
        # Filter for filled orders from the initial build process. This includes the main build,
        # in-build hedges, and sweeps, as they should all share the "BUILD_" prefix.
        build_orders = [
            o for o in all_trade_orders 
            if (o.get('OrderUniqueIdentifier') or o.get('order_unique_id', '')).startswith((f'BUILD_{trade_uid}', f'BUI_{trade_uid}'))
            and (str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in 
                 ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED'])
        ]

        initial_total_qty = 0
        if build_orders:
            initial_total_qty = sum(int(o.get('cumulative_quantity') or o.get('CumulativeQuantity', 0)) for o in build_orders)
            logger.info(f"Calculated initial total quantity of {initial_total_qty} from {len(build_orders)} build-related orders for {trade_uid}.")
        
        # Fallback logic if build orders are not found (e.g., old trades without UID prefix)
        if initial_total_qty <= 0:
            logger.warning(f"⚠️ Could not determine initial quantity for {trade_uid} from BUILD orders. Falling back to DB fields.")
            initial_ce_qty_db = straddle_data.get('initial_ce_quantity') or 0
            initial_pe_qty_db = straddle_data.get('initial_pe_quantity') or 0
            initial_total_qty = initial_ce_qty_db + initial_pe_qty_db
        
        # Further fallback
        if initial_total_qty <= 0:
             # Try using 'lots' * 'lot_size' * 2 (assuming straddle structure)
             lots = int(straddle_data.get('lots') or 0)
             lot_size = int(straddle_data.get('lot_size') or 0)
             if lots > 0 and lot_size > 0:
                 initial_total_qty = lots * lot_size * 2
             else:
                 # Last resort fallback to current quantity
                 initial_total_qty = current_total_qty
                 logger.warning(f"⚠️ Could not determine initial quantity for {trade_uid} from any method. Using current quantity as base.")

        target_sqf_qty = initial_total_qty * (percentage_of_original / 100.0)
        
        # Calculate the ratio relative to CURRENT quantity
        if current_total_qty > 0:
            sqf_ratio = target_sqf_qty / current_total_qty
        else:
            sqf_ratio = 0.0
            
        # Cap at 100% of current position
        if sqf_ratio > 1.0:
            logger.warning(f"⚠️ Target SQF quantity ({target_sqf_qty}) > Current quantity ({current_total_qty}). Capping at 100%.")
            sqf_ratio = 1.0
            
        adjusted_percentage = sqf_ratio * 100.0
        
        logger.info(f"Partial SQF for {trade_uid}:")
        logger.info(f"  - Initial Total Qty: {initial_total_qty}")
        logger.info(f"  - Current Net Qty: {current_total_qty}")
        logger.info(f"  - Target SQF Qty: {target_sqf_qty} ({percentage_of_original}% of Initial)")
        logger.info(f"  - Adjusted % of Current: {adjusted_percentage:.2f}%")

        # --- Calculate quantities for partial square off using the determined adjusted percentage ---
        legs_data_for_batching = []
        for pos in positions:
            # --- FIX: Ensure expected_price is valid before creating orders ---
            # If the price is missing from the cache, fetch it directly.
            current_price = pos.get('current_price', 0.0)
            if not current_price or current_price <= 0:
                logger.warning(f"⚠️ Missing current_price for token {pos['token']} in partial_square_off. Fetching fresh LTP.")
                from market_data import get_ltp
                current_price = await get_ltp(pos['token'], pos.get('exchange_segment'))
                if not current_price or current_price <= 0:
                    logger.error(f"❌ Could not fetch valid LTP for token {pos['token']}. Aborting partial square-off for this leg.")
                    continue # Skip this leg
            # --- END FIX ---

            current_qty = pos['quantity']
            qty_to_sqf = current_qty * (adjusted_percentage / 100.0)
            lots_to_sqf = round(qty_to_sqf / correct_lot_size) if correct_lot_size > 0 else 0
            
            if lots_to_sqf > 0:
                legs_data_for_batching.append({
                    'token': pos['token'], 'option_type': pos['option_type'],
                    'action': 'BUY' if pos['action'] == 'SELL' else 'SELL', # Reverse action
                    'total_lots': int(lots_to_sqf), 'lot_size': correct_lot_size,
                    'expected_price': current_price, 'exchange_segment': pos['exchange_segment'],
                    'product_type': pos['product_type']
                })

        if not legs_data_for_batching:
            logger.warning(f"⚠️ No orders generated for partial square-off of {trade_uid}. Quantities may be too small.")
            await loop.run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
            return {'success': True, 'message': 'Nothing to square off for the given percentage.'}

        base_lots_for_trade = max(leg['total_lots'] for leg in legs_data_for_batching) if legs_data_for_batching else 0

        all_chunks = generate_chunked_orders(
            trade_uid_prefix=f"PSQF_{trade_uid}",
            legs_data=legs_data_for_batching,
            base_lots_for_trade=base_lots_for_trade,
            chunk_divisor=7
        )
        
        config = straddle_data.get('config', {})
        buy_buffer = float(config.get('buy_buffer', 2.0))
        sell_buffer = float(config.get('sell_buffer', 2.0))
        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer

        all_successful_orders = []
        all_failed_orders = []
        all_verified_fills = []
        all_verification_failures = []
        psqf_aborted = False

        if not hasattr(state, 'temp_order_cache'):
            state.temp_order_cache = {}

        batch_execution_start = datetime.now()
        
        # --- FIX: Create an aggregated map to re-inject UIDs if missing from broker fills ---
        aggregated_app_order_id_to_uid_map = {}
        
        for chunk_idx, chunk_orders in enumerate(all_chunks, 1):
            # --- CANCELLATION CHECK ---
            if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                logger.warning(f"🛑 Partial Square-off for {trade_uid} cancelled by user during chunk execution.")                
                await loop.run_in_executor(
                    None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
                ) # Revert status
                if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
                return {'success': False, 'error': 'Cancelled by user'}
            # --- END CANCELLATION CHECK ---

            if not chunk_orders:
                continue
            
            logger.info(f"⚡ Executing PARTIAL SQF chunk {chunk_idx}/{len(all_chunks)} with {len(chunk_orders)} orders.")
            
            chunk_result = await executor.execute_batch(chunk_orders, f"PSQF_{trade_uid}_CHUNK{chunk_idx}")
            
            successful_in_chunk = chunk_result.get('successful_orders', [])
            failed_in_chunk = chunk_result.get('failed_orders', [])
            
            all_successful_orders.extend(successful_in_chunk)
            all_failed_orders.extend(failed_in_chunk)
            aggregated_app_order_id_to_uid_map.update({str(o.get('app_order_id')): o.get('uid') for o in successful_in_chunk})
            
            chunk_order_ids = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_chunk if o.get('order_id') or o.get('app_order_id')]

            verified_fills_for_chunk = []
            unverified_order_ids = list(chunk_order_ids)
            max_verification_attempts = 3 # Reduced retries

            orders_to_reexecute_in_next_chunk = []

            for attempt in range(max_verification_attempts):
                # --- CANCELLATION CHECK ---
                if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                    logger.warning(f"🛑 Partial Square-off for {trade_uid} cancelled by user during verification.")                    
                    await loop.run_in_executor(
                        None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
                    ) # Revert status
                    if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
                    return {'success': False, 'error': 'Cancelled by user'}
                # --- END CANCELLATION CHECK ---

                if not unverified_order_ids: break
                logger.info(f"📊 Verifying chunk {chunk_idx}, attempt {attempt + 1}/{max_verification_attempts} for {len(unverified_order_ids)} orders...")
                verification_result = await verify_orders_task(unverified_order_ids, f"PSQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{attempt+1}")
                if verification_result:
                    newly_verified = verification_result.get('verified_success', [])
                    newly_failed = verification_result.get('verified_failed', [])
                    verified_fills_for_chunk.extend(newly_verified)

                    verified_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_verified}
                    reexecute_ids = {str(o.get('order_id')) for o in newly_failed if o.get('status') == 'REEXECUTE_NEEDED'}
                    terminal_failure_statuses = {'REJECTED', 'CANCELLED', 'CANCELED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}
                    failed_ids = {str(o.get('order_id')) for o in newly_failed if o.get('status') in terminal_failure_statuses}
                    resolved_ids = verified_ids.union(failed_ids).union(reexecute_ids)

                    unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]
                if unverified_order_ids:
                    logger.warning(f"⚠️ {len(unverified_order_ids)} orders still pending in PARTIAL SQF chunk {chunk_idx}. Retrying in 1.0s...")
                    await asyncio.sleep(1.0)

            # Collect orders that need re-execution after all verification attempts for this chunk
            ids_to_remove_from_successful = set()
            if 'newly_failed' in locals():
                for failed_order_info in newly_failed: # newly_failed from the last verification attempt
                    if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                        order_id = str(failed_order_info.get('order_id'))
                        ids_to_remove_from_successful.add(order_id)
                        original_order_uid = aggregated_app_order_id_to_uid_map.get(order_id)
                        if original_order_uid:
                            original_order_data = next((o for o in chunk_orders if o['uid'] == original_order_uid), None)
                            if original_order_data:
                                # --- FIX: Clear the stale limit price to force recalculation ---
                                order_for_re_execution = original_order_data.copy()
                                order_for_re_execution['limit_price'] = 0.0
                                orders_to_reexecute_in_next_chunk.append(order_for_re_execution)
                                # --- END FIX ---
                                logger.info(f"🔄 Order {original_order_uid} marked for re-execution in next chunk.")
                    
                    terminal_failure_statuses = {'REJECTED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}
                    if failed_order_info.get('status') in terminal_failure_statuses:
                        logger.error(f"❌ Partial SQF Verification Failure for Order {failed_order_info.get('order_id')}: {failed_order_info.get('status')} - {failed_order_info.get('reason', 'N/A')}")
                        all_verification_failures.append(failed_order_info)

            # Correct the success tracking for this chunk
            if ids_to_remove_from_successful:
                all_successful_orders = [
                    o for o in all_successful_orders
                    if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful
                ]
                logger.info(f"Corrected success tracking: Removed {len(ids_to_remove_from_successful)} orders that were cancelled for re-execution in this chunk.")

            if unverified_order_ids:
                logger.error(f"❌ FAILED to verify all orders in PARTIAL SQF chunk {chunk_idx}. Some positions may remain open. Stopping further chunks.")
                psqf_aborted = True # Stop placing new chunks, but proceed to finalize
                break

            # --- FIX: Aggregate verified fills from all chunks ---
            all_verified_fills.extend(verified_fills_for_chunk)

            if verified_fills_for_chunk:
                # Accumulate fills in the cache. DB insertion and snapshot will happen once after all chunks.
                state.temp_order_cache.setdefault(trade_uid, []).extend(verified_fills_for_chunk)

        # If there are orders to re-execute, prepend them to the next chunk
        if orders_to_reexecute_in_next_chunk:
            if chunk_idx + 1 < len(all_chunks):
                all_chunks[chunk_idx + 1] = orders_to_reexecute_in_next_chunk + all_chunks[chunk_idx + 1]
                logger.info(f"Prepended {len(orders_to_reexecute_in_next_chunk)} orders to next chunk {chunk_idx + 2}.")
            else:
                # If this is the last chunk, create a new chunk for re-execution
                all_chunks.append(orders_to_reexecute_in_next_chunk)
                logger.info(f"Created new chunk for {len(orders_to_reexecute_in_next_chunk)} orders for re-execution.")

        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()
        total_time = (datetime.now() - start_time).total_seconds()
        
        is_successful = (len(all_failed_orders) == 0 and not psqf_aborted and not all_verification_failures)

        # --- REFACTORED FINALIZATION SEQUENCE ---
        # 1. Trigger a snapshot FIRST, while the new fills are still in the temp cache.
        # This gets the PnL pool at the exact moment of the square-off.
        await trigger_snapshot_and_broadcast(trade_uid)
        await asyncio.sleep(0.2) # small delay to allow snapshot to process

        # 2. Insert verified orders into DB
        if all_verified_fills and hasattr(state.db, 'insert_order'):
            logger.info(f"Inserting {len(all_verified_fills)} total verified partial SQF orders into DB for {trade_uid}...")
            orders_inserted_count = 0
            for fill_data in all_verified_fills:
                app_order_id = str(fill_data.get('AppOrderID') or fill_data.get('app_order_id') or fill_data.get('apporderid'))
                # Always prefer the full local UID over the potentially truncated broker UID
                if app_order_id in aggregated_app_order_id_to_uid_map:
                    fill_data['OrderUniqueIdentifier'] = aggregated_app_order_id_to_uid_map[app_order_id]
                    logger.info(f"Injected missing OrderUniqueIdentifier for {app_order_id} before DB insert.")
                if fill_data.get('OrderUniqueIdentifier'):
                    if 'order_unique_id' not in fill_data:
                        fill_data['order_unique_id'] = fill_data.get('OrderUniqueIdentifier')
                    await loop.run_in_executor(None, state.db.insert_order, fill_data)
                    orders_inserted_count += 1
            logger.info(f"✅ Inserted {orders_inserted_count} total verified partial SQF orders into DB.")
            if trade_uid in state.temp_order_cache:
                del state.temp_order_cache[trade_uid]
        
        # 3. Fetch latest trade data, calculate and persist the realized PnL.
        # --- FIX: Use the straddle_data from the start of the function, do not re-fetch from DB to avoid race conditions.
        if straddle_data:
            snapshot = state.trade_snapshots.get(trade_uid) # This snapshot was just created.
            if True: # FIX: Remove dependency on snapshot for PnL calculation
                # --- FIX: Calculate realized PnL accurately from the closing trades ---
                # The old method of taking a percentage of the total PnL pool was an approximation and incorrect.
                # This new method calculates the exact PnL of the contracts that were just closed.
                pnl_to_realize_now = 0.0
                
                # Create a map of token -> entry_price using the positions calculated BEFORE execution.
                # This ensures we have entry prices even for legs that were fully closed and removed from the snapshot.
                entry_price_map = {pos['token']: pos['entry_price'] for pos in positions}

                for fill in all_verified_fills:
                    token = int(fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id', 0))
                    if not token: continue

                    closing_price = float(fill.get('OrderAverageTradedPrice') or fill.get('fill_price') or 0.0)
                    closed_qty = int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or 0)
                    order_side = str(fill.get('OrderSide') or fill.get('order_side', '')).upper()
                    
                    original_entry_price = entry_price_map.get(token, 0.0)

                    if original_entry_price > 0 and closing_price > 0 and closed_qty > 0:
                        # Determine PnL based on the closing action
                        if order_side == 'BUY':
                            # We Bought to Close -> Original was Short (SELL)
                            # Profit = Entry - Exit
                            pnl_for_this_fill = (original_entry_price - closing_price) * closed_qty
                        elif order_side == 'SELL':
                            # We Sold to Close -> Original was Long (BUY)
                            # Profit = Exit - Entry
                            pnl_for_this_fill = (closing_price - original_entry_price) * closed_qty
                        else:
                            pnl_for_this_fill = 0.0
                        
                        pnl_to_realize_now += pnl_for_this_fill
                # --- END FIX ---
                
                # Update the original straddle_data object
                existing_realized_pnl = straddle_data.get('realized_pnl', 0.0)
                existing_psqf_percentage = straddle_data.get('psqf_percentage', 0.0)

                straddle_data['realized_pnl'] = existing_realized_pnl + pnl_to_realize_now
                straddle_data['psqf_percentage'] = existing_psqf_percentage + percentage_of_original

                # --- FIX: Persist correct lot_size to prevent snapshotter from overwriting PnL due to race condition ---
                if correct_lot_size > 0:
                    straddle_data['lot_size'] = correct_lot_size

                # --- FIX: Update global cache to prevent snapshotter from seeing stale DB data ---
                # This acts as a short-term memory until DB consistency is reached.
                if hasattr(state, 'trade_data_cache'):
                    state.trade_data_cache[trade_uid] = {'data': straddle_data, 'timestamp': datetime.now().timestamp()}
                # --- END FIX ---

                logger.info(f"💰 Booking Realized PnL for {trade_uid}:")
                logger.info(f"   - Calculated from {len(all_verified_fills)} closing fills.")
                logger.info(f"   - PnL to Realize now: ₹{pnl_to_realize_now:,.2f}")
                logger.info(f"   - Previous Realized PnL: ₹{existing_realized_pnl:,.2f}")
                logger.info(f"   - New Total Realized PnL: ₹{straddle_data['realized_pnl']:,.2f}")
                logger.info(f"   - New Total PSQF %: {straddle_data['psqf_percentage']:.2f}%")

            straddle_data['status'] = 'ACTIVE'
            
            # --- FIX: Robust DB Update with Retry for Partial Square-off ---
            saved_successfully = False
            for attempt in range(3):
                try:
                    await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
                    # Verify read-back to ensure PnL is actually saved
                    saved_doc = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                    if saved_doc and abs(saved_doc.get('realized_pnl', 0.0) - straddle_data['realized_pnl']) < 1.0:
                        saved_successfully = True
                        logger.info(f"✅ Realized PnL (₹{pnl_to_realize_now:,.2f}) persisted and verified in DB (Attempt {attempt+1}).")
                        break
                except Exception as e:
                    logger.error(f"❌ DB persistence error for {trade_uid} (Attempt {attempt+1}): {e}")
                    await asyncio.sleep(0.5)
            
            if not saved_successfully:
                logger.critical(f"🚨 CRITICAL: Failed to persist Realized PnL for {trade_uid} after 3 attempts. Data may be lost on restart.")
            # --- END FIX ---
            
            # --- NEW: Trigger a final snapshot AFTER the DB update so the UI sees the correct Realized PnL ---
            await trigger_snapshot_and_broadcast(trade_uid, trade_data=straddle_data)
            
        # --- FIX: Clear the temporary cache for this trade after the partial square-off is complete ---
        if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
            del state.temp_order_cache[trade_uid]
        # --- END FIX ---

        logger.info("="*100)
        logger.info(f"✅ PARTIAL-SQUARE-OFF_{trade_uid} COMPLETE | Time: {total_time:.2f}s")
        logger.info(f"✅ Success: {len(all_successful_orders)} | ❌ Failed: {len(all_failed_orders)}")
        logger.info("="*100)
        
        return {
            'success': is_successful,
            'successful_count': len(all_successful_orders),
            'failed_count': len(all_failed_orders),
            'execution_time': batch_execution_time,
            'trade_uid': trade_uid,
        }
        
    except Exception as e:
        logger.error(f"❌ Partial Square-off failed: {e}", exc_info=True)
        # Revert to ACTIVE so the remaining open position is monitored.
        await asyncio.get_event_loop().run_in_executor(
            None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
        )
        logger.warning(f"⚠️ Status for {trade_uid} reverted to ACTIVE after unexpected exception in partial_square_off.")
    finally:
        # --- CRITICAL FIX: Ensure the temporary cache is always cleared for this trade ---
        if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
            del state.temp_order_cache[trade_uid]
            logger.info(f"🧹 Final cleanup: Cleared temp order cache for {trade_uid} after partial square-off attempt.")
        # --- END FIX ---

async def calculate_realized_pnl(trade_uid: str, recent_fills: List[Dict] = None) -> Optional[Dict]:
    """
    Calculates the final, realized PnL for a trade by summing up the value
    of all BUY and SELL orders. This is the most accurate
    method as it accounts for all builds, hedges, and rolls.
    
    Args:
        trade_uid: The UID of the trade.
        recent_fills: An optional list of recently verified fills to merge.
                      This makes the function more robust against DB replication lag.
    """
    try:
        # 1. Get all historical orders for the trade from the database.
        loop = asyncio.get_event_loop()
        # Use the more efficient DB query to get all orders for this specific trade.
        all_db_orders = await loop.run_in_executor(
            None, state.db.get_orders_by_trade_id, trade_uid
        )
        # Filter for filled orders.
        trade_orders = [
            o for o in all_db_orders if (str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in
                                        ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED'])
        ]

        # 2. Merge with any recently executed fills that might not be in the DB yet.
        #    This is crucial for getting an accurate PnL right after an action like square-off.
        if recent_fills:
            logger.info(f"Found {len(recent_fills)} recent fills to merge for PnL calculation.")
            db_order_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')) for o in trade_orders}
            
            new_fills_from_cache = 0
            for fill in recent_fills:
                app_order_id = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid'))
                if app_order_id and app_order_id not in db_order_ids:
                    trade_orders.append(fill)
                    new_fills_from_cache += 1
            if new_fills_from_cache > 0:
                logger.info(f"Added {new_fills_from_cache} new orders from recent_fills for PnL calculation.")

        if not trade_orders: # If still no orders after merge, then there's nothing to calculate.
            logger.warning(f"No filled orders found for PnL calculation of {trade_uid}.")
            return None

        total_buy_value = 0.0
        total_sell_value = 0.0

        for order in trade_orders:
            qty = int(order.get('cumulative_quantity') or order.get('CumulativeQuantity', 0))
            price = float(order.get('order_avg_price') or order.get('OrderAverageTradedPrice', 0))
            side = str(order.get('order_side') or order.get('OrderSide', '')).upper()
            value = qty * price

            if side == 'BUY':
                total_buy_value += value
            elif side == 'SELL':
                total_sell_value += value
        
        realized_pnl = total_sell_value - total_buy_value

        return {
            'trade_uid': trade_uid,
            'total_sell_value': total_sell_value,
            'total_buy_value': total_buy_value,
            'realized_pnl': realized_pnl
        }

    except Exception as e:
        logger.error(f"❌ Realized PnL calculation failed for {trade_uid}: {e}", exc_info=True)
        return None
 

async def square_off_by_trade_uid(trade_uid: str) -> Optional[Dict]:
    """Square off by fetching from database"""
    loop = asyncio.get_event_loop()
    try:
        straddle_data = await loop.run_in_executor(
            None, state.db.get_straddle_by_id, trade_uid
        )
        
        # --- FIX: Check for any closed status ---
        # The 'trade_processes' attribute only exists on the main process's state object.
        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            logger.info(f"Dispatching SQUARE_OFF command to process for trade {trade_uid}.")
            process_info = state.trade_processes[trade_uid]
            process_info['command_q'].put({'command': 'SQUARE_OFF'})
            return {'success': True, 'message': 'Square-off command dispatched.'}
        elif hasattr(state, 'trade_processes'):
            # This means we are in the main process, but the trade process was not found.
            logger.warning(f"Process for trade {trade_uid} not found. Executing square-off in main process.")
        else:
            # This means we are in a worker process.
            logger.info(f"Executing square-off directly within worker process for {trade_uid}.")
        # --- END FIX ---

        if not straddle_data:
            logger.error(f"❌ Straddle not found: {trade_uid}")
            return None

        if str(straddle_data.get('status', '')).startswith('CLOSED'):
            logger.warning(f"⚠️  Straddle already closed with status '{straddle_data.get('status')}': {trade_uid}")
            return None
        
        return await square_off(
            trade_uid=trade_uid,
            straddle_data=straddle_data
        )
        
    except Exception as e:
        logger.error(f"❌ Square-off by trade UID failed: {e}")
        return None


async def square_off_all_active() -> Dict:
    """Square off ALL active straddles"""
    try:
        logger.info("="*100)
        logger.info("⏹️  SQUARE OFF ALL ACTIVE STRADDLES")
        logger.info("="*100)
        
        loop = asyncio.get_event_loop()
        active_straddles = await loop.run_in_executor(
            None, state.db.get_active_straddles
        )
        
        if not active_straddles:
            logger.info("ℹ️  No active straddles to square off")
            return {'success': True, 'count': 0, 'results': []}
        
        logger.info(f"📊 Found {len(active_straddles)} active straddles")
        
        results = []
        for straddle in active_straddles:
            trade_uid = straddle.get('trade_uid') or straddle.get('straddle_id')
            
            logger.info(f"⏹️  Squaring off: {trade_uid}")
            
            result = await square_off(
                trade_uid=trade_uid,
                straddle_data=straddle
            )
            
            results.append({
                'trade_uid': trade_uid,
                'success': result.get('success', False) if result else False,
                'result': result
            })
            
            # Small delay between square-offs
            await asyncio.sleep(0.5)
        
        success_count = sum(1 for r in results if r['success'])
        
        logger.info("="*100)
        logger.info(f"✅ Square-off complete: {success_count}/{len(results)} succeeded")
        logger.info("="*100)
        
        return {
            'success': success_count == len(results),
            'count': len(results),
            'success_count': success_count,
            'failed_count': len(results) - success_count,
            'results': results
        }
        
    except Exception as e:
        logger.error(f"❌ Square-off all failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'success': False, 'count': 0, 'results': []}
