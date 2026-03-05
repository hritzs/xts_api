"""
Position Roller - Execute delta-preserving rolls
"""
import asyncio
import math
import time
from datetime import datetime
from typing import Dict, Optional, List

from models.state import state
from utils.logger import logger
from utils.helpers import get_ist_now
from trading.order_batching_utils import generate_chunked_orders # NEW IMPORT
from trading.order_executor import get_order_executor
from background.tasks import verify_orders_task, trigger_snapshot_and_broadcast
from market_data import SYMBOL_CONFIG
from trading.square_off import extract_positions_from_straddle
from trading.data_client import get_option_chain_from_service
import config as app_config

_global_roll_lock = asyncio.Lock()



async def roll_position(trade_uid: str) -> Dict:
    """
    Main entry point for rolling a position.
    It checks conditions and then calls the delta-preserving execution logic.
    """
    loop = asyncio.get_event_loop()
    trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)

    if not trade:
        return {"success": False, "error": "Trade not found"}

    # The calling handler sets the status to ROLLING before calling this.
    # So we must check for ROLLING, not ACTIVE.
    if trade.get('status') != 'ROLLING':
        return {"success": False, "error": f"Cannot roll trade with status '{trade.get('status')}'"}

    logger.info(f"🔄 Roll initiated for {trade_uid}")

    # Get current spot price from snapshot for consistency
    snapshot = state.trade_snapshots.get(trade_uid)
    spot_price = snapshot.get('spot_price') if snapshot else 0

    if not spot_price or spot_price <= 0:
        return {"success": False, "error": "Could not determine spot price for roll"}

    # --- ROBUSTNESS FIX: Derive gap from SYMBOL_CONFIG ---
    symbol = trade.get("symbol", "NIFTY")
    symbol_upper = symbol.upper()
    base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
    derived_gap = SYMBOL_CONFIG.get(base_symbol, {}).get('gap') if base_symbol else None

    if derived_gap:
        gap = derived_gap
    else:
        gap = trade.get('config', {}).get('gap', 50) # Fallback
        logger.warning(f"Could not derive gap for {symbol_upper} from SYMBOL_CONFIG. Falling back to config value: {gap}.")
    # --- END FIX ---

    # Determine new ATM strike
    new_atm_strike = int(round(spot_price / gap) * gap)

    return await execute_delta_preserving_roll(trade_uid, new_atm_strike)


async def execute_delta_preserving_roll(trade_uid: str, new_atm_strike: int) -> Dict:
    """
    Executes a delta-weighted roll from an old strike to a new ATM strike.
    This logic is adapted from the TradingEngine.
    """
    logger.info(f"LOCK_TRACE: [GLOBAL_ROLL] Attempting to acquire _global_roll_lock for {trade_uid}. Lock status: {'LOCKED' if _global_roll_lock.locked() else 'UNLOCKED'}")
    async with _global_roll_lock:
        logger.info("=" * 100)
        logger.info(f"LOCK_TRACE: [GLOBAL_ROLL] Acquired _global_roll_lock for {trade_uid}")
        logger.info("=" * 100)

        loop = asyncio.get_event_loop()
        logger.info(f"DB_TRACE: [ROLL_EXEC] About to read trade for {trade_uid}")
        trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)

        if not trade:
            return {"success": False, "error": "Trade not found"}

        try:
            start_time = time.time()
            executor = get_order_executor()

            old_strike = int(trade.get("strike", 0))
            if old_strike == new_atm_strike:
                logger.warning(f"⚠️ Roll skipped: Already at target strike {new_atm_strike}")
                return {"success": True, "message": "Already at target strike"}

            # --- 1. Get Base Quantity & Market Data ---
            symbol = trade.get('symbol', 'NIFTY')
            option_chain = state.option_chains.get(symbol.upper())
            
            # --- FIX: Fallback to REST API if cache is empty ---
            if not option_chain:
                logger.info(f"🔄 Roll: Cache miss for {symbol}. Fetching from service...")
                option_chain = await get_option_chain_from_service(symbol.upper())
                if option_chain:
                    state.update_option_chain(symbol.upper(), option_chain)
            # --- END FIX ---
            
            if not option_chain:
                 return {"success": False, "error": f"Option chain for {symbol} not available"}

            # --- ROBUSTNESS FIX: Get lot_size and segment from live option chain ---
            lot_size = option_chain.get('lot_size')
            exchange_segment = option_chain.get('exchange_segment')
            if not lot_size or not exchange_segment:
                # Fallback to DB values if chain is incomplete
                lot_size = trade.get('lot_size', 65)
                exchange_segment = trade.get('exchange_segment', app_config.EXCHANGE_NSEFO)
                logger.warning(f"Using fallback lot_size ({lot_size}) and segment ({exchange_segment}) for {trade_uid}")
            # --- END FIX ---

            # --- REVISED: Calculate quantities based on actual open positions at the specific old strike ---
            logger.info(f"Calculating current open positions for old strike {old_strike} for {trade_uid}...")
            all_current_positions = await extract_positions_from_straddle(trade)
            if not all_current_positions:
                return {"success": False, "error": "Could not determine current open positions for roll."}

            # Find the specific quantities for the old strike legs that need to be closed.
            old_ce_pos = next((p for p in all_current_positions if p.get('strike') == old_strike and p.get('option_type') == 'CE'), None)
            old_pe_pos = next((p for p in all_current_positions if p.get('strike') == old_strike and p.get('option_type') == 'PE'), None)

            # The quantity to close is the exact open quantity of that leg.
            buy_old_ce_raw = old_ce_pos.get('quantity', 0) if old_ce_pos else 0
            buy_old_pe_raw = old_pe_pos.get('quantity', 0) if old_pe_pos else 0

            if buy_old_ce_raw <= 0 and buy_old_pe_raw <= 0:
                return {"success": False, "error": f"No open positions found at the old strike {old_strike} to roll."}

            # The "base" quantity for opening the new position is the average of the two legs we are closing.
            base_qty = (buy_old_ce_raw + buy_old_pe_raw) / 2.0
            current_total_quantity = buy_old_ce_raw + buy_old_pe_raw
            if not current_total_quantity > 0:
                return {"success": False, "error": f"Invalid calculated total_quantity for roll: {current_total_quantity}"}
            base_lots_for_trade = int(base_qty // lot_size) if lot_size > 0 else 0 # This is the 'lots' parameter for generate_chunked_orders
            logger.info(f"Rolling position for {trade_uid}: Old Strike: {old_strike}, New ATM Strike: {new_atm_strike}, Base Qty (per leg): {base_qty}")

            expiry = trade.get('expiry')
            spot_price = state.trade_snapshots.get(trade_uid, {}).get('spot_price', 0)
            dte_days = state.trade_snapshots.get(trade_uid, {}).get('days_to_expiry', 1)
            dte_years = dte_days / 365.0

            if not all([expiry, spot_price > 0, dte_years > 0]):
                return {"success": False, "error": "Missing critical data for roll (expiry, spot, dte)"}

            # --- 2. Resolve Tokens and Get LTPs ---

            def get_token_and_ltp(strike, opt_type):
                for row in option_chain.get('chain', []):
                    if row['strike'] == strike:
                        return row.get(f'{opt_type.lower()}_token'), row.get(f'{opt_type.lower()}_ltp', 0)
                return None, 0

            old_ce_token, old_ce_ltp = get_token_and_ltp(old_strike, 'CE')
            old_pe_token, old_pe_ltp = get_token_and_ltp(old_strike, 'PE')
            new_ce_token, new_ce_ltp = get_token_and_ltp(new_atm_strike, 'CE')
            new_pe_token, new_pe_ltp = get_token_and_ltp(new_atm_strike, 'PE')

            if not all([old_ce_token, old_pe_token, new_ce_token, new_pe_token]):
                return {"success": False, "error": "Could not resolve all option tokens for roll"}

            # --- 3. OPTIMIZATION: Extract Deltas directly from the cached Option Chain ---
            logger.info("Extracting deltas directly from cached option chain...")

            def get_delta_from_chain(strike, opt_type):
                for row in option_chain.get('chain', []):
                    if row['strike'] == strike:
                        # The delta in the chain is already the correct signed value
                        return row.get(f'{opt_type.lower()}_delta', 0.0)
                logger.warning(f"Could not find delta for {strike} {opt_type} in chain.")
                return 0.0

            old_ce_delta = abs(get_delta_from_chain(old_strike, 'CE'))
            old_pe_delta = abs(get_delta_from_chain(old_strike, 'PE'))
            new_ce_delta = abs(get_delta_from_chain(new_atm_strike, 'CE'))
            new_pe_delta = abs(get_delta_from_chain(new_atm_strike, 'PE'))

            if not all([old_ce_delta > 0, old_pe_delta > 0, new_ce_delta > 0, new_pe_delta > 0]):
                logger.error("Could not extract all deltas from the option chain. Aborting roll.")
                # Revert status to ACTIVE so it can be re-evaluated.
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                return {"success": False, "error": "Failed to extract deltas from option chain."}
            # --- END OPTIMIZATION ---

            logger.info(f" Deltas - Old CE: {old_ce_delta:.4f}, Old PE: {old_pe_delta:.4f}, New CE: {new_ce_delta:.4f}, New PE: {new_pe_delta:.4f}")
            # --- 4. Calculate Delta-Weighted Quantities ---

            # Close old strike (closing): Quantities are now delta-weighted based on user logic.
            total_old_delta = old_ce_delta + old_pe_delta
            buy_old_ce_raw = base_qty * (old_pe_delta / total_old_delta) * 2 if total_old_delta > 0 else base_qty
            buy_old_pe_raw = base_qty * (old_ce_delta / total_old_delta) * 2 if total_old_delta > 0 else base_qty

            # SELL new strike (opening): weighted by OPPOSITE delta
            total_new_delta = new_ce_delta + new_pe_delta
            sell_new_ce_raw = base_qty * (new_pe_delta / total_new_delta) * 2 if total_new_delta > 0 else base_qty
            sell_new_pe_raw = base_qty * (new_ce_delta / total_new_delta) * 2 if total_new_delta > 0 else base_qty
            
            # Round to nearest lot size
            buy_old_ce_qty = int(round(buy_old_ce_raw / lot_size) * lot_size)
            buy_old_pe_qty = int(round(buy_old_pe_raw / lot_size) * lot_size)
            sell_new_ce_qty = int(round(sell_new_ce_raw / lot_size) * lot_size)
            sell_new_pe_qty = int(round(sell_new_pe_raw / lot_size) * lot_size)

            logger.info(f"Calculated Roll Quantities (rounded to lot size {lot_size}):")
            logger.info(f"   BUY Old CE: {buy_old_ce_qty} (raw: {buy_old_ce_raw:.2f})")
            logger.info(f"   BUY Old PE: {buy_old_pe_qty} (raw: {buy_old_pe_raw:.2f})")
            logger.info(f"   SELL New CE: {sell_new_ce_qty} (raw: {sell_new_ce_raw:.2f})")
            logger.info(f"   SELL New PE: {sell_new_pe_qty} (raw: {sell_new_pe_raw:.2f})")

            # --- 5. Create Lot-by-Lot Orders for Execution ---
            buy_old_ce_lots = buy_old_ce_qty // lot_size
            buy_old_pe_lots = buy_old_pe_qty // lot_size
            sell_new_ce_lots = sell_new_ce_qty // lot_size
            sell_new_pe_lots = sell_new_pe_qty // lot_size

            legs_data_for_batching = []
            product_type = trade.get('product_type', 'MIS')

            if buy_old_ce_lots > 0:
                legs_data_for_batching.append({
                    'token': old_ce_token, 'option_type': 'CE', 'action': 'BUY',
                    'total_lots': buy_old_ce_lots, 'lot_size': lot_size,
                    'expected_price': old_ce_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
                })
            if buy_old_pe_lots > 0:
                legs_data_for_batching.append({
                    'token': old_pe_token, 'option_type': 'PE', 'action': 'BUY',
                    'total_lots': buy_old_pe_lots, 'lot_size': lot_size,
                    'expected_price': old_pe_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
                })
            if sell_new_ce_lots > 0:
                legs_data_for_batching.append({
                    'token': new_ce_token, 'option_type': 'CE', 'action': 'SELL',
                    'total_lots': sell_new_ce_lots, 'lot_size': lot_size,
                    'expected_price': new_ce_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
                })
            if sell_new_pe_lots > 0:
                legs_data_for_batching.append({
                    'token': new_pe_token, 'option_type': 'PE', 'action': 'SELL',
                    'total_lots': sell_new_pe_lots, 'lot_size': lot_size,
                    'expected_price': new_pe_ltp, 'exchange_segment': exchange_segment, 'product_type': product_type
                })
            all_chunks = generate_chunked_orders(
                trade_uid_prefix=f"ROLL_{trade_uid}",
                legs_data=legs_data_for_batching,
                base_lots_for_trade=base_lots_for_trade,
                # Process in ~15% chunks (100/7) to reduce verification cycles and speed up execution.
                chunk_divisor=7
            )
            
            # --- Inject limit order buffer from config into each order ---
            config = trade.get('config', {})
            buy_buffer = float(config.get('buy_buffer', 2.0))
            sell_buffer = float(config.get('sell_buffer', 2.0))
            for chunk in all_chunks:
                for order in chunk:
                    if order.get('action', '').upper() == 'BUY':
                        order['limit_order_buffer'] = buy_buffer
                    else:
                        order['limit_order_buffer'] = sell_buffer
            # --- END INJECTION ---

            logger.info(f"Generated {len(all_chunks)} chunks for roll execution.")

            logger.info("="*100)
            logger.info(f"⚡ EXECUTING ROLL: {trade_uid}")
            logger.info(f"   Old: {old_strike} -> New: {new_atm_strike}")
            for leg in legs_data_for_batching:
                logger.info(
                    f"   - {leg['action']} {leg['total_lots']} lots of {leg['option_type']} "
                    f"(Token: {leg['token']})"
                )
            logger.info(f"   Total chunks to execute: {len(all_chunks)}")
            logger.info("="*100)

            all_successful_orders = []
            all_failed_orders = []
            all_verified_fills = []
            all_verification_failures = []
            roll_aborted = False

            if not hasattr(state, 'temp_order_cache'):
                state.temp_order_cache = {}

            for chunk_idx, chunk_orders in enumerate(all_chunks, 1):
                if not chunk_orders:
                    continue

                # --- REFACTORED: Intra-chunk retry loop with more robust failure handling ---
                orders_to_process_in_chunk = list(chunk_orders)
                chunk_attempt = 0
                max_chunk_retries = 2  # Allow up to 2 retries for a failed chunk
 
                while orders_to_process_in_chunk and chunk_attempt <= max_chunk_retries:
                    if chunk_attempt > 0:
                        logger.info(f"🔄 Re-executing {len(orders_to_process_in_chunk)} orders within ROLL chunk {chunk_idx} (Attempt {chunk_attempt + 1})...")
                        await asyncio.sleep(2.0)  # Wait before retrying

                        # Increase buffer and clear price for re-calculation
                        for order in orders_to_process_in_chunk:
                            original_buffer = order.get('limit_order_buffer', 2.0)
                            order['limit_order_buffer'] = original_buffer + 4.0
                            order['limit_price'] = 0.0  # Force recalculation
                            logger.info(f"   -> For UID {order['uid']}, new buffer is {order['limit_order_buffer']:.1f}")

                    chunk_result = await executor.execute_batch(
                        orders_to_process_in_chunk, f"ROLL_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt + 1}"
                    )

                    successful_in_attempt = chunk_result.get('successful_orders', [])
                    failed_in_attempt = chunk_result.get('failed_orders', [])
 
                    all_successful_orders.extend(successful_in_attempt)
                    all_failed_orders.extend(failed_in_attempt) # Track placement failures

                    attempt_order_ids = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_attempt if o.get('order_id') or o.get('app_order_id')]
                    app_order_id_to_uid_map_attempt = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_attempt}

                    # --- VERIFICATION FOR THIS ATTEMPT ---
                    verified_fills_for_attempt = []
                    unverified_order_ids = list(attempt_order_ids)
                    max_verification_attempts = 3
                    ids_to_remove_from_successful_attempt = set()
                    orders_to_reprocess = []

                    for verification_attempt in range(max_verification_attempts):
                        if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                            logger.warning(f"🛑 Roll for {trade_uid} cancelled by user during verification.")
                            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                            if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
                            return {"success": False, "error": "Cancelled by user"}

                        if not unverified_order_ids: break

                        logger.info(f"📊 Verifying ROLL chunk {chunk_idx} (Attempt {chunk_attempt+1}), verification {verification_attempt + 1}/{max_verification_attempts} for {len(unverified_order_ids)} orders...")
                        verification_result = await verify_orders_task(unverified_order_ids, f"ROLL_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt+1}_VERIFY{verification_attempt+1}")

                        if verification_result:
                            newly_verified = verification_result.get('verified_success', [])
                            newly_failed = verification_result.get('verified_failed', [])
                            verified_fills_for_attempt.extend(newly_verified)
 
                            # --- MODIFIED: Handle terminal failures by retrying, not aborting ---
                            verified_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_verified}

                            # Define statuses that should trigger a re-execution of the order.
                            REEXECUTE_STATUSES = {'REEXECUTE_NEEDED', 'CANCELLED', 'CANCELED', 'REJECTED'}
                            reexecute_ids = {str(o.get('order_id')) for o in newly_failed if o.get('status') in REEXECUTE_STATUSES}

                            # Define statuses that are truly terminal and should abort the roll.
                            ABORT_STATUSES = {'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}
                            abort_failures = [f for f in newly_failed if f.get('status') in ABORT_STATUSES]
                            if abort_failures:
                                for f in abort_failures:
                                    logger.error(f"❌ Unrecoverable Roll Failure for Order {f.get('order_id')}: {f.get('status')}. Aborting roll.")
                                all_verification_failures.extend(abort_failures)
                                roll_aborted = True
                                break

                            failed_ids_to_resolve = {str(o.get('order_id')) for o in newly_failed if o.get('status') in ABORT_STATUSES}
                            resolved_ids = verified_ids.union(failed_ids_to_resolve).union(reexecute_ids)

                            unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]

                        if unverified_order_ids:
                            logger.warning(f"⚠️ {len(unverified_order_ids)} orders still pending in ROLL chunk {chunk_idx}. Retrying verification in 1.0s...")
                            await asyncio.sleep(1.0)
                    
                    if roll_aborted: break

                    # After verification, find orders to re-execute in the same chunk
                    if 'newly_failed' in locals():
                        for failed_order_info in newly_failed: # newly_failed from the last verification attempt
                            if failed_order_info.get('status') in REEXECUTE_STATUSES:
                                ids_to_remove_from_successful_attempt.add(str(failed_order_info.get('order_id')))
                                order_id = str(failed_order_info.get('order_id'))
                                original_order_uid = app_order_id_to_uid_map_attempt.get(order_id)
                                if original_order_uid:
                                    original_order_data = next((o for o in orders_to_process_in_chunk if o['uid'] == original_order_uid), None)
                                    if original_order_data:
                                        orders_to_reprocess.append(original_order_data.copy())
                                        logger.info(f"🔄 Order {original_order_uid} (Status: {failed_order_info.get('status')}) marked for re-execution in this chunk.")

                    # Correct the success tracking for this attempt
                    if ids_to_remove_from_successful_attempt:
                        all_successful_orders = [
                            o for o in all_successful_orders
                            if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful_attempt
                        ]
                        logger.info(f"Corrected success tracking: Removed {len(ids_to_remove_from_successful_attempt)} orders that were cancelled for re-execution in this attempt.")

                    # Update the list for the next `while` loop iteration
                    orders_to_process_in_chunk = orders_to_reprocess

                    # Aggregate verified fills and cache them
                    all_verified_fills.extend(verified_fills_for_attempt)
                    if verified_fills_for_attempt:
                        state.temp_order_cache.setdefault(trade_uid, []).extend(verified_fills_for_attempt)
                        logger.info(f"Cached {len(verified_fills_for_attempt)} roll orders from chunk {chunk_idx} attempt {chunk_attempt+1}.")

                    chunk_attempt += 1
                
                if roll_aborted: break

                # After all attempts for a chunk, if any orders are still unverified or failed re-execution, abort.
                if unverified_order_ids or orders_to_process_in_chunk or all_verification_failures:
                    if unverified_order_ids:
                        logger.error(f"❌ FAILED to verify {len(unverified_order_ids)} orders in ROLL chunk {chunk_idx} after all retries.")
                    if orders_to_process_in_chunk:
                        logger.error(f"❌ FAILED to re-execute {len(orders_to_process_in_chunk)} orders in ROLL chunk {chunk_idx}.")
                        all_failed_orders.extend(orders_to_process_in_chunk) # Add final failed-to-retry orders
                    if all_verification_failures:
                        logger.error(f"❌ Detected {len(all_verification_failures)} terminal verification failures in ROLL chunk {chunk_idx}.")
                    logger.error("Stopping further roll chunks due to failures.")
                    roll_aborted = True
                    break

                # Small delay between chunks
                if chunk_idx < len(all_chunks) - 1:
                    await asyncio.sleep(0.05) # Tiny delay

            # --- Insert all verified orders ONCE after all chunks ---
            all_verified_fills_from_cache = state.temp_order_cache.get(trade_uid, [])
            if all_verified_fills_from_cache:
                if hasattr(state.db, 'insert_order'):
                    logger.info(f"Inserting {len(all_verified_fills_from_cache)} total verified roll orders into DB for {trade_uid}...")
                    orders_inserted_count = 0
                    for fill_data in all_verified_fills_from_cache:
                        # The UID should be in the fill data from verification
                        if fill_data.get('OrderUniqueIdentifier'):
                            await loop.run_in_executor(None, state.db.insert_order, fill_data)
                            orders_inserted_count += 1
                    logger.info(f"✅ Inserted {orders_inserted_count} total verified roll orders into DB.")
                # Clean up the cache for this trade after processing
                if trade_uid in state.temp_order_cache:
                    del state.temp_order_cache[trade_uid]

            # --- UPDATE TRADE STRIKE ---
            # After a successful roll, update the primary strike of the trade record.
            if not all_failed_orders and not roll_aborted:
                try:
                    if hasattr(state.db, 'update_straddle_strike'):
                        logger.info(f"DB_TRACE: [ROLL_EXEC] About to update strike to {new_atm_strike} for {trade_uid}")
                        await loop.run_in_executor(None, state.db.update_straddle_strike, trade_uid, new_atm_strike)
                        logger.info(f"DB_TRACE: [ROLL_EXEC] Finished strike update for {trade_uid}")
                    else:
                        logger.warning(f"DB method 'update_straddle_strike' not found. Strike for {trade_uid} not updated in DB.")
                except Exception as e:
                    logger.error(f"Failed to update strike for {trade_uid} after roll: {e}")
            else:
                logger.warning(f"⚠️ Roll for {trade_uid} was partial or failed. Strike will not be updated.")

            # The snapshotter now correctly reconstructs the full position from the order history,
            # so an immediate, manual recalculation of greeks here is no longer necessary.
            logger.info(f"Greeks for {trade_uid} will be updated in the next snapshot.")


            total_time = time.time() - start_time
            logger.info(f"✅ Roll for {trade_uid} complete in {total_time:.2f}s")

            is_fully_successful = not all_failed_orders and not roll_aborted
            message = f"Roll from {old_strike} to {new_atm_strike} completed."
            if not is_fully_successful:
                message = f"PARTIAL roll from {old_strike} to {new_atm_strike} attempted. Position may be mixed."
                logger.warning(message)

            # --- UI UPDATE: Send a single, final snapshot to the UI after the entire roll is complete. ---
            await trigger_snapshot_and_broadcast(trade_uid)
            logger.info(f"✅ Final snapshot for {trade_uid} broadcasted to UI after roll.")

            return {
                "success": True, # Always return True to allow TradeManager to set status to ACTIVE
                "message": message,
                "execution_time": total_time, # Keep execution_time for debugging/metrics
                "successful_orders": len(all_successful_orders),
                "failed_orders": len(all_failed_orders)
            }

        except asyncio.CancelledError:
            logger.critical(f"🛑 Roll for {trade_uid} was cancelled abruptly. Reverting status to ACTIVE to prevent a stale state.")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            # Re-raise the cancellation error so the task actually stops.
            raise
        except Exception as e:
            logger.error(f"❌ Roll execution failed for {trade_uid}: {e}", exc_info=True)
            # Revert status to ACTIVE so it can be re-evaluated.
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {"success": False, "error": str(e)}
        finally:
            logger.info(f"LOCK_TRACE: [GLOBAL_ROLL] Exiting locked section for {trade_uid}. Lock will be released automatically by 'async with'.")
            # --- CRITICAL FIX: Ensure the temporary cache is always cleared for this trade ---
            if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
                del state.temp_order_cache[trade_uid]
                logger.info(f"🧹 Final cleanup: Cleared temp order cache for {trade_uid} after roll attempt.")
            # --- END FIX ---