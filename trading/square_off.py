"""
Square Off - Close positions with 1-lot-per-leg batching
Evenly distributes legs across batches
Verification happens as background task
"""
import asyncio
import re
from typing import Dict, List, Optional
from datetime import datetime
from utils.logger import logger
from models.state import state
from trading.order_executor import get_order_executor # Removed create_batch_orders
from market_data import SYMBOL_CONFIG, get_ltp_from_service
from trading.order_batching_utils import generate_chunked_orders
from background.tasks import trigger_snapshot_and_broadcast
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
    straddle_data: Dict = None,
    reason: str = None
) -> Optional[Dict]:
    """
    ⚡ SQUARE OFF - Close positions with chunked order execution.
    Verification happens after each chunk.
    """
    start_time = datetime.now()

    if not hasattr(state, 'closing_trades'):
        state.closing_trades = set()
    
    state.closing_trades.add(trade_uid)
    logger.info(f"ℹ️  Trade {trade_uid} marked as 'closing' to prevent concurrent actions.")

    try:
        loop = asyncio.get_event_loop()

        if not straddle_data:
            straddle_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            if not straddle_data:
                logger.error(f"❌ Could not find straddle data for {trade_uid} in DB. Aborting square_off.")
                return {'success': False, 'error': 'Trade data not found in DB'}

        current_status = straddle_data.get('status')
        if current_status == 'SQUARING-OFF':
            logger.warning(f"⚠️ Square-off for {trade_uid} is already in progress. Ignoring duplicate request.")
            return {'success': False, 'error': 'Square-off already in progress.'}
        
        allowed_statuses = ['ACTIVE', 'BUILDING', 'ROLLING']
        if current_status not in allowed_statuses:
            logger.warning(f"⚠️ Cannot square off trade {trade_uid} with status '{current_status}'.")
            return {'success': False, 'error': f'Trade status is {current_status}, not in {allowed_statuses}.'}

        from trading.trade_manager import get_trade_manager
        manager = get_trade_manager(trade_uid)
        if manager:
            logger.info(f"🛑 Stopping all monitors for {trade_uid} before square-off execution.")
            await manager.stop_monitoring()
        else:
            logger.warning(f"⚠️ Could not find TradeManager for {trade_uid} to stop monitors, but proceeding with square-off.")

        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'SQUARING-OFF')
        logger.info(f"🔄 Status updated: {trade_uid} -> SQUARING-OFF (monitors will now pause)")

        await _neutralize_delta_before_square_off(trade_uid)

        if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
            logger.warning(f"🛑 Square-off for {trade_uid} cancelled by user after pre-sqf hedge.")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            if trade_uid in state.cancellation_flags:
                del state.cancellation_flags[trade_uid]
            if manager:
                await manager.start_monitoring()
            return {'success': False, 'error': 'Cancelled by user'}

        executor = get_order_executor()
        if not executor:
            logger.error("❌ OrderExecutor not initialized")
            return None
        
        logger.info("="*100)
        logger.info(f"⏹️  SQUARE OFF | Trade UID: {trade_uid}")
        logger.info("="*100)

        await executor.cancel_all_open_orders_for_trade(trade_uid)

        if not positions:
            positions = await extract_positions_from_straddle(straddle_data)
        
        if not positions:
            logger.error("❌ No positions to square off")
            try:                
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                logger.info(f"🔄 Reverted status for {trade_uid} to 'ACTIVE' as no positions were found.")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to revert status for {trade_uid}. Trade may be stuck in SQUARING-OFF. Error: {e}")
            return {'success': False, 'error': 'No positions to square off'}
        
        correct_lot_size = await get_correct_lot_size(straddle_data)
        logger.info(f"✅ Verified lot_size: {correct_lot_size}")
        
        position_summary = analyze_positions(positions, correct_lot_size)
        logger.info(f"📊 Position summary: {len(position_summary)} legs")
        
        lots_list = []
        leg_names = []
        total_lots_across_all_legs = 0
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
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                logger.info(f"🔄 Reverted status for {trade_uid} to 'ACTIVE' as no valid legs were found.")
            except Exception as e:
                logger.error(f"CRITICAL: Failed to revert status for {trade_uid}. Trade may be stuck in SQUARING-OFF. Error: {e}")
            return {'success': False, 'error': 'No valid legs to square off'}

        base_lots_for_trade = max(leg['lots'] for leg in position_summary) if position_summary else 0
        if base_lots_for_trade == 0:
            logger.warning("⚠️ Base lots for trade is 0, defaulting min_lots_per_order to 1 for square-off.")
            
        legs_data_for_batching = []
        for leg in position_summary:
            legs_data_for_batching.append({
                'token': leg['token'],
                'option_type': leg['option_type'],
                'action': 'BUY' if leg['action'] == 'SELL' else 'SELL',
                'total_lots': leg['lots'],
                'lot_size': leg['lot_size'],
                'expected_price': leg['current_price'],
                'exchange_segment': leg['exchange_segment'],
                'product_type': leg['product_type']
            })
            
        symbol_upper = straddle_data.get('symbol', 'NIFTY').upper()
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        max_order_qty = SYMBOL_CONFIG.get(base_symbol, {}).get('max_order_qty', 1800) if base_symbol else 1800
        
        config_trade = straddle_data.get('config', {})
        
        if reason == 'SL':
            logger.info(f"🚨 SL hit for {trade_uid}: Using aggressive (max lot/order) execution.")

        # ── MAIN CHUNK GENERATION ─────────────────────────────────────────────
        all_chunks = generate_chunked_orders(
            trade_uid_prefix    = f"SQF_{trade_uid}",
            legs_data           = legs_data_for_batching,
            base_lots_for_trade = base_lots_for_trade,
            max_order_qty       = max_order_qty,
            aggressive          = (reason == 'SL'),
        )
        
        for chunk_idx, chunk in enumerate(all_chunks, 1):
            chunk_summary = []
            for order in chunk:
                chunk_summary.append(f"{order['action']} {order['quantity'] // order['lot_size']} lots {order['option_type']} {order['token']}")
            logger.info(f"   Chunk {chunk_idx:2d}: {', '.join(chunk_summary)}")
        
        config = straddle_data.get('config', {})
        symbol_upper = straddle_data.get('symbol', 'NIFTY').upper()
        
        if reason == 'SL':
            base_sl_buffer  = 10.0 if "SENSEX" in symbol_upper else 5.0
            final_sl_buffer = base_sl_buffer * 2
            buy_buffer      = final_sl_buffer
            sell_buffer     = final_sl_buffer
            logger.info(f"🚨 SL Square-off detected. Using aggressive buffer: {final_sl_buffer} (Base {base_sl_buffer} * 2)")
        else:
            default_buffer = 6.0 if "SENSEX" in symbol_upper else 2.0
            buy_buffer     = float(config.get('buy_buffer', default_buffer))
            sell_buffer    = float(config.get('sell_buffer', default_buffer))
            
        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer

        logger.info("="*100)
        logger.info(f"🔄 CHUNKED EXECUTION PLAN (min_lots_per_order based on max leg lots)")
        logger.info(f"   Total chunks: {len(all_chunks)}")
        logger.info(f"   Legs: {', '.join(leg_names)}")
        logger.info("="*100)
        
        all_successful_orders    = []
        all_failed_orders        = []
        all_verified_fills       = []
        all_verification_failures = []
        sqf_aborted              = False

        if not hasattr(state, 'temp_order_cache'):
            state.temp_order_cache = {}

        batch_execution_start = datetime.now()
        
        aggregated_app_order_id_to_uid_map = {}
        
        # ═══════════════════════════════════════════════════════════════════════
        # MAIN CHUNK EXECUTION LOOP
        # ═══════════════════════════════════════════════════════════════════════
        for chunk_idx, chunk_orders in enumerate(all_chunks, 1):
            if not chunk_orders:
                continue

            orders_to_process_in_chunk = list(chunk_orders)
            verified_fills_for_chunk   = []
            chunk_attempt              = 0
            MAX_REEXECUTE_ATTEMPTS     = 3

            while orders_to_process_in_chunk and chunk_attempt < MAX_REEXECUTE_ATTEMPTS:
                buffer_multiplier = chunk_attempt + 1

                if chunk_attempt > 0:
                    logger.info(f"🔄 Re-executing {len(orders_to_process_in_chunk)} orders within chunk {chunk_idx} (Attempt {chunk_attempt + 1}) | buffer={buffer_multiplier}x...")
                    await asyncio.sleep(0.5)

                    for order in orders_to_process_in_chunk:
                        action = order.get('action', '').upper()
                        base   = buy_buffer if action == 'BUY' else sell_buffer
                        order['limit_order_buffer'] = base * buffer_multiplier
                        order['limit_price']        = 0.0
                        
                        import time
                        old_uid  = order.get('uid', '')
                        base_uid = old_uid.split('_TRY')[0]
                        order['uid'] = f"{base_uid}_TRY{chunk_attempt}_{int(time.time()*1000)%10000}"[:20]

                logger.info(f"⚡ Executing SQUARE OFF chunk {chunk_idx}/{len(all_chunks)} (Re-Exec Attempt {chunk_attempt + 1}) with {len(orders_to_process_in_chunk)} orders.")

                chunk_result = await executor.execute_batch(
                    orders_to_process_in_chunk, f"SQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt + 1}"
                )

                successful_in_attempt = chunk_result.get('successful_orders', [])
                failed_placements     = chunk_result.get('failed_orders', [])

                failed_placement_uids       = {f['uid'] for f in failed_placements}
                placement_failures_to_retry = [
                    o for o in orders_to_process_in_chunk if o['uid'] in failed_placement_uids
                ]

                all_successful_orders.extend(successful_in_attempt)
                aggregated_app_order_id_to_uid_map.update({str(o.get('app_order_id')): o.get('uid') for o in successful_in_attempt})

                if successful_in_attempt:
                    for ord_data in successful_in_attempt:
                        try:
                            db_order = {
                                'AppOrderID':            str(ord_data.get('app_order_id')),
                                'OrderUniqueIdentifier': ord_data.get('uid'),
                                'order_unique_id':       ord_data.get('uid'),
                                'ExchangeInstrumentID':  ord_data.get('token'),
                                'OrderSide':             ord_data.get('action'),
                                'OrderQuantity':         ord_data.get('quantity'),
                                'LeavesQuantity':        ord_data.get('quantity'),
                                'CumulativeQuantity':    0,
                                'OrderStatus':           'OPEN',
                                'ProductType':           ord_data.get('product_type', 'MIS'),
                                'trade_uid':             trade_uid
                            }
                            await loop.run_in_executor(None, state.db.insert_order, db_order)
                        except Exception as ins_e:
                            logger.error(f"⚠️ Failed to persist placed order {ord_data.get('app_order_id')} to DB: {ins_e}")

                attempt_order_ids            = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_attempt if o.get('order_id') or o.get('app_order_id')]
                app_order_id_to_uid_map_attempt = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_attempt}

                unverified_order_ids                   = list(attempt_order_ids)
                max_verification_attempts              = 3
                orders_to_reexecute_in_this_chunk      = []
                ids_to_remove_from_successful_attempt  = set()
                newly_failed                           = []

                for verification_attempt in range(max_verification_attempts):
                    if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                        logger.warning(f"🛑 Square-off for {trade_uid} cancelled by user during verification.")
                        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                        if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
                        if manager: await manager.start_monitoring()
                        return {'success': False, 'error': 'Cancelled by user'}

                    if not unverified_order_ids:
                        break

                    logger.info(f"📊 Verifying chunk {chunk_idx} (Attempt {chunk_attempt+1}), verification {verification_attempt + 1}/{max_verification_attempts} for {len(unverified_order_ids)} orders...")
                    verification_result = await executor.verify_orders_bulk(
                        unverified_order_ids,
                        f"SQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt+1}_VERIFY{verification_attempt+1}",
                        trade_uid=trade_uid
                    )

                    if verification_result:
                        newly_verified = verification_result.get('verified_success', [])
                        newly_failed   = verification_result.get('verified_failed', [])
                        verified_fills_for_chunk.extend(newly_verified)

                        verified_ids   = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_verified}
                        reexecute_ids  = {str(o.get('order_id')) for o in newly_failed if o.get('status') == 'REEXECUTE_NEEDED'}
                        terminal_failure_statuses = {'REJECTED', 'CANCELLED', 'CANCELED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}
                        failed_ids     = {str(o.get('order_id')) for o in newly_failed if o.get('status') in terminal_failure_statuses}
                        resolved_ids   = verified_ids.union(failed_ids).union(reexecute_ids)

                        unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]

                        if unverified_order_ids:
                            logger.warning(f"⚠️ {len(unverified_order_ids)} orders still pending in chunk {chunk_idx}. Retrying verification in 0.5s...")
                            await asyncio.sleep(0.5)

                for failed_order_info in newly_failed:
                    if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                        ids_to_remove_from_successful_attempt.add(str(failed_order_info.get('order_id')))
                        order_id           = str(failed_order_info.get('order_id'))
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

                if ids_to_remove_from_successful_attempt and all_successful_orders:
                    all_successful_orders = [
                        o for o in all_successful_orders
                        if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful_attempt
                    ]
                    logger.info(f"Corrected success tracking: Removed {len(ids_to_remove_from_successful_attempt)} orders that were cancelled for re-execution in this attempt.")

                orders_to_process_in_chunk = orders_to_reexecute_in_this_chunk + placement_failures_to_retry
                chunk_attempt += 1

            if orders_to_process_in_chunk:
                logger.error(f"❌ FAILED to execute {len(orders_to_process_in_chunk)} orders in chunk {chunk_idx} after all retries. Marking as failed.")
                all_failed_orders.extend(orders_to_process_in_chunk)

            if unverified_order_ids:
                logger.error(f"❌ FAILED to verify {len(unverified_order_ids)} orders in chunk {chunk_idx} after all retries. Marking as failed.")
                for oid in unverified_order_ids:
                    all_failed_orders.append({'uid': 'unknown', 'app_order_id': oid, 'error': 'Verification timed out'})

            all_verified_fills.extend(verified_fills_for_chunk)
            if verified_fills_for_chunk:
                state.temp_order_cache.setdefault(trade_uid, []).extend(verified_fills_for_chunk)

        # ═══════════════════════════════════════════════════════════════════════
        # FINAL SWEEP — escalating buffer multiples
        # ═══════════════════════════════════════════════════════════════════════

        # ── Helper: recompute exactly what's still unfilled from verified fills.
        # Must be called AFTER each sweep attempt so the next iteration uses
        # updated fill data and doesn't re-send already-filled quantity.
        def _compute_unfilled_legs(verified_fills):
            filled_qty_map = {}
            for fill in verified_fills:
                token = int(fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id') or 0)
                qty   = int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or 0)
                filled_qty_map[token] = filled_qty_map.get(token, 0) + qty

            legs = []
            for token, target in target_qty_map.items():
                filled    = filled_qty_map.get(token, 0)
                remaining = target - filled
                if remaining > 0:
                    original_leg = next((p for p in position_summary if p['token'] == token), None)
                    if original_leg:
                        lot_size = original_leg.get('lot_size', 65)
                        lots     = remaining // lot_size if lot_size > 0 else 0
                        if lots > 0:
                            legs.append({
                                'token':            token,
                                'option_type':      original_leg['option_type'],
                                'action':           'BUY' if original_leg['action'] == 'SELL' else 'SELL',
                                'total_lots':       lots,
                                'lot_size':         lot_size,
                                'expected_price':   original_leg['current_price'],
                                'exchange_segment': original_leg['exchange_segment'],
                                'product_type':     original_leg['product_type']
                            })
            return legs

        target_qty_map     = {p['token']: p['total_quantity'] for p in position_summary}
        unfilled_legs_data = _compute_unfilled_legs(all_verified_fills)  # ← FIX: computed fresh

        if unfilled_legs_data:
            logger.warning(
                f"⚠️  After main chunks: "
                f"{sum(l['total_lots'] for l in unfilled_legs_data)} lots unfilled across "
                f"{len(unfilled_legs_data)} legs. Entering sweep loop..."
            )

        max_sweep_attempts = 3
        sweep_attempt      = 0

        while unfilled_legs_data and not sqf_aborted and sweep_attempt < max_sweep_attempts:
            sweep_attempt    += 1
            sweep_multiplier  = sweep_attempt + 1

            logger.info(
                f"🧹 Final Sweep (Attempt {sweep_attempt}/{max_sweep_attempts}) | "
                f"{len(unfilled_legs_data)} legs, "
                f"{sum(l['total_lots'] for l in unfilled_legs_data)} total lots | "
                f"buffer={sweep_multiplier}x"
            )

            max_lots     = max(l['total_lots'] for l in unfilled_legs_data)
            sweep_chunks = generate_chunked_orders(
                trade_uid_prefix    = f"SQF_{trade_uid}_SWEEP{sweep_attempt}",
                legs_data           = unfilled_legs_data,
                base_lots_for_trade = max_lots,
                max_order_qty       = max_order_qty,
                aggressive          = (reason == 'SL'),
            )

            for chunk in sweep_chunks:
                for order in chunk:
                    base_buffer = buy_buffer if order.get('action', '').upper() == 'BUY' else sell_buffer
                    order['limit_order_buffer'] = base_buffer * sweep_multiplier
                    order['limit_price']        = 0.0

                logger.info(f"🔄 Executing Sweep {sweep_attempt} chunk with {len(chunk)} orders...")
                sweep_result = await executor.execute_batch(
                    chunk, f"SQF_{trade_uid}_SWEEP{sweep_attempt}_CHUNK"
                )

                successful_in_sweep = sweep_result.get('successful_orders', [])
                all_successful_orders.extend(successful_in_sweep)

                if successful_in_sweep:
                    for ord_data in successful_in_sweep:
                        try:
                            db_order = {
                                'AppOrderID':            str(ord_data.get('app_order_id')),
                                'OrderUniqueIdentifier': ord_data.get('uid'),
                                'order_unique_id':       ord_data.get('uid'),
                                'ExchangeInstrumentID':  ord_data.get('token'),
                                'OrderSide':             ord_data.get('action'),
                                'OrderQuantity':         ord_data.get('quantity'),
                                'LeavesQuantity':        ord_data.get('quantity'),
                                'CumulativeQuantity':    0,
                                'OrderStatus':           'OPEN',
                                'ProductType':           ord_data.get('product_type', 'MIS'),
                                'trade_uid':             trade_uid
                            }
                            await loop.run_in_executor(None, state.db.insert_order, db_order)
                        except Exception as ins_e:
                            logger.error(f"⚠️ Failed to persist sweep order {ord_data.get('app_order_id')}: {ins_e}")

                sweep_ids     = [str(o.get('app_order_id')) for o in successful_in_sweep if o.get('app_order_id')]
                sweep_uid_map = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_sweep}

                if sweep_ids:
                    sweep_verify = await executor.verify_orders_bulk(
                        sweep_ids,
                        f"SQF_{trade_uid}_SWEEP{sweep_attempt}_VERIFY",
                        trade_uid=trade_uid
                    )
                    sweep_verified_fills = sweep_verify.get('verified_success', [])

                    # ← FIX: inject UID into fills so they persist with correct identity
                    for fill in sweep_verified_fills:
                        app_oid = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid'))
                        if app_oid in sweep_uid_map and 'OrderUniqueIdentifier' not in fill:
                            fill['OrderUniqueIdentifier'] = sweep_uid_map[app_oid]
                            fill['order_unique_id']       = sweep_uid_map[app_oid]

                    all_verified_fills.extend(sweep_verified_fills)
                    if sweep_verified_fills:
                        state.temp_order_cache.setdefault(trade_uid, []).extend(sweep_verified_fills)

            # ← FIX: recalculate unfilled after every sweep attempt.
            # Without this, the while-loop re-enters with original unfilled list
            # even after successful fills → duplicate orders on next attempt.
            unfilled_legs_data = _compute_unfilled_legs(all_verified_fills)

            if not unfilled_legs_data:
                logger.info(f"✅ All positions fully squared off after sweep {sweep_attempt}.")
            else:
                remaining_lots = sum(l['total_lots'] for l in unfilled_legs_data)
                logger.warning(
                    f"⚠️  After sweep {sweep_attempt}: {remaining_lots} lots still unfilled. "
                    f"{'Retrying...' if sweep_attempt < max_sweep_attempts else 'Max sweep attempts reached.'}"
                )
                if sweep_attempt < max_sweep_attempts:
                    await asyncio.sleep(0.5)

        # ═══════════════════════════════════════════════════════════════════════
        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()
        
        all_verified_fills_from_cache = state.temp_order_cache.get(trade_uid, [])
        if all_verified_fills_from_cache:
            if hasattr(state.db, 'insert_order'):
                logger.info(f"Inserting {len(all_verified_fills_from_cache)} total verified square-off orders into DB for {trade_uid}...")
            await trigger_snapshot_and_broadcast(trade_uid, trade_data=straddle_data)
        
        total_orders  = len(all_successful_orders) + len(all_failed_orders)
        success_count = len(all_successful_orders)
        failed_count  = len(all_failed_orders)
        
        logger.info("="*100)
        logger.info(f"⚡ EXECUTION COMPLETE: {success_count}/{total_orders} | Time: {batch_execution_time:.2f}s")
        logger.info("="*100)
        
        end_time   = datetime.now()
        total_time = (end_time - start_time).total_seconds()
        
        final_success = (failed_count == 0 and not sqf_aborted and not all_verification_failures)
        
        total_placed             = success_count + failed_count
        verification_failed_count = len(all_verification_failures)

        logger.info("="*100)
        logger.info(f"✅ SQUARE-OFF_{trade_uid} COMPLETE | Time: {total_time:.2f}s")
        logger.info(f"✅ Placed: {success_count}/{total_placed} | ❌ Failed to Place: {failed_count} | ❌ Verification Failures: {verification_failed_count}")
        if sqf_aborted:
            logger.warning("⚠️ Square-off was aborted mid-execution. Position may be partially open.")
        logger.info("="*100)
        
        if not all_verified_fills:
            logger.warning(f"⚠️ No fills found for {trade_uid} to calculate final stats.")

        if all_verified_fills:
            leg_quantities = aggregate_verified_quantities(all_verified_fills, position_summary)
            for leg_info in leg_quantities:
                logger.info(f"   {leg_info['name']}: {leg_info['quantity']} @ ₹{leg_info['avg_price']:.2f}")

        pnl_result   = None
        final_status = 'UNKNOWN'

        if final_success:
            final_status = 'CLOSED_SQF'
            pnl_result   = await calculate_realized_pnl(trade_uid, recent_fills=all_verified_fills)
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
        else:
            final_status = 'ACTIVE'
            logger.error(f"❌ Square-off for {trade_uid} was not fully successful. Reverting status to '{final_status}' for monitoring.")

        if straddle_data:
            if pnl_result:
                straddle_data['realized_pnl'] = pnl_result.get('realized_pnl', 0.0)
            straddle_data['status'] = final_status
            
            saved_successfully = False
            for attempt in range(3):
                try:
                    await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
                    saved_doc = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                    if saved_doc and saved_doc.get('status') == final_status:
                        saved_successfully = True
                        logger.info(f"✅ Saved realized PnL (₹{straddle_data.get('realized_pnl', 0.0):,.2f}) and set status to {final_status} for {trade_uid} (Verified).")
                        break
                except Exception as e:
                    logger.error(f"❌ DB persistence error for {trade_uid} (Attempt {attempt+1}): {e}")
                    await asyncio.sleep(0.5)
            
            if not saved_successfully:
                logger.critical(f"🚨 CRITICAL: Failed to persist final status for {trade_uid} after 3 attempts.")
        else:
            logger.error(f"❌ Could not retrieve trade data for {trade_uid} to persist final PnL and status.")

        await trigger_snapshot_and_broadcast(trade_uid, trade_data=straddle_data)
        logger.info(f"✅ Final snapshot for {trade_uid} broadcasted to UI after full square-off.")
        
        return {
            'success':          final_success,
            'total_orders':     total_orders,
            'successful_count': success_count,
            'failed_count':     failed_count,
            'successful_orders': all_successful_orders,
            'failed_orders':    all_failed_orders,
            'execution_time':   batch_execution_time,
            'total_time':       total_time,
            'batches':          len(all_chunks),
            'trade_uid':        trade_uid,
            'pnl':              pnl_result
        }

    except asyncio.CancelledError:
        logger.critical(f"🛑 Square-off for {trade_uid} was cancelled abruptly. Reverting status to ACTIVE to prevent a stale state.")
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
        except Exception as db_e:
            logger.error(f"CRITICAL: Failed to revert status to ACTIVE for {trade_uid} after cancellation. DB Error: {db_e}")
        raise
    except Exception as e:
        logger.error(f"❌ Square-off failed: {e}")
        try:
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
        try:
            executor = get_order_executor()
            if executor:
                await executor.cancel_all_open_orders_for_trade(trade_uid)
        except Exception as e:
            logger.error(f"Error during final open order cleanup for {trade_uid}: {e}")

        if hasattr(state, 'closing_trades') and trade_uid in state.closing_trades:
            state.closing_trades.remove(trade_uid)
            logger.info(f"ℹ️  Trade {trade_uid} unmarked as 'closing'.")
        
        all_verified_fills_from_cache = state.temp_order_cache.get(trade_uid, [])
        if all_verified_fills_from_cache and hasattr(state.db, 'insert_order'):
            logger.info(f"Finalizing: Inserting {len(all_verified_fills_from_cache)} verified orders into DB for {trade_uid}...")
            for fill_data in all_verified_fills_from_cache:
                if fill_data.get('OrderUniqueIdentifier'):
                    await asyncio.get_event_loop().run_in_executor(None, state.db.insert_order, fill_data)
        
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
        # --- FIX: Fetch ALL of today's orders and filter robustly in Python ---
        # This matches the logic of the parity checker to handle truncated UIDs.
        all_todays_orders = await loop.run_in_executor(
            None, state.db.get_todays_orders
        )

        # Regex to find the base UID pattern (e.g., 'ny240724100000a')
        # This is the same robust pattern used by the parity checker.
        uid_pattern = re.compile(r'((?:ny|sx|bn|fn|mc)\d{12}[a-z]?)')

        all_db_orders = []
        for order in all_todays_orders:
            ouid = order.get('order_unique_id', '')
            if ouid:
                match = uid_pattern.search(ouid)
                if match and match.group(1) == trade_uid:
                    all_db_orders.append(order)
        # --- END FIX ---

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
            try:
                token = int(token_val)
            except (ValueError, TypeError):
                logger.error(f"❌ Invalid token value '{token_val}' for order {order.get('AppOrderID')}. Skipping this order.", exc_info=True)
                continue
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
            
            # --- FIX: Fallback to parsing TradingSymbol if token not in live chain ---
            if not strike:
                logger.warning(f"Could not find token {token} in live option chain for {trade_uid}. Falling back to parsing TradingSymbol from order history.")
                order_with_token = next((o for o in trade_orders if (o.get('exchange_instrument_id') or o.get('ExchangeInstrumentID')) == token), None)
                if order_with_token:
                    trading_symbol = order_with_token.get('trading_symbol') or order_with_token.get('TradingSymbol')
                    if trading_symbol:
                        # Example: NIFTY24MAR23250CE
                        match = re.search(r'(\d{5,})(CE|PE)$', trading_symbol)
                        if match:
                            strike = int(match.group(1))
                            option_type = match.group(2)
                            logger.info(f"✅ Resolved {token} to {strike} {option_type} from TradingSymbol '{trading_symbol}'")
            
            if not strike:
                logger.error(f"❌ Could not resolve strike/type for token {token} from chain or order history. Square-off may fail for this leg.")
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
        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            logger.info(f"🚀 Dispatching PARTIAL_SQUARE_OFF command to dedicated process for {trade_uid}.")
            process_info = state.trade_processes[trade_uid]
            process_info['command_q'].put({
                'command': 'PARTIAL_SQUARE_OFF',
                'percentage': percentage_of_original
            })
            return {'success': True, 'message': 'Partial Square-off command dispatched to trade process.'}

        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'PARTIAL-SQF')
        logger.info(f"🔄 Status updated: {trade_uid} -> PARTIAL-SQF")
        await trigger_snapshot_and_broadcast(trade_uid)

        executor = get_order_executor()
        if not executor:
            logger.error("❌ OrderExecutor not initialized")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ERROR')
            return None

        logger.info("="*100)
        logger.info(f"🪓  PARTIAL SQUARE OFF ({percentage_of_original}%) | Trade UID: {trade_uid}")
        logger.info("="*100)

        await executor.cancel_all_open_orders_for_trade(trade_uid)

        positions = await extract_positions_from_straddle(straddle_data)

        if not positions:
            logger.error("❌ No positions to partially square off")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {'success': False, 'error': 'No positions to square off'}

        correct_lot_size = await get_correct_lot_size(straddle_data)
        logger.info(f"✅ Verified lot_size: {correct_lot_size}")

        current_total_qty = sum(p['quantity'] for p in positions)

        if current_total_qty <= 0:
            logger.warning(f"Current total quantity for {trade_uid} is zero. Nothing to square off.")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {'success': True, 'message': 'Current position is zero.'}

        all_trade_orders = await loop.run_in_executor(None, state.db.get_orders_by_trade_id, trade_uid)

        build_orders = [
            o for o in all_trade_orders
            if (o.get('OrderUniqueIdentifier') or o.get('order_unique_id', '')).startswith(f'BUILD_{trade_uid}')
            and (str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in
                 ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED'])
        ]

        initial_total_qty = 0
        if build_orders:
            initial_total_qty = sum(int(o.get('cumulative_quantity') or o.get('CumulativeQuantity', 0)) for o in build_orders)
            logger.info(f"Calculated initial total quantity of {initial_total_qty} from {len(build_orders)} build-related orders for {trade_uid}.")

        if initial_total_qty <= 0:
            logger.warning(f"⚠️ Could not determine initial quantity for {trade_uid} from BUILD orders. Falling back to DB fields.")
            initial_ce_qty_db = straddle_data.get('initial_ce_quantity') or 0
            initial_pe_qty_db = straddle_data.get('initial_pe_quantity') or 0
            initial_total_qty = initial_ce_qty_db + initial_pe_qty_db

        if initial_total_qty <= 0:
            lots = int(straddle_data.get('lots') or 0)
            lot_size = int(straddle_data.get('lot_size') or 0)
            if lots > 0 and lot_size > 0:
                initial_total_qty = lots * lot_size * 2
            else:
                initial_total_qty = current_total_qty
                logger.warning(f"⚠️ Could not determine initial quantity for {trade_uid} from any method. Using current quantity as base.")

        target_sqf_qty = initial_total_qty * (percentage_of_original / 100.0)

        if current_total_qty > 0:
            sqf_ratio = target_sqf_qty / current_total_qty
        else:
            sqf_ratio = 0.0

        if sqf_ratio > 1.0:
            logger.warning(f"⚠️ Target SQF quantity ({target_sqf_qty}) > Current quantity ({current_total_qty}). Capping at 100%.")
            sqf_ratio = 1.0

        adjusted_percentage = sqf_ratio * 100.0

        logger.info(f"Partial SQF for {trade_uid}:")
        logger.info(f"  - Initial Total Qty: {initial_total_qty}")
        logger.info(f"  - Current Net Qty: {current_total_qty}")
        logger.info(f"  - Target SQF Qty: {target_sqf_qty} ({percentage_of_original}% of Initial)")
        logger.info(f"  - Adjusted % of Current: {adjusted_percentage:.2f}%")

        legs_data_for_batching = []
        for pos in positions:
            current_price = pos.get('current_price', 0.0)
            if not current_price or current_price <= 0:
                logger.warning(f"⚠️ Missing current_price for token {pos['token']} in partial_square_off. Fetching fresh LTP.")
                from market_data import get_ltp
                current_price = await get_ltp(pos['token'], pos.get('exchange_segment'))
                if not current_price or current_price <= 0:
                    logger.error(f"❌ Could not fetch valid LTP for token {pos['token']}. Aborting partial square-off for this leg.")
                    continue

            current_qty = pos['quantity']
            qty_to_sqf = current_qty * (adjusted_percentage / 100.0)
            lots_to_sqf = round(qty_to_sqf / correct_lot_size) if correct_lot_size > 0 else 0

            if lots_to_sqf > 0:
                legs_data_for_batching.append({
                    'token': pos['token'], 'option_type': pos['option_type'],
                    'action': 'BUY' if pos['action'] == 'SELL' else 'SELL',
                    'total_lots': int(lots_to_sqf), 'lot_size': correct_lot_size,
                    'expected_price': current_price, 'exchange_segment': pos['exchange_segment'],
                    'product_type': pos['product_type']
                })

        if not legs_data_for_batching:
            logger.warning(f"⚠️ No orders generated for partial square-off of {trade_uid}. Quantities may be too small.")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {'success': True, 'message': 'Nothing to square off for the given percentage.'}

        # ── CHUNKING ──────────────────────────────────────────────────────────
        base_lots_for_trade = max(leg['total_lots'] for leg in legs_data_for_batching) if legs_data_for_batching else 0

        symbol_upper = straddle_data.get('symbol', 'NIFTY').upper()
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        max_order_qty = SYMBOL_CONFIG.get(base_symbol, {}).get('max_order_qty', 1800) if base_symbol else 1800

        # ✅ FIX: PSQF must NOT pass order_lots_per_call — use per-leg chunk-based sizing
        all_chunks = generate_chunked_orders(
            trade_uid_prefix    = f"PSQF_{trade_uid}",
            legs_data           = legs_data_for_batching,
            base_lots_for_trade = base_lots_for_trade,
            max_order_qty       = max_order_qty,
            # range-auto: same path as BUILD
        )

        config = straddle_data.get('config', {})
        default_buffer = 6.0 if "SENSEX" in symbol_upper else 2.0
        buy_buffer = float(config.get('buy_buffer', default_buffer))
        sell_buffer = float(config.get('sell_buffer', default_buffer))
        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer
        # ── END CHUNKING ──────────────────────────────────────────────────────

        all_successful_orders = []
        all_failed_orders = []
        all_verified_fills = []
        all_verification_failures = []
        psqf_aborted = False

        if not hasattr(state, 'temp_order_cache'):
            state.temp_order_cache = {}

        batch_execution_start = datetime.now()
        aggregated_app_order_id_to_uid_map = {}

        for chunk_idx, chunk_orders in enumerate(all_chunks, 1):
            # --- CANCELLATION CHECK ---
            if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                logger.warning(f"🛑 Partial Square-off for {trade_uid} cancelled by user during chunk execution.")
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
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

            if successful_in_chunk:
                for ord_data in successful_in_chunk:
                    try:
                        db_order = {
                            'AppOrderID': str(ord_data.get('app_order_id')),
                            'OrderUniqueIdentifier': ord_data.get('uid'),
                            'order_unique_id': ord_data.get('uid'),
                            'ExchangeInstrumentID': ord_data.get('token'),
                            'OrderSide': ord_data.get('action'),
                            'OrderQuantity': ord_data.get('quantity'),
                            'LeavesQuantity': ord_data.get('quantity'),
                            'CumulativeQuantity': 0,
                            'OrderStatus': 'OPEN',
                            'ProductType': ord_data.get('product_type', 'MIS'),
                            'trade_uid': trade_uid
                        }
                        await loop.run_in_executor(None, state.db.insert_order, db_order)
                    except Exception as ins_e:
                        logger.error(f"⚠️ Failed to persist partial SQF order {ord_data.get('app_order_id')}: {ins_e}")

            chunk_order_ids = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_chunk if o.get('order_id') or o.get('app_order_id')]

            verified_fills_for_chunk = []
            unverified_order_ids = list(chunk_order_ids)
            max_verification_attempts = 3
            orders_to_reexecute_in_next_chunk = []

            for attempt in range(max_verification_attempts):
                # --- CANCELLATION CHECK ---
                if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                    logger.warning(f"🛑 Partial Square-off for {trade_uid} cancelled by user during verification.")
                    await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                    if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
                    return {'success': False, 'error': 'Cancelled by user'}
                # --- END CANCELLATION CHECK ---

                if not unverified_order_ids: break
                logger.info(f"📊 Verifying chunk {chunk_idx}, attempt {attempt + 1}/{max_verification_attempts} for {len(unverified_order_ids)} orders...")
                verification_result = await executor.verify_orders_bulk(
                    unverified_order_ids,
                    f"PSQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{attempt+1}",
                    trade_uid=trade_uid
                )
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

            ids_to_remove_from_successful = set()
            if 'newly_failed' in locals():
                for failed_order_info in newly_failed:
                    if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                        order_id = str(failed_order_info.get('order_id'))
                        ids_to_remove_from_successful.add(order_id)
                        original_order_uid = aggregated_app_order_id_to_uid_map.get(order_id)
                        if original_order_uid:
                            original_order_data = next((o for o in chunk_orders if o['uid'] == original_order_uid), None)
                            if original_order_data:
                                order_for_re_execution = original_order_data.copy()
                                order_for_re_execution['limit_price'] = 0.0
                                orders_to_reexecute_in_next_chunk.append(order_for_re_execution)
                                logger.info(f"🔄 Order {original_order_uid} marked for re-execution in next chunk.")

                    terminal_failure_statuses = {'REJECTED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}
                    if failed_order_info.get('status') in terminal_failure_statuses:
                        logger.error(f"❌ Partial SQF Verification Failure for Order {failed_order_info.get('order_id')}: {failed_order_info.get('status')} - {failed_order_info.get('reason', 'N/A')}")
                        all_verification_failures.append(failed_order_info)

            if ids_to_remove_from_successful:
                all_successful_orders = [
                    o for o in all_successful_orders
                    if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful
                ]
                logger.info(f"Corrected success tracking: Removed {len(ids_to_remove_from_successful)} orders that were cancelled for re-execution in this chunk.")

            if unverified_order_ids:
                logger.error(f"❌ FAILED to verify all orders in PARTIAL SQF chunk {chunk_idx}. Some positions may remain open. Stopping further chunks.")
                psqf_aborted = True
                break

            all_verified_fills.extend(verified_fills_for_chunk)
            if verified_fills_for_chunk:
                state.temp_order_cache.setdefault(trade_uid, []).extend(verified_fills_for_chunk)

            if orders_to_reexecute_in_next_chunk:
                if chunk_idx + 1 < len(all_chunks):
                    all_chunks[chunk_idx + 1] = orders_to_reexecute_in_next_chunk + all_chunks[chunk_idx + 1]
                    logger.info(f"Prepended {len(orders_to_reexecute_in_next_chunk)} orders to next chunk {chunk_idx + 2}.")
                else:
                    all_chunks.append(orders_to_reexecute_in_next_chunk)
                    logger.info(f"Created new chunk for {len(orders_to_reexecute_in_next_chunk)} orders for re-execution.")

        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()
        total_time = (datetime.now() - start_time).total_seconds()

        is_successful = (len(all_failed_orders) == 0 and not psqf_aborted and not all_verification_failures)

        await trigger_snapshot_and_broadcast(trade_uid)
        await asyncio.sleep(0.2)

        if all_verified_fills and hasattr(state.db, 'insert_order'):
            logger.info(f"Inserting {len(all_verified_fills)} total verified partial SQF orders into DB for {trade_uid}...")
            orders_inserted_count = 0
            for fill_data in all_verified_fills:
                app_order_id = str(fill_data.get('AppOrderID') or fill_data.get('app_order_id') or fill_data.get('apporderid'))
                if 'OrderUniqueIdentifier' not in fill_data and app_order_id in aggregated_app_order_id_to_uid_map:
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

        if straddle_data:
            pnl_to_realize_now = 0.0
            entry_price_map = {pos['token']: pos['entry_price'] for pos in positions}

            for fill in all_verified_fills:
                token = int(fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id', 0))
                if not token: continue
                closing_price = float(fill.get('OrderAverageTradedPrice') or fill.get('fill_price') or 0.0)
                closed_qty = int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or 0)
                order_side = str(fill.get('OrderSide') or fill.get('order_side', '')).upper()
                original_entry_price = entry_price_map.get(token, 0.0)

                if original_entry_price > 0 and closing_price > 0 and closed_qty > 0:
                    if order_side == 'BUY':
                        pnl_for_this_fill = (original_entry_price - closing_price) * closed_qty
                    elif order_side == 'SELL':
                        pnl_for_this_fill = (closing_price - original_entry_price) * closed_qty
                    else:
                        pnl_for_this_fill = 0.0
                    pnl_to_realize_now += pnl_for_this_fill

            existing_realized_pnl = straddle_data.get('realized_pnl', 0.0)
            existing_psqf_percentage = straddle_data.get('psqf_percentage', 0.0)
            straddle_data['realized_pnl'] = existing_realized_pnl + pnl_to_realize_now
            straddle_data['psqf_percentage'] = existing_psqf_percentage + percentage_of_original

            if correct_lot_size > 0:
                straddle_data['lot_size'] = correct_lot_size

            if hasattr(state, 'trade_data_cache'):
                state.trade_data_cache[trade_uid] = {'data': straddle_data, 'timestamp': datetime.now().timestamp()}

            logger.info(f"💰 Booking Realized PnL for {trade_uid}:")
            logger.info(f"   - Calculated from {len(all_verified_fills)} closing fills.")
            logger.info(f"   - PnL to Realize now: ₹{pnl_to_realize_now:,.2f}")
            logger.info(f"   - Previous Realized PnL: ₹{existing_realized_pnl:,.2f}")
            logger.info(f"   - New Total Realized PnL: ₹{straddle_data['realized_pnl']:,.2f}")
            logger.info(f"   - New Total PSQF %: {straddle_data['psqf_percentage']:.2f}%")

            straddle_data['status'] = 'ACTIVE'

            saved_successfully = False
            for attempt in range(3):
                try:
                    await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
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

            await trigger_snapshot_and_broadcast(trade_uid, trade_data=straddle_data)

        if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
            del state.temp_order_cache[trade_uid]

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
        await asyncio.get_event_loop().run_in_executor(
            None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
        )
        logger.warning(f"⚠️ Status for {trade_uid} reverted to ACTIVE after unexpected exception in partial_square_off.")
    finally:
        try:
            executor = get_order_executor()
            if executor:
                await executor.cancel_all_open_orders_for_trade(trade_uid)
        except Exception as e:
            logger.error(f"Error during final open order cleanup for {trade_uid}: {e}")

        if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
            del state.temp_order_cache[trade_uid]
            logger.info(f"🧹 Final cleanup: Cleared temp order cache for {trade_uid} after partial square-off attempt.")


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
 

async def square_off_by_trade_uid(trade_uid: str, reason: str = None) -> Optional[Dict]:
    """Square off by fetching from database"""
    loop = asyncio.get_event_loop()
    try:
        straddle_data = await loop.run_in_executor(
            None, state.db.get_straddle_by_id, trade_uid
        )
        
        # --- FIX: Check for any closed status ---
        if str(straddle_data.get('status', '')).startswith('CLOSED'):
            logger.warning(f"⚠️  Straddle already closed with status '{straddle_data.get('status')}': {trade_uid}")
            return None

        # The 'trade_processes' attribute only exists on the main process's state object.
        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            logger.info(f"Dispatching SQUARE_OFF command to process for trade {trade_uid}.")
            process_info = state.trade_processes[trade_uid]
            process_info['command_q'].put({'command': 'SQUARE_OFF', 'reason': reason})
            return {'success': True, 'message': 'Square-off command dispatched.'}
        elif hasattr(state, 'trade_processes'):
            logger.warning(f"Process for trade {trade_uid} not found. Executing square-off in main process.")
        else:
            logger.info(f"Executing square-off directly within worker process for {trade_uid}.")
        
        if str(straddle_data.get('status', '')).startswith('CLOSED'):
            logger.warning(f"⚠️  Straddle already closed with status '{straddle_data.get('status')}': {trade_uid}")
            return None
        
        return await square_off(
            trade_uid=trade_uid,
            straddle_data=straddle_data,
            reason=reason
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
