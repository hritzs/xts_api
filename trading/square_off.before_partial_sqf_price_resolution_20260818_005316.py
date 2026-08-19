from trading.straddle_price_guard_runtime import exit_chunk_price_allowed
"""
Square Off - Close positions with 1-lot-per-leg batching
Evenly distributes legs across batches
Verification happens as background task
"""
import asyncio
import re
from typing import Dict, List, Optional,Set
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
    synthetic_spot = snapshot.get('synthetic_spot', 0.0)
    symbol = trade.get("symbol", "NIFTY")
    symbol_upper = symbol.upper()
    base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
    gap = SYMBOL_CONFIG[base_symbol]['gap'] if base_symbol else 50
    atm_strike_from_snapshot = int(round(synthetic_spot / gap) * gap) if synthetic_spot > 0 and gap > 0 else None
    # --- END NEW ---

    logger.warning(f"🛡️  Net delta is {net_delta:.2f}. Executing neutralizing hedge before square-off.")
    result = await execute_synthetic_hedge(
        trade_uid=trade_uid, net_delta=net_delta, target_delta_reduction=-net_delta, hedge_type="PRE_SQF_NEUTRAL", atm_strike_override=atm_strike_from_snapshot, uid_prefix_override=f"J{trade_uid}"
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

        logger.debug("=" * 100)
        logger.info(f"⏹️  SQUARE OFF | Trade UID: {trade_uid}")
        logger.debug("=" * 100)

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

        # Target quantity per token for the whole SQF
        target_qty_map = {p['token']: p['total_quantity'] for p in position_summary}

        # Helper to recompute remaining lots per leg from verified fills
        def _compute_unfilled_legs(verified_fills: List[Dict]) -> List[Dict]:
            filled_qty_map: Dict[int, int] = {}
            for fill in verified_fills:
                token = int(fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id') or 0)
                qty = int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or 0)
                if not token or qty <= 0:
                    continue
                filled_qty_map[token] = filled_qty_map.get(token, 0) + qty

            legs = []
            for token, target in target_qty_map.items():
                filled = filled_qty_map.get(token, 0)
                remaining = target - filled
                if remaining > 0:
                    original_leg = next((p for p in position_summary if p['token'] == token), None)
                    if original_leg:
                        lot_size = original_leg.get('lot_size', 65)
                        lots = remaining // lot_size if lot_size > 0 else 0
                        if lots > 0:
                            legs.append({
                                'token': token,
                                'option_type': original_leg['option_type'],
                                'action': 'BUY' if original_leg['action'] == 'SELL' else 'SELL',
                                'total_lots': lots,
                                'lot_size': lot_size,
                                'expected_price': original_leg['current_price'],
                                'exchange_segment': original_leg['exchange_segment'],
                                'product_type': original_leg['product_type']
                            })
            return legs

        # ── MAIN CHUNK GENERATION ─────────────────────────────────────────────
        all_chunks = generate_chunked_orders(
            trade_uid_prefix=f"S{trade_uid}",
            legs_data=legs_data_for_batching,
            base_lots_for_trade=base_lots_for_trade,
            max_order_qty=max_order_qty,
            aggressive=(reason == 'SL'),
        )

        for chunk_idx, chunk in enumerate(all_chunks, 1):
            chunk_summary = []
            for order in chunk:
                chunk_summary.append(f"{order['action']} {order['quantity'] // order['lot_size']} lots {order['option_type']} {order['token']}")
            logger.info(f"   Chunk {chunk_idx:2d}: {', '.join(chunk_summary)}")

        config = straddle_data.get('config', {})
        symbol_upper = straddle_data.get('symbol', 'NIFTY').upper()

        if reason == 'SL':
            base_sl_buffer = 10.0 if "SENSEX" in symbol_upper else 5.0
            final_sl_buffer = base_sl_buffer * 2
            buy_buffer = final_sl_buffer
            sell_buffer = final_sl_buffer
            logger.info(f"🚨 SL Square-off detected. Using aggressive buffer: {final_sl_buffer} (Base {base_sl_buffer} * 2)")
        else:
            default_buffer = 6.0 if "SENSEX" in symbol_upper else 2.0
            buy_buffer = float(config.get('buy_buffer', default_buffer))
            sell_buffer = float(config.get('sell_buffer', default_buffer))

        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer

        logger.debug("=" * 100)
        logger.info(f"🔄 CHUNKED EXECUTION PLAN (min_lots_per_order based on max leg lots)")
        logger.info(f"   Total chunks: {len(all_chunks)}")
        logger.info(f"   Legs: {', '.join(leg_names)}")
        logger.debug("=" * 100)

        all_successful_orders = []
        all_failed_orders = []
        all_verified_fills = []
        all_verification_failures = []
        sqf_aborted = False

        if not hasattr(state, 'trade_fill_cache') or state.trade_fill_cache is None:
            state.trade_fill_cache = {}

        batch_execution_start = datetime.now()

        aggregated_app_order_id_to_uid_map: Dict[str, str] = {}

        # ═══════════════════════════════════════════════════════════════════════
        # MAIN CHUNK EXECUTION LOOP
        # ═══════════════════════════════════════════════════════════════════════
        for chunk_idx, chunk_orders in enumerate(all_chunks, 1):
            if not chunk_orders:
                continue

            orders_to_process_in_chunk = list(chunk_orders)
            verified_fills_for_chunk: List[Dict] = []
            chunk_attempt = 0
            MAX_REEXECUTE_ATTEMPTS = 3

            while orders_to_process_in_chunk and chunk_attempt < MAX_REEXECUTE_ATTEMPTS:
                buffer_multiplier = chunk_attempt + 1

                if chunk_attempt > 0:
                    logger.info(
                        f"🔄 Re-executing {len(orders_to_process_in_chunk)} orders within chunk {chunk_idx} "
                        f"(Attempt {chunk_attempt + 1}) | buffer={buffer_multiplier}x..."
                    )
                    await asyncio.sleep(0.5)

                    for order in orders_to_process_in_chunk:
                        action = order.get('action', '').upper()
                        base = buy_buffer if action == 'BUY' else sell_buffer
                        order['limit_order_buffer'] = base * buffer_multiplier
                        order['limit_price'] = 0.0

                        old_uid = order.get('uid', '')
                        if old_uid and old_uid[0].isalpha():
                            next_char = chr(ord(old_uid[0]) + 1)
                            if next_char > 'Z' and old_uid[0].isupper():
                                next_char = 'A'
                            if next_char > 'z' and old_uid[0].islower():
                                next_char = 'a'
                            order['uid'] = f"{next_char}{old_uid[1:]}"
                        else:
                            order['uid'] = f"{old_uid}R"[:20]

                logger.info(
                    f"⚡ Executing SQUARE OFF chunk {chunk_idx}/{len(all_chunks)} "
                    f"(Re-Exec Attempt {chunk_attempt + 1}) with {len(orders_to_process_in_chunk)} orders."
                )

                # ============================================================
                # EXIT_AT_STRADDLE_PRICE_GUARD_V2
                #
                # Check immediately BEFORE EVERY SQUARE-OFF CHUNK.
                #
                # target <= 0 / None / "" / "0":
                #     guard bypassed; normal exit behavior.
                #
                # target > 0:
                #     current straddle <= exit target -> BUY/EXIT
                #     current straddle >  exit target -> WAIT
                #
                # This check occurs for every chunk AND every re-execution
                # attempt, so an upward move prevents the next exit order.
                # ============================================================
                while True:
                    exit_target = (
                        (straddle_data.get("config") or {})
                        .get("exit_at_straddle")
                    )

                    exit_price_allowed = await exit_chunk_price_allowed(
                        trade_uid=trade_uid,
                        symbol=straddle_data.get("symbol", "NIFTY"),
                        target_exit_price=exit_target,
                    )

                    if exit_price_allowed:
                        logger.info(
                            f"[{trade_uid}] EXIT PRICE GUARD PASS | "
                            f"SQUARE-OFF chunk {chunk_idx}/{len(all_chunks)} "
                            f"may be submitted."
                        )
                        break

                    logger.warning(
                        f"[{trade_uid}] EXIT PRICE GUARD BLOCK | "
                        f"SQUARE-OFF chunk {chunk_idx}/{len(all_chunks)} "
                        f"NOT submitted. Waiting for exit_at_straddle "
                        f"condition."
                    )

                    await asyncio.sleep(1.0)

                chunk_result = await executor.execute_batch(
                    orders_to_process_in_chunk, f"SQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt + 1}"
                )

                successful_in_attempt = chunk_result.get('successful_orders', [])
                failed_placements = chunk_result.get('failed_orders', [])

                failed_placement_uids = {f['uid'] for f in failed_placements}
                placement_failures_to_retry = [
                    o for o in orders_to_process_in_chunk if o['uid'] in failed_placement_uids
                ]

                all_successful_orders.extend(successful_in_attempt)
                aggregated_app_order_id_to_uid_map.update(
                    {str(o.get('app_order_id')): o.get('uid') for o in successful_in_attempt}
                )

                if successful_in_attempt:
                    for ord_data in successful_in_attempt:
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
                            logger.error(f"⚠️ Failed to persist placed order {ord_data.get('app_order_id')} to DB: {ins_e}")

                attempt_order_ids = [
                    str(o.get('order_id') or o.get('app_order_id'))
                    for o in successful_in_attempt
                    if o.get('order_id') or o.get('app_order_id')
                ]
                app_order_id_to_uid_map_attempt = {
                    str(o.get('app_order_id')): o.get('uid') for o in successful_in_attempt
                }

                unverified_order_ids = list(attempt_order_ids)
                max_verification_attempts = 3
                orders_to_reexecute_in_this_chunk: List[Dict] = []
                ids_to_remove_from_successful_attempt: Set[str] = set()
                newly_failed: List[Dict] = []

                for verification_attempt in range(max_verification_attempts):
                    if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                        logger.warning(f"🛑 Square-off for {trade_uid} cancelled by user during verification.")
                        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                        if trade_uid in state.cancellation_flags:
                            del state.cancellation_flags[trade_uid]
                        if manager:
                            await manager.start_monitoring()
                        return {'success': False, 'error': 'Cancelled by user'}

                    if not unverified_order_ids:
                        break

                    logger.info(
                        f"📊 Verifying chunk {chunk_idx} (Attempt {chunk_attempt + 1}), "
                        f"verification {verification_attempt + 1}/{max_verification_attempts} "
                        f"for {len(unverified_order_ids)} orders..."
                    )
                    verification_result = await executor.verify_orders_bulk(
                        unverified_order_ids,
                        f"SQF_{trade_uid}_CHUNK{chunk_idx}_ATTEMPT{chunk_attempt + 1}_VERIFY{verification_attempt + 1}",
                        trade_uid=trade_uid
                    )

                    if verification_result:
                        newly_verified = verification_result.get('verified_success', [])
                        newly_failed = verification_result.get('verified_failed', [])
                        verified_fills_for_chunk.extend(newly_verified)

                        verified_ids = {
                            str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id'))
                            for o in newly_verified
                        }
                        reexecute_ids = {
                            str(o.get('order_id')) for o in newly_failed if o.get('status') == 'REEXECUTE_NEEDED'
                        }
                        terminal_failure_statuses = {
                            'REJECTED', 'CANCELLED', 'CANCELED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'
                        }
                        failed_ids = {
                            str(o.get('order_id')) for o in newly_failed if o.get('status') in terminal_failure_statuses
                        }
                        resolved_ids = verified_ids.union(failed_ids).union(reexecute_ids)

                        unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]

                        if unverified_order_ids:
                            logger.warning(
                                f"⚠️ {len(unverified_order_ids)} orders still pending in chunk {chunk_idx}. "
                                f"Retrying verification in 0.5s..."
                            )
                            await asyncio.sleep(0.5)

                # Final small cross-check for remaining unverified IDs
                if unverified_order_ids:
                    logger.warning(
                        f"⚠️ {len(unverified_order_ids)} orders still unverified after bulk checks. "
                        f"Doing final per-order cross-check before marking failed."
                    )
                    final_verified: List[Dict] = []
                    still_unresolved: List[str] = []
                    for oid in unverified_order_ids:
                        single_check = await executor.verify_orders_bulk(
                            [oid],
                            f"SQF_{trade_uid}_CHUNK{chunk_idx}_FINAL_SINGLE_CHECK",
                            trade_uid=trade_uid
                        )
                        if single_check:
                            v_success = single_check.get('verified_success', [])
                            v_failed = single_check.get('verified_failed', [])
                            if v_success:
                                final_verified.extend(v_success)
                            elif v_failed:
                                still_unresolved.append(oid)
                        else:
                            still_unresolved.append(oid)

                    if final_verified:
                        verified_fills_for_chunk.extend(final_verified)

                    unverified_order_ids = still_unresolved

                for failed_order_info in newly_failed:
                    if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                        ids_to_remove_from_successful_attempt.add(str(failed_order_info.get('order_id')))
                        order_id = str(failed_order_info.get('order_id'))
                        original_order_uid = app_order_id_to_uid_map_attempt.get(order_id)
                        if original_order_uid:
                            original_order_data = next(
                                (o for o in orders_to_process_in_chunk if o['uid'] == original_order_uid),
                                None
                            )
                            if original_order_data:
                                orders_to_reexecute_in_this_chunk.append(original_order_data.copy())
                                logger.info(f"🔄 Order {original_order_uid} marked for re-execution in this chunk.")

                    terminal_failure_statuses = {
                        'REJECTED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'
                    }
                    if failed_order_info.get('status') in terminal_failure_statuses:
                        logger.error(
                            f"❌ Verification Failure for Order {failed_order_info.get('order_id')}: "
                            f"{failed_order_info.get('status')} - {failed_order_info.get('reason', 'N/A')}"
                        )
                        all_verification_failures.append(failed_order_info)

                if ids_to_remove_from_successful_attempt and all_successful_orders:
                    all_successful_orders = [
                        o for o in all_successful_orders
                        if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful_attempt
                    ]
                    logger.info(
                        f"Corrected success tracking: Removed {len(ids_to_remove_from_successful_attempt)} "
                        f"orders that were cancelled for re-execution in this attempt."
                    )

                orders_to_process_in_chunk = orders_to_reexecute_in_this_chunk + placement_failures_to_retry
                chunk_attempt += 1

            if orders_to_process_in_chunk:
                logger.error(
                    f"❌ FAILED to execute {len(orders_to_process_in_chunk)} orders in chunk {chunk_idx} "
                    f"after all retries. Marking as failed."
                )
                all_failed_orders.extend(orders_to_process_in_chunk)

            if unverified_order_ids:
                logger.error(
                    f"❌ FAILED to verify {len(unverified_order_ids)} orders in chunk {chunk_idx} "
                    f"after all retries. Marking as failed."
                )
                for oid in unverified_order_ids:
                    all_failed_orders.append(
                        {'uid': 'unknown', 'app_order_id': oid, 'error': 'Verification timed out'}
                    )

            all_verified_fills.extend(verified_fills_for_chunk)
            if verified_fills_for_chunk:
                state.trade_fill_cache.setdefault(trade_uid, []).extend(verified_fills_for_chunk)

            # NEW: position-based early-exit after each chunk
            unfilled_after_chunk = _compute_unfilled_legs(all_verified_fills)
            if not unfilled_after_chunk:
                logger.info(
                    f"✅ All legs fully squared off by main chunks (up to chunk {chunk_idx}). "
                    f"Skipping remaining chunks and sweeps."
                )
                # break out of chunk loop; sweeps will see no unfilled legs
                break

        # ═══════════════════════════════════════════════════════════════════════
        # FINAL SWEEP — escalating buffer multiples
        # ═══════════════════════════════════════════════════════════════════════

        unfilled_legs_data = _compute_unfilled_legs(all_verified_fills)

        if unfilled_legs_data:
            logger.warning(
                f"⚠️  After main chunks: "
                f"{sum(l['total_lots'] for l in unfilled_legs_data)} lots unfilled across "
                f"{len(unfilled_legs_data)} legs. Entering sweep loop..."
            )

        max_sweep_attempts = 3
        sweep_attempt = 0

        while unfilled_legs_data and not sqf_aborted and sweep_attempt < max_sweep_attempts:
            sweep_attempt += 1
            sweep_multiplier = sweep_attempt + 1

            logger.info(
                f"🧹 Final Sweep (Attempt {sweep_attempt}/{max_sweep_attempts}) | "
                f"{len(unfilled_legs_data)} legs, "
                f"{sum(l['total_lots'] for l in unfilled_legs_data)} total lots | "
                f"buffer={sweep_multiplier}x"
            )

            max_lots = max(l['total_lots'] for l in unfilled_legs_data)
            sweep_prefix_char = chr(87 + sweep_attempt) if sweep_attempt <= 3 else 'W'
            sweep_chunks = generate_chunked_orders(
                trade_uid_prefix=f"{sweep_prefix_char}{trade_uid}",
                legs_data=unfilled_legs_data,
                base_lots_for_trade=max_lots,
                max_order_qty=max_order_qty,
                aggressive=(reason == 'SL'),
            )

            for chunk in sweep_chunks:
                for order in chunk:
                    base_buffer = buy_buffer if order.get('action', '').upper() == 'BUY' else sell_buffer
                    order['limit_order_buffer'] = base_buffer * sweep_multiplier
                    order['limit_price'] = 0.0

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
                            logger.error(f"⚠️ Failed to persist sweep order {ord_data.get('app_order_id')}: {ins_e}")

                sweep_ids = [str(o.get('app_order_id')) for o in successful_in_sweep if o.get('app_order_id')]
                sweep_uid_map = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_sweep}

                if sweep_ids:
                    sweep_verify = await executor.verify_orders_bulk(
                        sweep_ids,
                        f"SQF_{trade_uid}_SWEEP{sweep_attempt}_VERIFY",
                        trade_uid=trade_uid
                    )
                    sweep_verified_fills = sweep_verify.get('verified_success', []) if sweep_verify else []

                    # inject UID into fills so they persist with correct identity
                    for fill in sweep_verified_fills:
                        app_oid = str(
                            fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid')
                        )
                        if app_oid in sweep_uid_map and 'OrderUniqueIdentifier' not in fill:
                            fill['OrderUniqueIdentifier'] = sweep_uid_map[app_oid]
                            fill['order_unique_id'] = sweep_uid_map[app_oid]

                    all_verified_fills.extend(sweep_verified_fills)
                    if sweep_verified_fills:
                        state.trade_fill_cache.setdefault(trade_uid, []).extend(sweep_verified_fills)

            # recalculate unfilled after every sweep attempt
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

        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()

        all_verified_fills_from_cache = state.trade_fill_cache.get(trade_uid, [])
        if all_verified_fills_from_cache:
            if hasattr(state.db, 'insert_order'):
                logger.info(
                    f"Inserting {len(all_verified_fills_from_cache)} total verified square-off orders "
                    f"into DB for {trade_uid}..."
                )
            await trigger_snapshot_and_broadcast(trade_uid, trade_data=straddle_data)

        total_orders = len(all_successful_orders) + len(all_failed_orders)
        success_count = len(all_successful_orders)
        failed_count = len(all_failed_orders)

        logger.debug("=" * 100)
        logger.info(f"⚡ EXECUTION COMPLETE: {success_count}/{total_orders} | Time: {batch_execution_time:.2f}s")
        logger.debug("=" * 100)

        end_time = datetime.now()
        total_time = (end_time - start_time).total_seconds()

        final_success = (failed_count == 0 and not sqf_aborted and not all_verification_failures)

        total_placed = success_count + failed_count
        verification_failed_count = len(all_verification_failures)

        logger.debug("=" * 100)
        logger.info(f"✅ SQUARE-OFF_{trade_uid} COMPLETE | Time: {total_time:.2f}s")
        logger.info(
            f"✅ Placed: {success_count}/{total_placed} | ❌ Failed to Place: {failed_count} | "
            f"❌ Verification Failures: {verification_failed_count}"
        )
        if sqf_aborted:
            logger.warning("⚠️ Square-off was aborted mid-execution. Position may be partially open.")
        logger.debug("=" * 100)

        if not all_verified_fills:
            logger.warning(f"⚠️ No fills found for {trade_uid} to calculate final stats.")

        if all_verified_fills:
            leg_quantities = aggregate_verified_quantities(all_verified_fills, position_summary)
            for leg_info in leg_quantities:
                logger.info(f"   {leg_info['name']}: {leg_info['quantity']} @ ₹{leg_info['avg_price']:.2f}")

        pnl_result = None
        final_status = 'UNKNOWN'

        if final_success:
            final_status = 'CLOSED_SQF'
            pnl_result = await calculate_realized_pnl(trade_uid, recent_fills=all_verified_fills)
            if pnl_result:
                realized_pnl = pnl_result.get('realized_pnl', 0.0)
                logger.debug("=" * 100)
                logger.info(f"💰 REALIZED P&L CALCULATION for {trade_uid}")
                logger.info(f"   Total Sell Value: ₹{pnl_result.get('total_sell_value', 0):,.2f}")
                logger.info(f"   Total Buy Value:  ₹{pnl_result.get('total_buy_value', 0):,.2f}")
                logger.info(f"   Realized P&L:     ₹{realized_pnl:,.2f}")
                logger.debug("=" * 100)
            else:
                logger.warning(f"⚠️ Could not calculate realized PnL for {trade_uid}.")
        else:
            final_status = 'ACTIVE'
            logger.error(
                f"❌ Square-off for {trade_uid} was not fully successful. "
                f"Reverting status to '{final_status}' for monitoring."
            )

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
                        logger.info(
                            f"✅ Saved realized PnL (₹{straddle_data.get('realized_pnl', 0.0):,.2f}) "
                            f"and set status to {final_status} for {trade_uid} (Verified)."
                        )
                        break
                except Exception as e:
                    logger.error(f"❌ DB persistence error for {trade_uid} (Attempt {attempt + 1}): {e}")
                    await asyncio.sleep(0.5)

            if not saved_successfully:
                logger.critical(f"🚨 CRITICAL: Failed to persist final status for {trade_uid} after 3 attempts.")
        else:
            logger.error(f"❌ Could not retrieve trade data for {trade_uid} to persist final PnL and status.")

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
            'batches': len(all_chunks),
            'trade_uid': trade_uid,
            'pnl': pnl_result
        }

    except asyncio.CancelledError:
        logger.critical(
            f"🛑 Square-off for {trade_uid} was cancelled abruptly. "
            f"Reverting status to ACTIVE to prevent a stale state."
        )
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
        except Exception as db_e:
            logger.error(
                f"CRITICAL: Failed to revert status to ACTIVE for {trade_uid} after cancellation. DB Error: {db_e}"
            )
        raise
    except Exception as e:
        logger.error(f"❌ Square-off failed: {e}")
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, state.db.update_straddle_status, trade_uid, 'ACTIVE'
            )
            logger.warning(f"⚠️ Status updated: {trade_uid} -> ACTIVE due to unexpected exception in square_off.")
        except Exception as db_e:
            logger.error(
                f"CRITICAL: Failed to revert status to ACTIVE for {trade_uid} after another error. DB Error: {db_e}"
            )
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
        
        # --- FINAL CLEANUP: Persist all fills and clear cache ---
        fills_for_finalize = []
        if hasattr(state, 'trade_fill_cache') and state.trade_fill_cache:
            fills_for_finalize = list(state.trade_fill_cache.get(trade_uid, []))

        # Also include any verified fills from the square-off itself
        if 'all_verified_fills' in locals() and all_verified_fills:
            existing_ids = {
                str(f.get('AppOrderID') or f.get('app_order_id'))
                for f in fills_for_finalize
            }
            new_fills = [f for f in all_verified_fills if str(f.get('AppOrderID') or f.get('app_order_id')) not in existing_ids]
            fills_for_finalize.extend(new_fills)

        if fills_for_finalize and hasattr(state.db, 'insert_order'):
            logger.info(
                f"Finalizing: Inserting {len(fills_for_finalize)} verified orders into DB for {trade_uid}..."
            )
            for fill_data in fills_for_finalize:
                if fill_data.get('OrderUniqueIdentifier'):
                    await asyncio.get_event_loop().run_in_executor(
                        None, state.db.insert_order, fill_data
                    )

        if hasattr(state, 'trade_fill_cache') and trade_uid in state.trade_fill_cache:
            del state.trade_fill_cache[trade_uid]
            logger.info(f"🧹 Final cleanup: Cleared trade fill cache for {trade_uid} after square-off.")

def analyze_positions(positions: List[Dict], correct_lot_size: int = None) -> List[Dict]:
    """Analyze position structure and ensure quantities are multiples of lot_size"""
    summary = []
    from utils.logger import logger
    for pos in positions:
        quantity = pos.get('quantity', 0)
        lot_size = correct_lot_size or pos.get('lot_size', 65)
        lots = quantity // lot_size if lot_size > 0 else 0
        corrected_quantity = lots * lot_size
        if lots == 0:
            logger.warning(f"⚠️  Skipping {pos.get('option_type')} - quantity {quantity} less than lot size {lot_size}")
            continue
        if corrected_quantity != quantity:
            logger.warning(f"⚠️  Adjusted {pos.get('option_type')} quantity: {quantity} → {corrected_quantity}")
        summary.append({
            'token': pos.get('token'), 'strike': pos.get('strike', 0), 'option_type': pos.get('option_type', 'UNKNOWN'),
            'action': pos.get('action', 'SELL'), 'total_quantity': corrected_quantity, 'lots': lots,
            'lot_size': lot_size, 'current_price': pos.get('current_price', 0), 'entry_price': pos.get('entry_price', 0),
            'exchange_segment': pos.get('exchange_segment', 2), 'product_type': pos.get('product_type', 'MIS')
        })
    return summary

async def extract_positions_from_straddle(straddle_data: Dict) -> List[Dict]:
    from typing import Dict, List
    import asyncio
    import re
    from utils.logger import logger
    from models.state import state
    
    trade_uid = straddle_data.get('trade_uid')
    if not trade_uid:
        logger.error("❌ Cannot extract positions: trade_uid missing from straddle_data.")
        return []

    loop = asyncio.get_event_loop()
    logger.info(f"🔍 Calculating net open positions for {trade_uid} from order history...")

    try:
        all_todays_orders = await loop.run_in_executor(None, state.db.get_todays_orders)
        uid_pattern = re.compile(r'([a-zA-Z]{2}\d{12}[a-z])')
        all_db_orders = []
        for order in all_todays_orders:
            ouid = order.get('order_unique_id', '') or order.get('OrderUniqueIdentifier', '')
            if ouid:
                match = uid_pattern.search(str(ouid))
                if match and match.group(1) == trade_uid:
                    all_db_orders.append(order)

        trade_orders = [
            o for o in all_db_orders
            if str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']
        ]

        cached_fills = []
        if hasattr(state, 'trade_fill_cache') and state.trade_fill_cache:
            cached_fills = list(state.trade_fill_cache.get(trade_uid, []))
            if cached_fills:
                logger.info(f"Found {len(cached_fills)} orders in trade_fill_cache for {trade_uid}.")
        
        if not cached_fills:
            db_fills = await loop.run_in_executor(None, state.db.get_orders_by_trade_id, trade_uid)
            if db_fills:
                cached_fills = db_fills
            elif hasattr(state, 'temp_order_cache') and state.temp_order_cache:
                cached_fills = state.temp_order_cache.get(trade_uid, []) or []

        if cached_fills:
            db_order_ids = {
                str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('app_orderid'))
                for o in trade_orders
            }
            new_fills_from_cache = 0
            for fill in cached_fills:
                app_order_id = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid') or fill.get('app_orderid'))
                if app_order_id and app_order_id not in db_order_ids:
                    trade_orders.append(fill)
                    new_fills_from_cache += 1
            if new_fills_from_cache > 0:
                logger.info(f"Added {new_fills_from_cache} new orders from cache for square-off calculation.")

        if not trade_orders:
            logger.warning(f"⚠️ No filled orders found for trade {trade_uid} in DB or cache.")
            return []

        logger.info(f"Found {len(trade_orders)} filled orders for {trade_uid}.")

        net_positions = {}
        for order in trade_orders:
            token_val = order.get('exchange_instrument_id') or order.get('ExchangeInstrumentID')
            if not token_val: continue
            try: token = int(token_val)
            except (ValueError, TypeError): continue

            qty = int(order.get('cumulative_quantity') or order.get('CumulativeQuantity', 0))
            price = float(order.get('order_avg_price') or order.get('OrderAverageTradedPrice', 0))
            side = str(order.get('order_side') or order.get('OrderSide', '')).upper()

            if token not in net_positions:
                net_positions[token] = {'quantity': 0, 'token': token, 'buy_val': 0.0, 'buy_qty': 0, 'sell_val': 0.0, 'sell_qty': 0}

            if side == 'SELL':
                net_positions[token]['quantity'] += qty
                net_positions[token]['sell_qty'] += qty
                net_positions[token]['sell_val'] += (qty * price)
            elif side == 'BUY':
                net_positions[token]['quantity'] -= qty
                net_positions[token]['buy_qty'] += qty
                net_positions[token]['buy_val'] += (qty * price)

        final_positions = []
        symbol_upper = str(straddle_data.get('symbol', 'NIFTY')).upper()
        from market_data import SYMBOL_CONFIG
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        derived_segment = SYMBOL_CONFIG.get(base_symbol, {}).get('segment') if base_symbol else None
        correct_exchange_segment = derived_segment or straddle_data.get('exchange_segment', 2)

        option_chain = state.option_chains.get(symbol_upper)
        persisted_strike = int(straddle_data.get('strike') or 0)
        persisted_ce_token = int(straddle_data.get('ce_token') or 0)
        persisted_pe_token = int(straddle_data.get('pe_token') or 0)

        for token, pos_data in net_positions.items():
            net_qty = pos_data['quantity']
            if net_qty == 0: continue

            strike, option_type = None, None
            if persisted_strike > 0:
                if token == persisted_ce_token: strike, option_type = persisted_strike, 'CE'
                elif token == persisted_pe_token: strike, option_type = persisted_strike, 'PE'

            if not strike and option_chain and option_chain.get('chain'):
                for row in option_chain['chain']:
                    if row.get('ce_token') == token: strike, option_type = row['strike'], 'CE'; break
                    if row.get('pe_token') == token: strike, option_type = row['strike'], 'PE'; break

            if not strike:
                order_with_token = next((o for o in trade_orders if int(o.get('exchange_instrument_id') or o.get('ExchangeInstrumentID') or 0) == token), None)
                if order_with_token:
                    trading_symbol = order_with_token.get('trading_symbol') or order_with_token.get('TradingSymbol')
                    if trading_symbol:
                        match = re.search(r'(\d{5,})(CE|PE)$', str(trading_symbol))
                        if match: strike, option_type = int(match.group(1)), match.group(2)

            if not strike: strike, option_type = 0, 'UNKNOWN'

            current_price = state.get_price(token) or 0.0
            if current_price <= 0:
                from market_data import get_ltp_from_service
                current_price = await get_ltp_from_service(token)
                if current_price <= 0: current_price = 0.0

            entry_price = 0.0
            if net_qty > 0 and pos_data['sell_qty'] > 0: entry_price = pos_data['sell_val'] / pos_data['sell_qty']
            elif net_qty < 0 and pos_data['buy_qty'] > 0: entry_price = pos_data['buy_val'] / pos_data['buy_qty']

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
    leg_data = {}
    token_to_leg_name = {leg['token']: f"{leg['strike']} {leg['option_type']} {leg['action']}" for leg in position_summary}
    for order in orders:
        token = int(order.get('ExchangeInstrumentID') or order.get('exchange_instrument_id', 0))
        if not token: continue
        leg_name = token_to_leg_name.get(token) or f"UNKNOWN_TOKEN_{token}"
        qty = int(order.get('CumulativeQuantity') or order.get('filled_qty', 0))
        price = float(order.get('OrderAverageTradedPrice') or order.get('avg_price', 0) or order.get('fill_price', 0))

        if leg_name not in leg_data: leg_data[leg_name] = {'quantity': 0, 'total_value': 0}
        leg_data[leg_name]['quantity'] += qty
        leg_data[leg_name]['total_value'] += (price * qty)

    result = []
    for leg_key, data in leg_data.items():
        avg_price = data['total_value'] / data['quantity'] if data['quantity'] > 0 else 0
        result.append({'name': leg_key, 'quantity': data['quantity'], 'avg_price': avg_price})
    return result

async def partial_square_off(trade_uid: str, percentage_of_original: float):
    from datetime import datetime
    import asyncio
    from typing import Dict, List, Optional
    from utils.logger import logger
    from models.state import state

    loop = asyncio.get_event_loop()
    start_time = datetime.now()

    try:
        straddle_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
        if not straddle_data: return {'success': False, 'error': 'Trade data not found in DB'}
        if straddle_data.get('status') != 'ACTIVE': return {'success': False, 'error': f"Trade status is {straddle_data.get('status')}."}

        # ========================================================
        # PARTIAL SQF DIRECT EXECUTION
        # ========================================================
        # IMPORTANT:
        # Do NOT only enqueue PARTIAL_SQUARE_OFF and return here.
        #
        # The public partial_square_off() call must reach the actual
        # executor.execute_batch() path below so that the requested
        # partial quantity is really submitted.
        #
        # The normal worker/command architecture may still exist,
        # but this function is the execution boundary for a direct
        # partial-square-off request.
        # ========================================================
        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            process = getattr(state, 'local_process_refs', {}).get(trade_uid)
            command_q = getattr(state, 'local_command_queues', {}).get(trade_uid)

            if process and process.is_alive():
                logger.info(
                    f"⚡ PARTIAL-SQF DIRECT EXECUTION | "
                    f"{trade_uid} | "
                    f"Worker process is active, but execution will continue "
                    f"through square_off.py executor path."
                )
            else:
                logger.warning(
                    f"⚠️ PARTIAL-SQF | "
                    f"Worker reference exists but is not active. "
                    f"Continuing with direct execution."
                )

        await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'PARTIAL-SQF')
        from background.tasks import trigger_snapshot_and_broadcast
        await trigger_snapshot_and_broadcast(trade_uid)

        from trading.order_executor import get_order_executor
        executor = get_order_executor()
        if not executor: return None
        await executor.cancel_all_open_orders_for_trade(trade_uid)

        positions = await extract_positions_from_straddle(straddle_data)
        if not positions:
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {'success': False, 'error': 'No positions to square off'}

        from utils.helpers import get_correct_lot_size
        correct_lot_size = await get_correct_lot_size(straddle_data)
        position_summary = analyze_positions(positions, correct_lot_size)
        current_total_qty = sum(p['quantity'] for p in positions)

        if current_total_qty <= 0:
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {'success': True, 'message': 'Position is zero.'}

        all_trade_orders = await loop.run_in_executor(None, state.db.get_orders_by_trade_id, trade_uid)
        build_orders = [o for o in all_trade_orders if trade_uid in (o.get('OrderUniqueIdentifier') or o.get('order_unique_id', '')) and str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']]
        initial_total_qty = sum(int(o.get('cumulative_quantity') or o.get('CumulativeQuantity', 0)) for o in build_orders) if build_orders else 0
        if initial_total_qty <= 0: initial_total_qty = (straddle_data.get('initial_ce_quantity') or 0) + (straddle_data.get('initial_pe_quantity') or 0)
        if initial_total_qty <= 0:
            lots = int(straddle_data.get('lots') or 0); lot_size = int(straddle_data.get('lot_size') or 0)
            initial_total_qty = lots * lot_size * 2 if (lots > 0 and lot_size > 0) else current_total_qty

        target_sqf_qty = initial_total_qty * (percentage_of_original / 100.0)
        sqf_ratio = min(1.0, (target_sqf_qty / current_total_qty) if current_total_qty > 0 else 0.0)
        adjusted_percentage = sqf_ratio * 100.0

        legs_data_for_batching = []
        for pos in positions:
            current_price = pos.get('current_price', 0.0)
            if not current_price or current_price <= 0:
                from market_data import get_ltp
                import inspect
                _ltp_res = get_ltp(pos['token'], pos.get('exchange_segment'))
                current_price = float(await _ltp_res) if inspect.isawaitable(_ltp_res) else float(_ltp_res or 0.0)
                if not current_price or current_price <= 0: continue

            lots_to_sqf = round((pos['quantity'] * (adjusted_percentage / 100.0)) / correct_lot_size) if correct_lot_size > 0 else 0
            if lots_to_sqf > 0:
                legs_data_for_batching.append({
                    'token': pos['token'], 'option_type': pos['option_type'], 'action': 'BUY' if pos['action'] == 'SELL' else 'SELL',
                    'total_lots': int(lots_to_sqf), 'lot_size': correct_lot_size, 'expected_price': current_price,
                    'exchange_segment': pos['exchange_segment'], 'product_type': pos['product_type']
                })

        logger.warning(
            f"🔎 PARTIAL-SQF GENERATION DIAGNOSTIC | "
            f"Trade={trade_uid}"
        )

        logger.warning(
            f"   Position count      = {len(positions)}"
        )

        logger.warning(
            f"   Position summary    = {position_summary}"
        )

        logger.warning(
            f"   Correct lot size    = {correct_lot_size}"
        )

        logger.warning(
            f"   Initial total qty   = {initial_total_qty}"
        )

        logger.warning(
            f"   Current total qty   = {current_total_qty}"
        )

        logger.warning(
            f"   Requested percent   = {percentage_of_original}"
        )

        logger.warning(
            f"   Target SQF qty      = {target_sqf_qty}"
        )

        logger.warning(
            f"   SQF ratio           = {sqf_ratio}"
        )

        logger.warning(
            f"   Adjusted percentage = {adjusted_percentage}"
        )

        logger.warning(
            f"   Legs for batching   = {legs_data_for_batching}"
        )

        logger.warning(
            f"   Legs count          = {len(legs_data_for_batching)}"
        )

        if not legs_data_for_batching:
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {'success': True, 'message': 'Nothing to square off.'}

        base_lots_for_trade = max(leg['total_lots'] for leg in legs_data_for_batching)
        symbol_upper = straddle_data.get('symbol', 'NIFTY').upper()
        from market_data import SYMBOL_CONFIG
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        max_order_qty = SYMBOL_CONFIG.get(base_symbol, {}).get('max_order_qty', 1800) if base_symbol else 1800

        from trading.order_batching_utils import generate_chunked_orders
        all_chunks = generate_chunked_orders(trade_uid_prefix=f"P{trade_uid}", legs_data=legs_data_for_batching, base_lots_for_trade=base_lots_for_trade, max_order_qty=max_order_qty)

        config = straddle_data.get('config', {})
        buy_buffer = float(config.get('buy_buffer', 6.0 if "SENSEX" in symbol_upper else 2.0))
        sell_buffer = float(config.get('sell_buffer', 6.0 if "SENSEX" in symbol_upper else 2.0))

        for chunk in all_chunks:
            for order in chunk:
                order['limit_order_buffer'] = buy_buffer if order.get('action', '').upper() == 'BUY' else sell_buffer

        all_successful_orders, all_failed_orders, all_verified_fills, all_verification_failures = [], [], [], []
        psqf_aborted = False
        if not hasattr(state, 'trade_fill_cache') or state.trade_fill_cache is None: state.trade_fill_cache = {}
        batch_execution_start = datetime.now()
        aggregated_app_order_id_to_uid_map = {}

        from trading.straddle_price_guard_runtime import exit_chunk_price_allowed
        for chunk_idx, chunk_orders in enumerate(all_chunks, 1):
            if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                return {'success': False, 'error': 'Cancelled'}

            for order in chunk_orders:
                if float(order.get('expected_price') or 0) <= 0:
                    try:
                        from market_data import get_ltp
                        import inspect
                        _ltp_res = get_ltp(order['token'], order.get('exchange_segment'))
                        fresh_ltp = float(await _ltp_res) if inspect.isawaitable(_ltp_res) else float(_ltp_res or 0.0)
                        if fresh_ltp > 0: order['expected_price'] = float(fresh_ltp)
                    except Exception: pass

            # ----------------------------------------------------
            # DO NOT SILENTLY ABORT WHEN EXPECTED PRICE IS MISSING
            # ----------------------------------------------------
            invalid_orders = [
                o for o in chunk_orders
                if float(o.get('expected_price') or 0) <= 0
            ]

            if invalid_orders:
                logger.error(
                    f"❌ PARTIAL-SQF INVALID PRICE | "
                    f"Trade={trade_uid} | "
                    f"Chunk={chunk_idx} | "
                    f"InvalidOrders={len(invalid_orders)}/{len(chunk_orders)}"
                )

                for _bad in invalid_orders:
                    logger.error(
                        f"   INVALID PARTIAL-SQF ORDER | "
                        f"Action={_bad.get('action')} | "
                        f"Token={_bad.get('token')} | "
                        f"Qty={_bad.get('quantity')} | "
                        f"Expected={_bad.get('expected_price')}"
                    )

                # We cannot safely submit orders without a valid
                # expected price. Stop this partial request rather
                # than silently doing nothing.
                psqf_aborted = True
                break

            logger.info(
                f"✅ PARTIAL-SQF PRICES READY | "
                f"Trade={trade_uid} | "
                f"Chunk={chunk_idx} | "
                f"Orders={len(chunk_orders)}"
            )

            while True:
                exit_target = (
                    (straddle_data.get("config") or {}).get(
                        "exit_at_straddle"
                    )
                )

                guard_allowed = await exit_chunk_price_allowed(
                    trade_uid,
                    straddle_data.get("symbol", "NIFTY"),
                    exit_target,
                )

                if guard_allowed:
                    logger.info(
                        f"✅ PARTIAL-SQF EXIT PRICE GUARD PASS | "
                        f"Trade={trade_uid} | "
                        f"Chunk={chunk_idx}/{len(all_chunks)} | "
                        f"Target={exit_target}"
                    )
                    break

                logger.warning(
                    f"🛑 PARTIAL-SQF EXIT PRICE GUARD BLOCK | "
                    f"Trade={trade_uid} | "
                    f"Chunk={chunk_idx}/{len(all_chunks)} | "
                    f"Target={exit_target} | "
                    f"Waiting for current_straddle <= target"
                )

                await asyncio.sleep(1.0)

            logger.warning(
                f"🚨 PARTIAL-SQF ORDER SUBMISSION | "
                f"Trade={trade_uid} | "
                f"Chunk={chunk_idx}/{len(all_chunks)} | "
                f"Orders={len(chunk_orders)} | "
                f"ExitTarget={exit_target}"
            )

            logger.info(
                f"📤 PARTIAL-SQF ABOUT TO EXECUTE | "
                f"Trade={trade_uid} | "
                f"Chunk={chunk_idx} | "
                f"OrderCount={len(chunk_orders)}"
            )

            for _order in chunk_orders:
                logger.info(
                    f"   PARTIAL-SQF ORDER | "
                    f"Action={_order.get('action')} | "
                    f"Token={_order.get('token')} | "
                    f"Qty={_order.get('quantity')} | "
                    f"Expected={_order.get('expected_price')}"
                )

            chunk_result = await executor.execute_batch(
                chunk_orders,
                f"PSQF_{trade_uid}_CHUNK{chunk_idx}"
            )

            successful_in_chunk = chunk_result.get(
                'successful_orders',
                []
            )
            all_successful_orders.extend(successful_in_chunk)
            all_failed_orders.extend(chunk_result.get('failed_orders', []))
            aggregated_app_order_id_to_uid_map.update({str(o.get('app_order_id')): o.get('uid') for o in successful_in_chunk})

            if successful_in_chunk:
                for ord_data in successful_in_chunk:
                    try:
                        db_order = {
                            'AppOrderID': str(ord_data.get('app_order_id')), 'OrderUniqueIdentifier': ord_data.get('uid'), 'order_unique_id': ord_data.get('uid'),
                            'ExchangeInstrumentID': ord_data.get('token'), 'OrderSide': ord_data.get('action'), 'OrderQuantity': ord_data.get('quantity'),
                            'LeavesQuantity': ord_data.get('quantity'), 'CumulativeQuantity': 0, 'OrderStatus': 'OPEN', 'ProductType': ord_data.get('product_type', 'MIS'), 'trade_uid': trade_uid
                        }
                        await loop.run_in_executor(None, state.db.insert_order, db_order)
                    except Exception: pass

            chunk_order_ids = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_chunk if o.get('order_id') or o.get('app_order_id')]
            verified_fills_for_chunk = []
            unverified_order_ids = list(chunk_order_ids)

            for attempt in range(3):
                if not unverified_order_ids: break
                verification_result = await executor.verify_orders_bulk(unverified_order_ids, f"PSQF_{trade_uid}_C{chunk_idx}_A{attempt+1}", trade_uid)
                if verification_result:
                    newly_verified = verification_result.get('verified_success', [])
                    newly_failed = verification_result.get('verified_failed', [])
                    verified_fills_for_chunk.extend(newly_verified)

                    verified_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('order_id')) for o in newly_verified}
                    reexecute_ids = {str(o.get('order_id')) for o in newly_failed if o.get('status') == 'REEXECUTE_NEEDED'}
                    failed_ids = {str(o.get('order_id')) for o in newly_failed if o.get('status') in {'REJECTED', 'CANCELLED', 'CANCELED', 'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'}}
                    resolved_ids = verified_ids.union(failed_ids).union(reexecute_ids)
                    unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]
                if unverified_order_ids: await asyncio.sleep(1.0)

            if unverified_order_ids: psqf_aborted = True; break
            all_verified_fills.extend(verified_fills_for_chunk)

            # ----------------------------------------------------
            # ACTUAL PARTIAL-SQF FILL RECONCILIATION
            # ----------------------------------------------------
            verified_qty = 0

            for _fill in verified_fills_for_chunk:
                try:
                    verified_qty += int(
                        _fill.get('CumulativeQuantity')
                        or _fill.get('filled_qty')
                        or 0
                    )
                except (TypeError, ValueError):
                    pass

            logger.warning(
                f"✅ PARTIAL-SQF FILL RESULT | "
                f"Trade={trade_uid} | "
                f"Chunk={chunk_idx} | "
                f"Placed={len(successful_in_chunk)} | "
                f"VerifiedFillQty={verified_qty} | "
                f"VerifiedFillCount={len(verified_fills_for_chunk)}"
            )

            if verified_fills_for_chunk:
                for _fill in verified_fills_for_chunk:
                    logger.info(
                        f"   PARTIAL-SQF VERIFIED FILL | "
                        f"Token={_fill.get('ExchangeInstrumentID') or _fill.get('exchange_instrument_id')} | "
                        f"Qty={_fill.get('CumulativeQuantity') or _fill.get('filled_qty')} | "
                        f"Price={_fill.get('OrderAverageTradedPrice') or _fill.get('fill_price')}"
                    )
            else:
                logger.warning(
                    f"⚠️ PARTIAL-SQF CHUNK {chunk_idx} "
                    f"produced NO VERIFIED FILLS."
                )


            # ----------------------------------------------------
            # LIVE BROKER POSITION RECONCILIATION
            # ----------------------------------------------------
            try:
                broker_positions = None

                if hasattr(state.db, "get_positions_for_trade"):
                    broker_positions = await loop.run_in_executor(
                        None,
                        state.db.get_positions_for_trade,
                        trade_uid,
                    )

                if broker_positions:
                    logger.info(
                        f"📊 PARTIAL-SQF BROKER RECONCILIATION | "
                        f"Trade={trade_uid} | "
                        f"AfterChunk={chunk_idx} | "
                        f"Positions={broker_positions}"
                    )

            except Exception as recon_err:
                logger.warning(
                    f"⚠️ PARTIAL-SQF broker reconciliation unavailable | "
                    f"Trade={trade_uid} | "
                    f"Error={recon_err}"
                )

        batch_execution_time = (datetime.now() - batch_execution_start).total_seconds()
        total_time = (datetime.now() - start_time).total_seconds()
        is_successful = (len(all_failed_orders) == 0 and not psqf_aborted and not all_verification_failures)

        await trigger_snapshot_and_broadcast(trade_uid)
        await asyncio.sleep(0.2)

        if all_verified_fills and hasattr(state.db, 'insert_order'):
            for fill_data in all_verified_fills:
                app_order_id = str(fill_data.get('AppOrderID') or fill_data.get('app_order_id') or fill_data.get('apporderid'))
                if 'OrderUniqueIdentifier' not in fill_data and app_order_id in aggregated_app_order_id_to_uid_map:
                    fill_data['OrderUniqueIdentifier'] = aggregated_app_order_id_to_uid_map[app_order_id]
                if fill_data.get('OrderUniqueIdentifier'):
                    fill_data['order_unique_id'] = fill_data.get('OrderUniqueIdentifier')
                    await loop.run_in_executor(None, state.db.insert_order, fill_data)

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
                    if order_side == 'BUY': pnl_to_realize_now += (original_entry_price - closing_price) * closed_qty
                    elif order_side == 'SELL': pnl_to_realize_now += (closing_price - original_entry_price) * closed_qty

            straddle_data['realized_pnl'] = straddle_data.get('realized_pnl', 0.0) + pnl_to_realize_now
            straddle_data['psqf_percentage'] = straddle_data.get('psqf_percentage', 0.0) + percentage_of_original
            if correct_lot_size > 0: straddle_data['lot_size'] = correct_lot_size
            straddle_data['status'] = 'ACTIVE'

            for _ in range(3):
                try:
                    await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
                    break
                except Exception: await asyncio.sleep(0.5)

            await trigger_snapshot_and_broadcast(trade_uid, trade_data=straddle_data)

        logger.warning(
            f"🏁 PARTIAL-SQF EXECUTION COMPLETE | "
            f"Trade={trade_uid} | "
            f"Requested={percentage_of_original:.2f}% | "
            f"SuccessfulOrders={len(all_successful_orders)} | "
            f"FailedOrders={len(all_failed_orders)} | "
            f"VerifiedFills={len(all_verified_fills)} | "
            f"Success={is_successful}"
        )

        return {
            'success': is_successful,
            'successful_count': len(all_successful_orders),
            'failed_count': len(all_failed_orders),
            'verified_fill_count': len(all_verified_fills),
            'execution_time': batch_execution_time,
            'trade_uid': trade_uid
        }

    except asyncio.CancelledError:
        await asyncio.get_event_loop().run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
        raise
    except Exception as e:
        logger.error(f"❌ Partial Square-off failed: {e}", exc_info=True)
        await asyncio.get_event_loop().run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
        return {'success': False, 'error': str(e), 'trade_uid': trade_uid}
    finally:
        try:
            executor = get_order_executor()
            if executor: await executor.cancel_all_open_orders_for_trade(trade_uid)
        except Exception: pass

def _get_fallback_expected_price(trade_uid: str, leg: Dict) -> float:
    px = float(leg.get('current_price') or 0.0)
    if px > 0: return px
    token = leg.get('token')
    snap = getattr(state, 'trade_snapshots', {}).get(trade_uid, {})
    for p in snap.get('live_positions', []):
        if str(p.get('token')) == str(token):
            ltp = float(p.get('ltp') or 0.0)
            if ltp > 0: return ltp
    entry = float(leg.get('entry_price') or 0.0)
    if entry > 0: return entry
    return 0.0

async def square_off_by_trade_uid(trade_uid: str, reason: str = None) -> Optional[Dict]:
    import asyncio
    from models.state import state
    from utils.logger import logger
    loop = asyncio.get_event_loop()
    try:
        straddle_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
        if not straddle_data: return {'success': False, 'error': 'Trade data not found in DB'}
        if str(straddle_data.get('status', '')).startswith('CLOSED'): return None

        if hasattr(state, 'trade_processes') and trade_uid in state.trade_processes:
            process = getattr(state, 'local_process_refs', {}).get(trade_uid)
            command_q = getattr(state, 'local_command_queues', {}).get(trade_uid)
            if not process or not process.is_alive():
                getattr(state, 'local_process_refs', {}).pop(trade_uid, None)
                getattr(state, 'local_command_queues', {}).pop(trade_uid, None)
                state.trade_processes.pop(trade_uid, None)
            elif command_q:
                command_q.put({'command': 'SQUARE_OFF', 'reason': reason})
                return {'success': True, 'message': 'Square-off command dispatched.'}

        return await square_off(trade_uid=trade_uid, straddle_data=straddle_data, reason=reason)

    except asyncio.CancelledError: raise
    except Exception as e: return {'success': False, 'error': str(e), 'trade_uid': trade_uid}

async def calculate_realized_pnl(trade_uid: str, recent_fills: List[Dict] = None) -> Optional[Dict]:
    import asyncio
    from models.state import state
    try:
        loop = asyncio.get_event_loop()
        all_db_orders = await loop.run_in_executor(None, state.db.get_orders_by_trade_id, trade_uid)
        trade_orders = [o for o in all_db_orders if str(o.get('order_status', '') or o.get('OrderStatus', '')).upper() in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']]

        if recent_fills:
            db_order_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')) for o in trade_orders}
            for fill in recent_fills:
                app_order_id = str(fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid'))
                if app_order_id and app_order_id not in db_order_ids:
                    trade_orders.append(fill)

        if not trade_orders: return None
        total_buy_value = 0.0; total_sell_value = 0.0

        for order in trade_orders:
            qty = int(order.get('cumulative_quantity') or order.get('CumulativeQuantity', 0))
            price = float(order.get('order_avg_price') or order.get('OrderAverageTradedPrice', 0))
            side = str(order.get('order_side') or order.get('OrderSide', '')).upper()
            if side == 'BUY': total_buy_value += qty * price
            elif side == 'SELL': total_sell_value += qty * price

        return {'trade_uid': trade_uid, 'total_sell_value': total_sell_value, 'total_buy_value': total_buy_value, 'realized_pnl': total_sell_value - total_buy_value}
    except Exception as e: return None

async def square_off_all_active() -> Dict:
    import asyncio
    from models.state import state
    try:
        loop = asyncio.get_event_loop()
        active_straddles = await loop.run_in_executor(None, state.db.get_active_straddles)
        if not active_straddles: return {'success': True, 'count': 0, 'results': []}

        results = []
        for straddle in active_straddles:
            trade_uid = straddle.get('trade_uid') or straddle.get('straddle_id')
            result = await square_off(trade_uid=trade_uid, straddle_data=straddle)
            results.append({'trade_uid': trade_uid, 'success': result.get('success', False) if result else False, 'result': result})
            await asyncio.sleep(0.5)
        success_count = sum(1 for r in results if r['success'])
        return {'success': success_count == len(results), 'count': len(results), 'success_count': success_count, 'failed_count': len(results) - success_count, 'results': results}
    except Exception: return {'success': False, 'count': 0, 'results': []}
