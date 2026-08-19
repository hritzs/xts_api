"""
Position Roller - Execute delta-preserving rolls
"""
import asyncio
import time
from typing import Dict

from models.state import state
from utils.logger import logger
from trading.order_batching_utils import generate_chunked_orders
from trading.order_executor import get_order_executor
from background.tasks import trigger_snapshot_and_broadcast
from market_data import SYMBOL_CONFIG
from trading.square_off import extract_positions_from_straddle
from trading.data_client import get_option_chain_from_service
import config as app_config


_global_roll_lock = asyncio.Lock()


async def roll_position(trade_uid: str, roll_params: Dict = None) -> Dict:
    """
    Main entry point for rolling a position.
    Checks conditions and calls the delta-preserving execution logic.
    """
    loop = asyncio.get_event_loop()
    trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)

    if not trade:
        return {"success": False, "error": "Trade not found"}

    if trade.get('status') != 'ROLLING':
        return {"success": False, "error": f"Cannot roll trade with status '{trade.get('status')}'"}

    logger.info(f"🔄 Roll initiated for {trade_uid}")

    snapshot = state.trade_snapshots.get(trade_uid)
    spot_price = snapshot.get('synthetic_spot') if snapshot else 0

    if not spot_price or spot_price <= 0:
        return {"success": False, "error": "Could not determine spot price for roll"}

    symbol = trade.get("symbol", "NIFTY")
    symbol_upper = symbol.upper()
    base_symbol = next(
        (key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper),
        None
    )
    derived_gap = SYMBOL_CONFIG.get(base_symbol, {}).get('gap') if base_symbol else None

    if derived_gap:
        gap = derived_gap
    else:
        gap = trade.get('config', {}).get('gap', 50)
        logger.warning(
            f"Could not derive gap for {symbol_upper} from SYMBOL_CONFIG. "
            f"Falling back to config value: {gap}."
        )

    new_atm_strike = int(round(spot_price / gap) * gap)
    return await execute_delta_preserving_roll(trade_uid, new_atm_strike)


async def execute_delta_preserving_roll(trade_uid: str, new_atm_strike: int) -> Dict:
    """
    Executes a delta-weighted roll from an old strike to a new ATM strike.

    Re-execute attempts use escalating buffer multiples:
      attempt 0 -> 1x buffer
      attempt 1 -> 2x buffer
      attempt 2 -> 3x buffer

    Verification behavior is intentionally aligned with the builder:
    recoverable verification failures are retried/re-executed rather than
    immediately aborting the roll.
    """
    logger.info(
        f"LOCK_TRACE: [GLOBAL_ROLL] Attempting to acquire _global_roll_lock for {trade_uid}. "
        f"Lock status: {'LOCKED' if _global_roll_lock.locked() else 'UNLOCKED'}"
    )

    async with _global_roll_lock:
        logger.debug("=" * 100)
        logger.info(f"LOCK_TRACE: [GLOBAL_ROLL] Acquired _global_roll_lock for {trade_uid}")
        logger.debug("=" * 100)

        loop = asyncio.get_event_loop()
        logger.info(f"DB_TRACE: [ROLL_EXEC] About to read trade for {trade_uid}")
        trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)

        if not trade:
            return {"success": False, "error": "Trade not found"}

        executor = None

        try:
            start_time = time.time()
            executor = get_order_executor()

            old_strike = int(trade.get("strike", 0))
            if old_strike == new_atm_strike:
                logger.warning(f"⚠️ Roll skipped: Already at target strike {new_atm_strike}")
                return {"success": True, "message": "Already at target strike"}

            await executor.cancel_all_open_orders_for_trade(trade_uid)

            symbol = trade.get('symbol', 'NIFTY')
            option_chain = state.option_chains.get(symbol.upper())

            if not option_chain:
                logger.info(f"🔄 Roll: Cache miss for {symbol}. Fetching from service...")
                option_chain = await get_option_chain_from_service(symbol.upper())
                if option_chain:
                    state.publish_option_chain(symbol.upper(), option_chain)

            if not option_chain:
                return {"success": False, "error": f"Option chain for {symbol} not available"}

            lot_size = option_chain.get('lot_size')
            exchange_segment = option_chain.get('exchange_segment')
            if not lot_size or not exchange_segment:
                lot_size = trade.get('lot_size', 65)
                exchange_segment = trade.get('exchange_segment', app_config.EXCHANGE_NSEFO)
                logger.warning(
                    f"Using fallback lot_size ({lot_size}) and segment ({exchange_segment}) "
                    f"for {trade_uid}"
                )

            logger.info(
                f"Calculating current open positions for old strike {old_strike} for {trade_uid}..."
            )
            all_current_positions = await extract_positions_from_straddle(trade)
            if not all_current_positions:
                return {"success": False, "error": "Could not determine current open positions for roll."}

            old_ce_pos = next(
                (p for p in all_current_positions
                 if p.get('strike') == old_strike and p.get('option_type') == 'CE'),
                None
            )
            old_pe_pos = next(
                (p for p in all_current_positions
                 if p.get('strike') == old_strike and p.get('option_type') == 'PE'),
                None
            )

            buy_old_ce_raw = old_ce_pos.get('quantity', 0) if old_ce_pos else 0
            buy_old_pe_raw = old_pe_pos.get('quantity', 0) if old_pe_pos else 0

            if buy_old_ce_raw <= 0 and buy_old_pe_raw <= 0:
                return {
                    "success": False,
                    "error": f"No open positions found at the old strike {old_strike} to roll."
                }

            base_qty = (buy_old_ce_raw + buy_old_pe_raw) / 2.0
            current_total_quantity = buy_old_ce_raw + buy_old_pe_raw
            if not current_total_quantity > 0:
                return {
                    "success": False,
                    "error": f"Invalid calculated total_quantity for roll: {current_total_quantity}"
                }

            base_lots_for_trade = int(base_qty // lot_size) if lot_size > 0 else 0
            logger.info(
                f"Rolling position for {trade_uid}: Old Strike: {old_strike}, "
                f"New ATM Strike: {new_atm_strike}, Base Qty (per leg): {base_qty}"
            )

            expiry = trade.get('expiry')
            synthetic_spot = state.trade_snapshots.get(trade_uid, {}).get('synthetic_spot', 0)
            dte_days = state.trade_snapshots.get(trade_uid, {}).get('days_to_expiry', 1)
            dte_years = dte_days / 365.0

            if not all([expiry, synthetic_spot > 0, dte_years > 0]):
                return {"success": False, "error": "Missing critical data for roll (expiry, synthetic_spot, dte)"}

            def get_leg_details(strike, opt_type):
                for row in option_chain.get('chain', []):
                    if row['strike'] == strike:
                        prefix = opt_type.lower()
                        return (
                            row.get(f'{prefix}_token'),
                            row.get(f'{prefix}_ltp', 0),
                            row.get(f'{prefix}_symbol')
                        )
                return None, 0, None

            old_ce_token, old_ce_ltp, _ = get_leg_details(old_strike, 'CE')
            old_pe_token, old_pe_ltp, _ = get_leg_details(old_strike, 'PE')
            new_ce_token, new_ce_ltp, new_ce_symbol = get_leg_details(new_atm_strike, 'CE')
            new_pe_token, new_pe_ltp, new_pe_symbol = get_leg_details(new_atm_strike, 'PE')

            if not all([old_ce_token, old_pe_token, new_ce_token, new_pe_token]):
                return {"success": False, "error": "Could not resolve all option tokens for roll"}

            logger.info("Extracting deltas directly from cached option chain...")

            def get_delta_from_chain(strike, opt_type):
                for row in option_chain.get('chain', []):
                    if row['strike'] == strike:
                        return row.get(f'{opt_type.lower()}_delta', 0.0)
                logger.warning(f"Could not find delta for {strike} {opt_type} in chain.")
                return 0.0

            old_ce_delta = abs(get_delta_from_chain(old_strike, 'CE'))
            old_pe_delta = abs(get_delta_from_chain(old_strike, 'PE'))
            new_ce_delta = abs(get_delta_from_chain(new_atm_strike, 'CE'))
            new_pe_delta = abs(get_delta_from_chain(new_atm_strike, 'PE'))

            if not all([old_ce_delta > 0, old_pe_delta > 0, new_ce_delta > 0, new_pe_delta > 0]):
                logger.error("Could not extract all deltas from the option chain. Aborting roll.")
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                return {"success": False, "error": "Failed to extract deltas from option chain."}

            logger.info(
                f"Deltas - Old CE: {old_ce_delta:.4f}, Old PE: {old_pe_delta:.4f}, "
                f"New CE: {new_ce_delta:.4f}, New PE: {new_pe_delta:.4f}"
            )

            total_old_delta = old_ce_delta + old_pe_delta
            buy_old_ce_raw = base_qty * (old_pe_delta / total_old_delta) * 2 if total_old_delta > 0 else base_qty
            buy_old_pe_raw = base_qty * (old_ce_delta / total_old_delta) * 2 if total_old_delta > 0 else base_qty

            total_new_delta = new_ce_delta + new_pe_delta
            sell_new_ce_raw = base_qty * (new_pe_delta / total_new_delta) * 2 if total_new_delta > 0 else base_qty
            sell_new_pe_raw = base_qty * (new_ce_delta / total_new_delta) * 2 if total_new_delta > 0 else base_qty

            buy_old_ce_qty = int(round(buy_old_ce_raw / lot_size) * lot_size)
            buy_old_pe_qty = int(round(buy_old_pe_raw / lot_size) * lot_size)
            sell_new_ce_qty = int(round(sell_new_ce_raw / lot_size) * lot_size)
            sell_new_pe_qty = int(round(sell_new_pe_raw / lot_size) * lot_size)

            logger.info(f"Calculated Roll Quantities (rounded to lot size {lot_size}):")
            logger.info(f"   BUY  Old CE: {buy_old_ce_qty}  (raw: {buy_old_ce_raw:.2f})")
            logger.info(f"   BUY  Old PE: {buy_old_pe_qty}  (raw: {buy_old_pe_raw:.2f})")
            logger.info(f"   SELL New CE: {sell_new_ce_qty} (raw: {sell_new_ce_raw:.2f})")
            logger.info(f"   SELL New PE: {sell_new_pe_qty} (raw: {sell_new_pe_raw:.2f})")

            buy_old_ce_lots = buy_old_ce_qty // lot_size
            buy_old_pe_lots = buy_old_pe_qty // lot_size
            sell_new_ce_lots = sell_new_ce_qty // lot_size
            sell_new_pe_lots = sell_new_pe_qty // lot_size

            legs_data_for_batching = []
            product_type = trade.get('product_type', 'MIS')

            if buy_old_ce_lots > 0:
                legs_data_for_batching.append({
                    'token': old_ce_token,
                    'option_type': 'CE',
                    'action': 'BUY',
                    'total_lots': buy_old_ce_lots,
                    'lot_size': lot_size,
                    'expected_price': old_ce_ltp,
                    'exchange_segment': exchange_segment,
                    'product_type': product_type
                })

            if buy_old_pe_lots > 0:
                legs_data_for_batching.append({
                    'token': old_pe_token,
                    'option_type': 'PE',
                    'action': 'BUY',
                    'total_lots': buy_old_pe_lots,
                    'lot_size': lot_size,
                    'expected_price': old_pe_ltp,
                    'exchange_segment': exchange_segment,
                    'product_type': product_type
                })

            if sell_new_ce_lots > 0:
                legs_data_for_batching.append({
                    'token': new_ce_token,
                    'option_type': 'CE',
                    'action': 'SELL',
                    'total_lots': sell_new_ce_lots,
                    'lot_size': lot_size,
                    'expected_price': new_ce_ltp,
                    'exchange_segment': exchange_segment,
                    'product_type': product_type
                })

            if sell_new_pe_lots > 0:
                legs_data_for_batching.append({
                    'token': new_pe_token,
                    'option_type': 'PE',
                    'action': 'SELL',
                    'total_lots': sell_new_pe_lots,
                    'lot_size': lot_size,
                    'expected_price': new_pe_ltp,
                    'exchange_segment': exchange_segment,
                    'product_type': product_type
                })

            symbol_upper = symbol.upper()
            base_symbol = next(
                (key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper),
                None
            )
            max_order_qty = SYMBOL_CONFIG.get(base_symbol, {}).get('max_order_qty', 1800) if base_symbol else 1800

            all_chunks = generate_chunked_orders(
                trade_uid_prefix=f"R{trade_uid}",
                legs_data=legs_data_for_batching,
                base_lots_for_trade=base_lots_for_trade,
                chunk_divisor=7,
                max_order_qty=max_order_qty
            )

            trade_config = trade.get('config', {})
            default_buffer = 6.0 if "SENSEX" in symbol.upper() else 2.0
            buy_buffer = float(trade_config.get('buy_buffer', default_buffer))
            sell_buffer = float(trade_config.get('sell_buffer', default_buffer))

            for chunk in all_chunks:
                for order in chunk:
                    order['limit_order_buffer'] = (
                        buy_buffer if order.get('action', '').upper() == 'BUY' else sell_buffer
                    )

            logger.info(f"Generated {len(all_chunks)} chunks for roll execution.")
            logger.debug("=" * 100)
            logger.info(f"⚡ EXECUTING ROLL: {trade_uid}")
            logger.info(f"   Old: {old_strike} -> New: {new_atm_strike}")
            for leg in legs_data_for_batching:
                logger.info(
                    f"   - {leg['action']} {leg['total_lots']} lots of {leg['option_type']} "
                    f"(Token: {leg['token']})"
                )
            logger.info(f"   Total chunks to execute: {len(all_chunks)}")
            logger.debug("=" * 100)

            all_successful_orders = []
            all_failed_orders = []
            all_verified_fills = []
            all_verification_failures = []
            roll_aborted = False

            if not hasattr(state, 'temp_order_cache'):
                state.temp_order_cache = {}

            MAX_REEXECUTE_ATTEMPTS = 3
            MAX_VERIFICATION_ATTEMPTS = 3

            TERMINAL_ABORT_STATUSES = {
                'NOT_FOUND_ON_RETRY',
                'FATAL',
                'INTERNAL_ERROR'
            }

            RECOVERABLE_RETRY_STATUSES = {
                'REEXECUTE_NEEDED',
                'CANCELLED',
                'CANCELED',
                'REJECTED',
                'CANCEL_FAILED',
                'MODIFY_FAILED',
                'PARTIALLYFILLED',
                'PARTIAL',
                'OPEN',
                'NEW',
                'PENDINGNEW',
                'PENDING_REPLACE',
                'PENDINGREPLACE',
                'PENDINGCANCEL',
                'TRIGGER_PENDING'
            }

            chunk_idx = 0
            while chunk_idx < len(all_chunks):
                chunk_orders = all_chunks[chunk_idx]
                if not chunk_orders:
                    chunk_idx += 1
                    continue

                if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                    logger.warning(f"🛑 Roll for {trade_uid} cancelled by user during chunk execution.")
                    await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                    if trade_uid in state.cancellation_flags:
                        del state.cancellation_flags[trade_uid]
                    return {"success": False, "error": "Cancelled by user"}

                active_orders = list(chunk_orders)
                verified_fills_for_chunk = []
                chunk_successful_placed = []
                retry_iter = 0

                while active_orders and retry_iter < MAX_REEXECUTE_ATTEMPTS:
                    current_chunk_uid = f"ROLL_{trade_uid}_CHUNK{chunk_idx + 1}_TRY{retry_iter + 1}"

                    if retry_iter > 0:
                        buffer_multiplier = retry_iter + 1
                        logger.info(
                            f"🔄 Re-executing {len(active_orders)} orders in ROLL chunk "
                            f"{chunk_idx + 1} (Attempt {retry_iter + 1}/{MAX_REEXECUTE_ATTEMPTS}) "
                            f"with {buffer_multiplier}x buffer..."
                        )
                        for order in active_orders:
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
                                order['uid'] = f"{old_uid}R{retry_iter}"[:20]

                        await asyncio.sleep(0.5)

                    logger.info(
                        f"Executing ROLL chunk {chunk_idx + 1}/{len(all_chunks)} "
                        f"(Iter {retry_iter + 1}) with {len(active_orders)} orders."
                    )

                    chunk_result = await executor.execute_batch(active_orders, current_chunk_uid)

                    successful_in_attempt = chunk_result.get('successful_orders', [])
                    failed_placements = chunk_result.get('failed_orders', [])

                    chunk_successful_placed.extend(successful_in_attempt)
                    all_successful_orders.extend(successful_in_attempt)

                    failed_placement_uids = {f.get('uid') for f in failed_placements if f.get('uid')}
                    placement_failures_to_retry = [
                        o for o in active_orders if o.get('uid') in failed_placement_uids
                    ]

                    if successful_in_attempt:
                        db_orders_batch = []
                        for ord_data in successful_in_attempt:
                            db_orders_batch.append({
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
                            })
                        try:
                            if hasattr(state.db, 'insert_orders_bulk'):
                                await loop.run_in_executor(None, state.db.insert_orders_bulk, db_orders_batch)
                            else:
                                for row in db_orders_batch:
                                    await loop.run_in_executor(None, state.db.insert_order, row)
                        except Exception as ins_e:
                            logger.error(
                                f"⚠️ Failed to persist placed roll orders for chunk {chunk_idx + 1}: {ins_e}"
                            )

                    chunk_order_ids = [
                        str(o.get('order_id') or o.get('app_order_id'))
                        for o in successful_in_attempt
                        if o.get('order_id') or o.get('app_order_id')
                    ]
                    app_order_id_to_uid_map = {
                        str(o.get('app_order_id') or o.get('order_id')): o.get('uid')
                        for o in successful_in_attempt
                    }

                    unverified_order_ids = list(chunk_order_ids)
                    newly_failed = []
                    orders_to_retry_now = []

                    for verification_attempt in range(MAX_VERIFICATION_ATTEMPTS):
                        if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                            logger.warning(f"🛑 Roll for {trade_uid} cancelled by user during verification.")
                            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
                            if trade_uid in state.cancellation_flags:
                                del state.cancellation_flags[trade_uid]
                            return {"success": False, "error": "Cancelled by user"}

                        if not unverified_order_ids:
                            break

                        logger.info(
                            f"📊 Verifying ROLL chunk {chunk_idx + 1}, "
                            f"try {retry_iter + 1}/{MAX_REEXECUTE_ATTEMPTS}, "
                            f"verify {verification_attempt + 1}/{MAX_VERIFICATION_ATTEMPTS} "
                            f"for {len(unverified_order_ids)} orders..."
                        )

                        verification_result = await executor.verify_orders_bulk(
                            unverified_order_ids,
                            f"ROLL_{trade_uid}_CHUNK{chunk_idx + 1}_TRY{retry_iter + 1}_VER{verification_attempt + 1}",
                            trade_uid=trade_uid
                        )

                        if verification_result:
                            newly_verified = verification_result.get('verified_success', [])
                            newly_failed = verification_result.get('verified_failed', [])
                            verified_fills_for_chunk.extend(newly_verified)

                            verified_ids = {
                                str(
                                    o.get('AppOrderID') or
                                    o.get('app_order_id') or
                                    o.get('apporderid') or
                                    o.get('order_id')
                                )
                                for o in newly_verified
                            }

                            terminal_failures = [
                                f for f in newly_failed
                                if str(f.get('status', '')).upper() in TERMINAL_ABORT_STATUSES
                            ]
                            if terminal_failures:
                                for f in terminal_failures:
                                    logger.error(
                                        f"❌ Terminal Roll Failure for Order "
                                        f"{f.get('order_id') or f.get('AppOrderID')}: "
                                        f"{f.get('status')}. Aborting roll."
                                    )
                                all_verification_failures.extend(terminal_failures)
                                roll_aborted = True
                                break

                            if verification_attempt < MAX_VERIFICATION_ATTEMPTS - 1:
                                recoverable_ids = {
                                    str(
                                        o.get('AppOrderID') or
                                        o.get('app_order_id') or
                                        o.get('apporderid') or
                                        o.get('order_id')
                                    )
                                    for o in newly_failed
                                    if str(o.get('status', '')).upper() in RECOVERABLE_RETRY_STATUSES
                                }
                            else:
                                recoverable_ids = {
                                    str(
                                        o.get('AppOrderID') or
                                        o.get('app_order_id') or
                                        o.get('apporderid') or
                                        o.get('order_id')
                                    )
                                    for o in newly_failed
                                }

                            resolved_ids = verified_ids.union(recoverable_ids)
                            unverified_order_ids = [
                                oid for oid in unverified_order_ids if oid not in resolved_ids
                            ]

                        if unverified_order_ids:
                            logger.warning(
                                f"⚠️ {len(unverified_order_ids)} orders still pending in "
                                f"ROLL chunk {chunk_idx + 1}. Retrying verification in 0.5s..."
                            )
                            await asyncio.sleep(0.5)

                    if roll_aborted:
                        break

                    ids_to_remove_from_successful = set()
                    if newly_failed:
                        for failed_order_info in newly_failed:
                            status = str(failed_order_info.get('status', '')).upper()
                            if status not in RECOVERABLE_RETRY_STATUSES:
                                continue

                            order_id = str(
                                failed_order_info.get('order_id') or
                                failed_order_info.get('AppOrderID') or
                                failed_order_info.get('app_order_id') or ''
                            )
                            ids_to_remove_from_successful.add(order_id)

                            original_uid = app_order_id_to_uid_map.get(order_id)
                            original_order_data = next(
                                (o for o in active_orders if o.get('uid') == original_uid),
                                None
                            )

                            if original_order_data:
                                new_order = original_order_data.copy()
                                new_order['limit_price'] = 0.0
                                orders_to_retry_now.append(new_order)
                                logger.info(
                                    f"🔄 Order {original_uid} (AppID={order_id}, status={status}) "
                                    f"marked for re-execution in current chunk."
                                )
                            else:
                                logger.error(
                                    f"❌ Cannot rebuild re-execute order for {order_id}: "
                                    f"original not found in active_orders. Skipping."
                                )

                    if ids_to_remove_from_successful:
                        all_successful_orders = [
                            o for o in all_successful_orders
                            if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful
                        ]
                        chunk_successful_placed = [
                            o for o in chunk_successful_placed
                            if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful
                        ]
                        logger.info(
                            f"Corrected success tracking: Removed "
                            f"{len(ids_to_remove_from_successful)} orders queued for re-execution."
                        )

                    if not hasattr(state, 'trade_fill_cache') or state.trade_fill_cache is None:
                        state.trade_fill_cache = {}

                    if verified_fills_for_chunk:
                        state.trade_fill_cache.setdefault(trade_uid, []).extend(verified_fills_for_chunk)
                        logger.info(
                            f"Cached {len(verified_fills_for_chunk)} roll fills "
                            f"from chunk {chunk_idx + 1}."
                        )

                        fills_with_uid = []
                        for fill_data in verified_fills_for_chunk:
                            app_order_id = str(
                                fill_data.get('AppOrderID') or
                                fill_data.get('app_order_id') or
                                fill_data.get('apporderid') or
                                fill_data.get('order_id')
                            )
                            if app_order_id in app_order_id_to_uid_map:
                                fill_data['OrderUniqueIdentifier'] = app_order_id_to_uid_map[app_order_id]
                            if fill_data.get('OrderUniqueIdentifier'):
                                if 'order_unique_id' not in fill_data:
                                    fill_data['order_unique_id'] = fill_data.get('OrderUniqueIdentifier')
                                fills_with_uid.append(fill_data)

                        if fills_with_uid:
                            try:
                                if hasattr(state.db, 'insert_orders_bulk'):
                                    await loop.run_in_executor(None, state.db.insert_orders_bulk, fills_with_uid)
                                else:
                                    for row in fills_with_uid:
                                        await loop.run_in_executor(None, state.db.insert_order, row)
                                logger.info(
                                    f"✅ Persisted {len(fills_with_uid)} verified roll fills "
                                    f"from chunk {chunk_idx + 1}."
                                )
                            except Exception as fill_ins_e:
                                logger.error(
                                    f"Failed to persist verified fills for chunk {chunk_idx + 1}: {fill_ins_e}"
                                )

                    active_orders = orders_to_retry_now + placement_failures_to_retry
                    retry_iter += 1

                if roll_aborted:
                    break

                if active_orders:
                    logger.warning(
                        f"⚠️ After max retries, {len(active_orders)} orders in ROLL chunk "
                        f"{chunk_idx + 1} remain unresolved. Proceeding as partial roll."
                    )
                    all_failed_orders.extend(active_orders)

                all_verified_fills.extend(verified_fills_for_chunk)

                if chunk_idx < len(all_chunks) - 1:
                    await asyncio.sleep(0.05)

                chunk_idx += 1

            if not all_failed_orders and not roll_aborted:
                try:
                    logger.info(
                        f"DB_TRACE: [ROLL_EXEC] About to update strike and tokens "
                        f"to {new_atm_strike} for {trade_uid}"
                    )
                    trade_to_update = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                    if trade_to_update:
                        trade_to_update['strike'] = new_atm_strike
                        trade_to_update['ce_token'] = new_ce_token
                        trade_to_update['pe_token'] = new_pe_token
                        trade_to_update['ce_symbol'] = new_ce_symbol
                        trade_to_update['pe_symbol'] = new_pe_symbol
                        await loop.run_in_executor(None, state.db.insert_straddle, trade_to_update)
                        logger.info(
                            f"DB_TRACE: [ROLL_EXEC] Finished strike and token update for {trade_uid}"
                        )
                    else:
                        logger.error(
                            f"Could not fetch trade {trade_uid} from DB to update "
                            f"strike/tokens after roll."
                        )
                except Exception as e:
                    logger.error(f"Failed to update strike/tokens for {trade_uid} after roll: {e}")
            else:
                logger.warning(
                    f"⚠️ Roll for {trade_uid} was partial or failed. Strike will not be updated."
                )

            logger.info(f"Greeks for {trade_uid} will be updated in the next snapshot.")

            total_time = time.time() - start_time
            is_fully_successful = not all_failed_orders and not roll_aborted
            message = f"Roll from {old_strike} to {new_atm_strike} completed."
            if not is_fully_successful:
                message = (
                    f"PARTIAL roll from {old_strike} to {new_atm_strike} attempted. "
                    f"Position may be mixed."
                )
                logger.warning(message)

            logger.info(f"✅ Roll for {trade_uid} complete in {total_time:.2f}s")

            await trigger_snapshot_and_broadcast(trade_uid, bypass_debounce=True)
            logger.info(f"✅ Final snapshot for {trade_uid} broadcasted to UI after roll.")

            return {
                "success": True,
                "message": message,
                "execution_time": total_time,
                "successful_orders": len(all_successful_orders),
                "failed_orders": len(all_failed_orders),
                "verified_fills": len(all_verified_fills),
                "roll_aborted": roll_aborted
            }

        except asyncio.CancelledError:
            logger.critical(
                f"🛑 Roll for {trade_uid} was cancelled abruptly. "
                f"Reverting status to ACTIVE to prevent a stale state."
            )
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            raise

        except Exception as e:
            logger.error(f"❌ Roll execution failed for {trade_uid}: {e}", exc_info=True)
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            return {"success": False, "error": str(e)}

        finally:
            logger.info(
                f"LOCK_TRACE: [GLOBAL_ROLL] Exiting locked section for {trade_uid}. "
                f"Lock will be released automatically by 'async with'."
            )
            try:
                if executor:
                    await executor.cancel_all_open_orders_for_trade(trade_uid)
            except Exception:
                pass

            if hasattr(state, 'trade_fill_cache') and trade_uid in state.trade_fill_cache:
                del state.trade_fill_cache[trade_uid]
                logger.info(
                    f"🧹 Final cleanup: Cleared temp order cache for {trade_uid} after roll attempt."
                )