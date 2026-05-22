"""
Position Builder - Build ATM Straddles (Delta-Neutral)
Supports: NSE F&O, BSE F&O, and other segments dynamically
Uses same UID for entire trade lifecycle
Verification runs as background task
"""
import asyncio
import math
import multiprocessing
import functools
import time
from typing import Dict, Optional, List, Any
from datetime import datetime, timedelta
from utils.logger import logger
from models.state import state
from trading.order_batching_utils import generate_chunked_orders
from utils.helpers import get_ist_now
from market_data import get_option_chain, SYMBOL_CONFIG
from trading.order_executor import get_order_executor
from trading.delta_neutral_utils import calculate_delta_neutral_quantities
from background.tasks import broadcast_log, create_snapshot_for_trade, trigger_snapshot_and_broadcast
from trading.data_client import get_option_chain_from_service
import config


async def _get_fresh_snapshot(trade_uid: str, timeout: float = 3.0) -> Optional[Dict]:
    """
    Triggers and polls for a fresh snapshot from the local cache.
    """
    logger.debug(f"Requesting and polling for fresh snapshot for {trade_uid} (timeout: {timeout}s)")
    await trigger_snapshot_and_broadcast(trade_uid, bypass_debounce=True)

    start_poll = time.time()
    while time.time() - start_poll < timeout:
        snapshot = state.trade_snapshots.get(trade_uid)
        if snapshot:
            logger.debug(f"Got snapshot for {trade_uid} after {time.time() - start_poll:.2f}s")
            return snapshot
        await asyncio.sleep(0.2)

    logger.warning(f"Timed out waiting for fresh snapshot for {trade_uid} after {timeout}s.")
    return None

async def build_straddle(
    symbol: str,
    lots: int,
    trade_uid: str = None,
    delta_neutral: bool = True,
    product_type: str = "MIS",
    strike_range: int = 15,
    trade_config: Dict = None,
    target_expiry: str = None,
    ce_strike_price: int = None,
    pe_strike_price: int = None
) -> Optional[Dict]:
    """
    BUILD ATM STRADDLE (DELTA-NEUTRAL)

    Args:
        symbol: Index symbol (NIFTY, SENSEX, BANKNIFTY, etc.)
        lots: Number of lots (baseline for delta-neutral calc)
        trade_uid: Trade UID (auto-generated if None)
        delta_neutral: If True, calculates unequal PE/CE for delta neutrality
        product_type: Product type (MIS/NRML, default: MIS)
        strike_range: Strike range for option chain (default: 15)
        trade_config: Full configuration dictionary for the trade
        target_expiry: Specific expiry to build for (e.g., "25JUL2024"), used for rolling.
        ce_strike_price: Manually specify CE strike. If provided, pe_strike_price must also be given.
        pe_strike_price: Manually specify PE strike. If provided, ce_strike_price must also be given.

    Returns:
        Straddle data dict or None if failed
    """
    start_time = get_ist_now()

    try:
        if trade_config is None:
            trade_config = {}

        now = get_ist_now()
        next_minute_start = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
        next_minute_str = next_minute_start.strftime('%H:%M:%S')

        if 'entry_time' not in trade_config or not trade_config.get('entry_time'):
            trade_config['entry_time'] = next_minute_str
        if 'sl_start_time' not in trade_config or not trade_config.get('sl_start_time'):
            trade_config['sl_start_time'] = next_minute_str
        if 'hedge_start_time' not in trade_config or not trade_config.get('hedge_start_time'):
            trade_config['hedge_start_time'] = next_minute_str
        if 'roll_start_time' not in trade_config or not trade_config.get('roll_start_time'):
            trade_config['roll_start_time'] = next_minute_str
        if 'exit_time' not in trade_config or not trade_config.get('exit_time'):
            trade_config['exit_time'] = '15:27:00'

        if not trade_uid:
            now = get_ist_now()
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
                timestamp = now.strftime("%d%m%y%H%M%S")

            SYMBOL_PREFIXES = {
                "NIFTY": "ny", "SENSEX": "sx", "BANKNIFTY": "bn",
                "FINNIFTY": "fn", "MIDCPNIFTY": "mc",
            }
            symbol_upper = symbol.upper()
            sorted_keys = sorted(SYMBOL_PREFIXES.keys(), key=len, reverse=True)
            prefix = next(
                (SYMBOL_PREFIXES[key] for key in sorted_keys if key in symbol_upper),
                symbol[:2].lower()
            )
            base_trade_uid = f"{prefix}{timestamp}"

            suffix_counter = 0
            loop = asyncio.get_event_loop()
            trade_uid = f"{base_trade_uid}{chr(ord('a') + suffix_counter)}"
            while (
                trade_uid in state.trade_processes or
                await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            ):
                suffix_counter += 1
                trade_uid = f"{base_trade_uid}{chr(ord('a') + suffix_counter)}"
            logger.info(f"Generated unique trade_uid: {trade_uid}")

        executor = get_order_executor()
        if not executor:
            logger.error("OrderExecutor not initialized")
            return None

        logger.info("=" * 100)
        logger.info(f"BUILD STRADDLE | Trade UID: {trade_uid}")
        if delta_neutral:
            logger.info("DELTA-NEUTRAL MODE: Calculating unequal PE/CE quantities")
        logger.info("=" * 100)

        loop = asyncio.get_event_loop()

        if target_expiry:
            logger.info(f"Building a specific chain for target expiry: {target_expiry}")
            chain_data = await loop.run_in_executor(
                None, get_option_chain, symbol, strike_range, target_expiry
            )
        else:
            chain_data = state.get_option_chain(symbol.upper())
            if not chain_data:
                logger.info(f"Cache miss for {symbol}. Fetching from service...")
                chain_data = await get_option_chain_from_service(symbol.upper())
                if chain_data:
                    state.update_option_chain(symbol.upper(), chain_data)

        if not chain_data:
            logger.error(
                f"Option chain for {symbol.upper()} not found in cache. "
                "The background task may not have run yet or failed. Aborting build."
            )
            return None

        exchange_segment = chain_data.get('exchange_segment', config.EXCHANGE_NSEFO)
        exchange_name = {2: "NSE", 12: "BSE", 1: "NSECM", 11: "BSECM"}.get(
            exchange_segment, f"SEG{exchange_segment}"
        )
        logger.info(f"Exchange: {exchange_name} (Segment: {exchange_segment})")

        is_custom_strike = ce_strike_price is not None and pe_strike_price is not None

        if is_custom_strike:
            logger.info(f"Building custom position with CE Strike: {ce_strike_price} and PE Strike: {pe_strike_price}")
            ce_row = next((row for row in chain_data['chain'] if row['strike'] == ce_strike_price), None)
            pe_row = next((row for row in chain_data['chain'] if row['strike'] == pe_strike_price), None)

            if not ce_row:
                logger.error(f"CE Strike {ce_strike_price} not found in option chain for {symbol}.")
                return {'success': False, 'error': f"CE Strike {ce_strike_price} not found"}
            if not pe_row:
                logger.error(f"PE Strike {pe_strike_price} not found in option chain for {symbol}.")
                return {'success': False, 'error': f"PE Strike {pe_strike_price} not found"}

            atm = ce_strike_price if ce_strike_price == pe_strike_price else f"{pe_strike_price}/{ce_strike_price}"
        else:
            logger.info("Building ATM straddle (default).")
            atm = chain_data['atm']
            atm_row = next((row for row in chain_data['chain'] if row['is_atm']), None)
            if not atm_row:
                logger.error("ATM strike not found")
                return None
            ce_row = pe_row = atm_row

        lot_size = chain_data.get("lot_size")
        if not lot_size or lot_size <= 0:
            logger.error(f"Invalid or missing lot size in option chain for {symbol}. Aborting build.")
            return None

        ce_token = ce_row.get('ce_token')
        pe_token = pe_row.get('pe_token')
        ce_symbol = ce_row.get('ce_symbol')
        pe_symbol = pe_row.get('pe_symbol')

        if (ce_token and not isinstance(ce_token, int)) or \
           (pe_token and not isinstance(pe_token, int)):
            logger.critical(
                f"CRITICAL DATA CORRUPTION DETECTED in option chain for {symbol}. "
                f"CE Token: '{ce_token}' (type: {type(ce_token)}), "
                f"PE Token: '{pe_token}' (type: {type(pe_token)}). "
                "This should be an integer. Aborting build."
            )
            return None

        ce_ltp = ce_row.get('ce_ltp', 0.0)
        pe_ltp = pe_row.get('pe_ltp', 0.0)

        if ce_ltp <= 0 or pe_ltp <= 0:
            logger.error(f"Invalid LTP: CE={ce_ltp}, PE={pe_ltp}")
            return None

        ce_delta = ce_row.get('ce_delta', 0.5)
        pe_delta = pe_row.get('pe_delta', -0.5)

        base_symbol = next(
            (key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True)
             if key in symbol.upper()), None
        )
        sym_config = SYMBOL_CONFIG.get(base_symbol, {})
        max_order_qty = sym_config.get('max_order_qty', config.MAX_ORDER_QTY)
        logger.info(f"Using MaxOrderQty: {max_order_qty} for {symbol}")

        if delta_neutral and ce_delta != 0 and pe_delta != 0:
            logger.info("=" * 100)
            logger.info("DELTA-NEUTRAL CALCULATION")
            logger.info("=" * 100)
            logger.info(f"CE Delta: {ce_delta:.6f}")
            logger.info(f"PE Delta: {pe_delta:.6f}")
            logger.info(f"Baseline lots: {lots}")

            target_contracts = lots * lot_size
            pe_lots, ce_lots, pe_contracts, ce_contracts, net_delta = calculate_delta_neutral_quantities(
                ce_option_delta=ce_delta,
                pe_option_delta=pe_delta,
                target_contracts=target_contracts,
                lotsize=lot_size
            )
            logger.info("=" * 100)
            logger.info("DELTA-NEUTRAL ALLOCATION:")
            logger.info(f"   PE: {pe_contracts} contracts ({pe_lots} lots)")
            logger.info(f"   CE: {ce_contracts} contracts ({ce_lots} lots)")
            logger.info(f"   Net Delta: {net_delta:.4f}")
            logger.info("=" * 100)
        else:
            pe_lots = lots
            ce_lots = lots
            pe_contracts = lots * lot_size
            ce_contracts = lots * lot_size
            net_delta = 0.0
            logger.info("=" * 100)
            logger.info("EQUAL ALLOCATION:")
            logger.info(f"   PE: {pe_contracts} contracts ({pe_lots} lots)")
            logger.info(f"   CE: {ce_contracts} contracts ({ce_lots} lots)")
            logger.info("=" * 100)

        total_quantity = pe_contracts + ce_contracts
        logger.info(f"[{exchange_name}] {symbol} ATM {atm} | Total Qty: {total_quantity}")
        logger.info(f"CE: {ce_token} @ {ce_ltp:.2f} x {ce_contracts}")
        logger.info(f"PE: {pe_token} @ {pe_ltp:.2f} x {pe_contracts}")

        legs_data_for_batching = []
        if ce_lots > 0:
            legs_data_for_batching.append({
                'token': ce_token, 'option_type': 'CE', 'action': 'SELL',
                'total_lots': ce_lots, 'lot_size': lot_size,
                'expected_price': ce_ltp, 'exchange_segment': exchange_segment,
                'product_type': product_type
            })
        if pe_lots > 0:
            legs_data_for_batching.append({
                'token': pe_token, 'option_type': 'PE', 'action': 'SELL',
                'total_lots': pe_lots, 'lot_size': lot_size,
                'expected_price': pe_ltp, 'exchange_segment': exchange_segment,
                'product_type': product_type
            })

        # ── CHUNKING STRATEGY ──────────────────────────────────────────────────
        # Automated path (build_with_config / standard flow):
        #   force_min_lots_per_order=1 → every order = 1 lot = 65 qty
        #   chunk_divisor=7 → 7 execution waves × ~(lots/7) micro-orders each
        #
        # Manual override path:
        #   order_lots_per_call is set in trade_config (e.g. 10)
        #   generate_chunked_orders overrides chunk_divisor = ceil(max_lots / 10)
        #   each order = 10 lots = 650 qty (for NIFTY)
        # ──────────────────────────────────────────────────────────────────────
        order_lots_per_call = trade_config.get('order_lots_per_call') if trade_config else None
        logger.info(
            f"BUILD chunking: "
            f"{'MANUAL order_lots_per_call=' + str(order_lots_per_call) if order_lots_per_call else 'RANGE-AUTO ceil(' + str(lots) + '/100)'} "
            f"| lots={lots}"
        )


        all_chunks = generate_chunked_orders(
            trade_uid_prefix    = f"BUI_{trade_uid}",
            legs_data           = legs_data_for_batching,
            base_lots_for_trade = lots,
            max_order_qty       = max_order_qty,
            order_lots_per_call = order_lots_per_call,
        )

        default_buffer = 6.0 if "SENSEX" in symbol.upper() else 2.0
        buy_buffer = float(trade_config.get('buy_buffer', default_buffer)) if trade_config else default_buffer
        sell_buffer = float(trade_config.get('sell_buffer', default_buffer)) if trade_config else default_buffer
        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer

        # --- NEW: Straddle price stop-loss threshold ---
        straddle_price_filter = float(trade_config.get('straddle_filter', 0.0)) if trade_config else 0.0
        straddle_stop_pct = float(trade_config.get('straddle_stop_loss_pct', 1.0)) if trade_config else 1.0
        stop_price_threshold = 0.0
        if straddle_price_filter > 0 and straddle_stop_pct > 0:
            stop_price_threshold = straddle_price_filter * (1 - (straddle_stop_pct / 100.0))
            logger.info(f"Straddle price stop threshold enabled: < ₹{stop_price_threshold:.2f}")

        logger.info(f"Generated {len(all_chunks)} chunks for execution.")

        all_successful_orders = []
        all_failed_orders = []
        all_verified_fills = []
        build_aborted = False
        is_first_fill_processed = False
        total_execution_time = 0.0

        if not hasattr(state, 'temp_order_cache'):
            state.temp_order_cache = {}

        # ═══════════════════════════════════════════════════════════════════════
        # CHUNK EXECUTION LOOP
        # ═══════════════════════════════════════════════════════════════════════
        chunk_idx = 0
        while chunk_idx < len(all_chunks):
            orders_to_process = all_chunks[chunk_idx]
            if not orders_to_process:
                chunk_idx += 1
                continue

            if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
                logger.warning(f"Build for {trade_uid} cancelled by user during chunk execution.")
                if is_first_fill_processed:
                    await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'PARTIAL')
                if trade_uid in state.cancellation_flags:
                    del state.cancellation_flags[trade_uid]
                build_aborted = True
                break

            # --- NEW: Check straddle price stop condition before executing chunk ---
            if stop_price_threshold > 0:
                # Get latest ATM straddle price
                fresh_chain_data = await get_option_chain_from_service(symbol.upper())
                if fresh_chain_data:
                    state.update_option_chain(symbol.upper(), fresh_chain_data)
                    current_chain_data = fresh_chain_data
                else:
                    current_chain_data = chain_data # Fallback to older chain if service fails

                current_atm_strike = current_chain_data.get('atm')
                current_atm_row = next((row for row in current_chain_data.get('chain', []) if row.get('strike') == current_atm_strike), None)

                if current_atm_row:
                    current_ce_ltp = current_atm_row.get('ce_ltp', 0)
                    current_pe_ltp = current_atm_row.get('pe_ltp', 0)
                    current_straddle_price = current_ce_ltp + current_pe_ltp

                    if current_straddle_price < stop_price_threshold:
                        logger.warning(
                            f"🛑 BUILD STOPPED for {trade_uid} due to straddle price drop. "
                            f"Current: ₹{current_straddle_price:.2f} < Threshold: ₹{stop_price_threshold:.2f}"
                        )
                        build_aborted = True
                        break
                    else:
                        logger.info(
                            f"In-build Straddle Price Check OK for {trade_uid}. "
                            f"Current: ₹{current_straddle_price:.2f} >= Threshold: ₹{stop_price_threshold:.2f}"
                        )

            max_chunk_retries = 3
            retry_iter = 0

            while orders_to_process and retry_iter < max_chunk_retries:
                current_chunk_uid = f"BUI_{trade_uid}_CHUNK{chunk_idx+1}_TRY{retry_iter+1}"

                if retry_iter > 0:
                    buffer_multiplier = retry_iter + 1
                    logger.info(f"Retrying {len(orders_to_process)} orders in CHUNK {chunk_idx + 1} (Attempt {retry_iter+1}) with {buffer_multiplier}x buffer...")
                    for order in orders_to_process:
                        action = order.get('action', '').upper()
                        base_buffer = buy_buffer if action == 'BUY' else sell_buffer
                        order['limit_order_buffer'] = base_buffer * buffer_multiplier
                        order['limit_price'] = 0.0
                        old_uid = order.get('uid', '')
                        new_uid = f"{old_uid.split('_TRY')[0]}_TRY{retry_iter}_{int(time.time()*1000)%10000}"[:20]
                        order['uid'] = new_uid
                    await asyncio.sleep(0)

                logger.info(f"Executing BUILD chunk {chunk_idx + 1}/{len(all_chunks)} (Iter {retry_iter+1}) with {len(orders_to_process)} orders.")

                chunk_result = await executor.execute_batch(orders_to_process, current_chunk_uid)
                total_execution_time += chunk_result.get('execution_time', 0.0)

                successful_in_chunk = chunk_result.get('successful_orders', [])
                failed_placements = chunk_result.get('failed_orders', [])

                failed_placement_uids = {f['uid'] for f in failed_placements}
                placement_failures_to_retry = [
                    o for o in orders_to_process if o['uid'] in failed_placement_uids
                ]

                all_successful_orders.extend(successful_in_chunk)

                if successful_in_chunk:
                    db_orders_batch = [
                        {
                            'AppOrderID': str(o.get('app_order_id')),
                            'OrderUniqueIdentifier': o.get('uid'),
                            'order_unique_id': o.get('uid'),
                            'ExchangeInstrumentID': o.get('token'),
                            'OrderSide': o.get('action'),
                            'OrderQuantity': o.get('quantity'),
                            'LeavesQuantity': o.get('quantity'),
                            'CumulativeQuantity': 0,
                            'OrderStatus': 'OPEN',
                            'ProductType': o.get('product_type', 'MIS'),
                            'trade_uid': trade_uid
                        }
                        for o in successful_in_chunk
                    ]
                    try:
                        await loop.run_in_executor(None, state.db.insert_orders_bulk, db_orders_batch)
                    except Exception as ins_e:
                        logger.error(f"Failed to bulk-persist {len(db_orders_batch)} placed orders for chunk {chunk_idx+1}: {ins_e}")

                chunk_order_ids = [str(o.get('order_id') or o.get('app_order_id')) for o in successful_in_chunk if o.get('order_id') or o.get('app_order_id')]
                app_order_id_to_uid_map = {str(o.get('app_order_id')): o.get('uid') for o in successful_in_chunk}

                verified_fills_for_chunk = []
                unverified_order_ids = list(chunk_order_ids)
                max_verification_attempts = 3
                newly_failed = []
                orders_to_retry_now = []

                for attempt in range(max_verification_attempts):
                    if not unverified_order_ids:
                        break

                    logger.info(f"Verifying BUILD chunk {chunk_idx + 1}, attempt {attempt + 1}/{max_verification_attempts} for {len(unverified_order_ids)} orders...")
                    verification_result = await executor.verify_orders_bulk(
                        unverified_order_ids,
                        f"BUI_{trade_uid}_CHUNK{chunk_idx+1}_ITER{retry_iter+1}_VER{attempt+1}",
                        trade_uid=trade_uid
                    )

                    if verification_result:
                        newly_verified = verification_result.get('verified_success', [])
                        newly_failed = verification_result.get('verified_failed', [])
                        verified_fills_for_chunk.extend(newly_verified)

                        verified_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_verified}

                        if attempt < max_verification_attempts - 1:
                            terminal_statuses = {'REJECTED', 'CANCELLED', 'CANCELED', 'REEXECUTE_NEEDED', 'NOT_FOUND_ON_RETRY'}
                            failed_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_failed if str(o.get('status')).upper() in terminal_statuses}
                        else:
                            failed_ids = {str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id')) for o in newly_failed}

                        resolved_ids = verified_ids.union(failed_ids)
                        unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]

                    if unverified_order_ids:
                        logger.warning(f"{len(unverified_order_ids)} orders still pending in BUILD chunk {chunk_idx + 1}. Retrying in 0.5s...")
                        await asyncio.sleep(0.5)

                ids_to_remove_from_successful = set()
                if newly_failed:
                    for failed_order_info in newly_failed:
                        if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                            order_id = str(failed_order_info.get('order_id'))
                            ids_to_remove_from_successful.add(order_id)
                            original_order_uid = app_order_id_to_uid_map.get(order_id)
                            if original_order_uid:
                                original_order_data = next((o for o in orders_to_process if o['uid'] == original_order_uid), None)
                                if original_order_data:
                                    orders_to_retry_now.append(original_order_data)
                                    logger.info(f"Order {original_order_uid} marked for re-execution in current chunk (Iter {retry_iter+1}).")

                if ids_to_remove_from_successful:
                    all_successful_orders = [o for o in all_successful_orders if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful]
                    logger.info(f"Corrected success tracking: Removed {len(ids_to_remove_from_successful)} orders that were cancelled for re-execution.")

                all_verified_fills.extend(verified_fills_for_chunk)

                fills_to_process_for_chunk = verified_fills_for_chunk
                if fills_to_process_for_chunk:
                    state.temp_order_cache.setdefault(trade_uid, []).extend(fills_to_process_for_chunk)
                    logger.info(
                        f"Cached {len(fills_to_process_for_chunk)} build orders "
                        f"under key '{trade_uid}' from chunk {chunk_idx + 1}."
                    )

                    app_order_id_to_uid_map_final = {
                        str(o.get('app_order_id')): o.get('uid')
                        for o in successful_in_chunk
                    }
                    fills_with_uid = []
                    for fill_data in fills_to_process_for_chunk:
                        app_order_id = str(
                            fill_data.get('AppOrderID') or fill_data.get('app_order_id') or
                            fill_data.get('apporderid')
                        )
                        if app_order_id in app_order_id_to_uid_map_final:
                            fill_data['OrderUniqueIdentifier'] = app_order_id_to_uid_map_final[app_order_id]
                        if fill_data.get('OrderUniqueIdentifier'):
                            if 'order_unique_id' not in fill_data:
                                fill_data['order_unique_id'] = fill_data.get('OrderUniqueIdentifier')
                            fills_with_uid.append(fill_data)
                    if fills_with_uid:
                        state.db.insert_orders_bulk(fills_with_uid)
                        logger.info(f"Bulk inserted {len(fills_with_uid)} verified fills from chunk {chunk_idx + 1}.")

                if all_verified_fills:
                    ce_total_value, ce_filled_qty, pe_total_value, pe_filled_qty = 0.0, 0, 0.0, 0
                    for fill in all_verified_fills:
                        avg_price = float(
                            fill.get('OrderAverageTradedPrice') or fill.get('fill_price') or 0.0
                        )
                        qty = int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or 0)
                        token_val = fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id')
                        if not token_val:
                            logger.error(f"Build Error: Fill data missing instrument token during live check: {fill}")
                            continue
                        token = int(token_val)
                        if token == ce_token:
                            ce_total_value += avg_price * qty; ce_filled_qty += qty
                        elif token == pe_token:
                            pe_total_value += avg_price * qty; pe_filled_qty += qty

                    avg_ce_fill = (ce_total_value / ce_filled_qty) if ce_filled_qty > 0 else ce_ltp
                    avg_pe_fill = (pe_total_value / pe_filled_qty) if pe_filled_qty > 0 else pe_ltp

                    current_straddle_data = {
                        'straddle_id': trade_uid, 'trade_uid': trade_uid,
                        'symbol': symbol, 'strike': atm,
                        'expiry': chain_data['expiry'], 'expiry_date': chain_data.get('expiry_date'),
                        'exchange_segment': exchange_segment, 'exchange_name': exchange_name,
                        'product_type': product_type, 'lot_size': lot_size, 'lots': lots,
                        'initial_pe_quantity': pe_contracts, 'initial_ce_quantity': ce_contracts,
                        'pe_lots': pe_filled_qty // lot_size, 'ce_lots': ce_filled_qty // lot_size,
                        'pe_quantity': pe_filled_qty, 'ce_quantity': ce_filled_qty,
                        'total_quantity': ce_filled_qty + pe_filled_qty,
                        'ce_token': ce_token, 'ce_symbol': ce_symbol, 'ce_entry_price': avg_ce_fill,
                        'pe_token': pe_token, 'pe_symbol': pe_symbol, 'pe_entry_price': avg_pe_fill,
                        'status': 'BUILDING', 'config': trade_config or {},
                        'entry_spot': chain_data['fut_ltp'],
                        'ce_delta': ce_delta, 'pe_delta': pe_delta,
                        'net_delta': net_delta, 'delta_neutral': delta_neutral,
                    }
                    await loop.run_in_executor(None, state.db.insert_straddle, current_straddle_data)
                    if not is_first_fill_processed:
                        logger.info(
                            f"First chunk verified for {trade_uid}. "
                            "Trade is now live with status 'BUILDING'."
                        )
                        is_first_fill_processed = True

                    # Fetch a fresh snapshot, polling until it's available or times out.
                    snapshot = await _get_fresh_snapshot(trade_uid)

                    try:
                        if snapshot:
                            logger.info(f"Performing in-build HEDGE check for {trade_uid}...")
                            pts_out        = snapshot.get('pts_out', 0.0)
                            points_allowed = snapshot.get('points_allowed', float('inf'))
                            net_delta      = snapshot.get('net_delta', 0.0)
                            if pts_out > points_allowed:
                                log_msg = (
                                    f"HEDGE NEEDED DURING BUILD for {trade_uid}! "
                                    f"Pts Out: {pts_out:.2f} > Allowed: {points_allowed:.2f}. "
                                    "Triggering hedge."
                                )
                                logger.warning(log_msg)
                                await broadcast_log('WARNING', log_msg)
                                from trading.hedger import execute_synthetic_hedge
                                hedge_result = await execute_synthetic_hedge(
                                    trade_uid=trade_uid, net_delta=net_delta,
                                    target_delta_reduction=-net_delta, hedge_type="BUI_HEDGE",
                                    uid_prefix_override=f"BUI_{trade_uid}"
                                )
                                if hedge_result and hedge_result.get('success'):
                                    logger.info(f"In-build hedge for {trade_uid} completed successfully.")
                                    # Re-fetch snapshot after hedge to get latest state
                                    snapshot = await _get_fresh_snapshot(trade_uid)
                                else:
                                    logger.error(
                                        f"In-build hedge for {trade_uid} FAILED. "
                                        "Continuing build with unhedged position."
                                    )
                            else:
                                logger.info(
                                    f"In-build Hedge Check OK for {trade_uid}. "
                                    f"Pts Out: {pts_out:.2f} <= Allowed: {points_allowed:.2f}"
                                )
                        else:
                            logger.warning(f"Could not get snapshot for HEDGE check on {trade_uid}. Skipping.")
                    except Exception as hedge_check_e:
                        logger.error(
                            f"Error during HEDGE check in build process for {trade_uid}: {hedge_check_e}"
                        )

                    try:
                        if not snapshot:
                            logger.warning(
                                f"Could not get snapshot for SL check on {trade_uid}. "
                                "Skipping SL check for this chunk."
                            )
                        else:
                            logger.info(f"Performing in-build SL check for {trade_uid}...")
                            total_pnl        = snapshot.get('total_pnl', 0.0)
                            sl_value_per_lot = snapshot.get('sl_points', 0.0)
                            current_gross_lots = (
                                (ce_filled_qty + pe_filled_qty) / (2.0 * lot_size) if lot_size > 0 else 0
                            )
                            number_of_straddle_units = current_gross_lots * lot_size
                            total_sl_threshold = -1 * sl_value_per_lot * number_of_straddle_units

                            if total_sl_threshold < 0 and total_pnl <= total_sl_threshold:
                                log_msg = (
                                    f"STOP-LOSS HIT DURING BUILD for {trade_uid}! "
                                    f"Total PnL: {total_pnl:.2f}, "
                                    f"Threshold: {total_sl_threshold:.2f} "
                                    f"(Gross Lots: {current_gross_lots:.2f}). "
                                    "Aborting build and squaring off."
                                )
                                logger.critical(log_msg)
                                await broadcast_log('CRITICAL', log_msg)
                                from trading.square_off import square_off
                                logger.info(
                                    f"Initiating immediate square-off for partially built trade {trade_uid}..."
                                )
                                sqf_result = await square_off(
                                    trade_uid=trade_uid, straddle_data=current_straddle_data, reason='SL'
                                )
                                if sqf_result and sqf_result.get('success'):
                                    logger.info(
                                        f"Square-off for {trade_uid} completed successfully "
                                        "after in-build SL hit."
                                    )
                                else:
                                    logger.error(
                                        f"Square-off for {trade_uid} FAILED after in-build SL hit. "
                                        "Position may be open."
                                    )
                                build_aborted = True
                                await loop.run_in_executor(
                                    None, state.db.update_straddle_status, trade_uid, 'CLOSED_SL_BUILD'
                                )
                                break
                            else:
                                pnl_per_unit = (
                                    total_pnl / number_of_straddle_units
                                    if number_of_straddle_units > 0 else 0.0
                                )
                                logger.info(
                                    f"In-build SL Check OK for {trade_uid}. "
                                    f"PnL/Unit: {pnl_per_unit:.2f} > "
                                    f"Threshold/Unit: {-sl_value_per_lot:.2f}"
                                )
                    except Exception as sl_check_e:
                        logger.error(
                            f"Error during SL check in build process for {trade_uid}: {sl_check_e}"
                        )

                orders_to_process = orders_to_retry_now + placement_failures_to_retry
                retry_iter += 1

            if orders_to_process:
                logger.error(f"After all retries, {len(orders_to_process)} orders in chunk {chunk_idx + 1} failed.")
                all_failed_orders.extend(orders_to_process)

            chunk_idx += 1
            await asyncio.sleep(0.05)

        # ═══════════════════════════════════════════════════════════════════════
        # AFTER THE LOOP
        # ═══════════════════════════════════════════════════════════════════════
        if build_aborted:
            final_straddle_data_check = await loop.run_in_executor(
                None, state.db.get_straddle_by_id, trade_uid
            )
            current_status = (
                final_straddle_data_check.get('status')
                if final_straddle_data_check else 'UNKNOWN'
            )
            if current_status == 'CLOSED_SL_BUILD':
                logger.warning(
                    f"Build for {trade_uid} was aborted due to SL hit. "
                    f"Final status: {current_status}"
                )
                return final_straddle_data_check
            logger.warning(
                f"Build for {trade_uid} was aborted mid-way. "
                "Proceeding to finalize with partial data."
            )

        if not all_successful_orders:
            logger.error(f"Build failed for {trade_uid}. No orders were successful.")
            return None

        placed_ids = {str(o.get('app_order_id') or o.get('order_id')) for o in all_successful_orders}
        filled_ids = {
            str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid') or o.get('order_id'))
            for o in all_verified_fills
        }
        ids_to_check = list(placed_ids - filled_ids)

        if ids_to_check:
            logger.info(
                f"Pre-Sweep Check: Re-verifying {len(ids_to_check)} pending orders "
                "before calculating unfilled quantity..."
            )

            async def _pre_sweep():
                return await executor.verify_orders_bulk(
                    ids_to_check, f"BUI_{trade_uid}_PRE_SWEEP",
                    trade_uid=trade_uid, timeout=3.0
                )

            async def _status_update():
                if is_first_fill_processed:
                    await loop.run_in_executor(
                        None, state.db.update_straddle_status, trade_uid, 'BUILDING'
                    )

            pre_sweep_result, _ = await asyncio.gather(_pre_sweep(), _status_update())

            new_fills = pre_sweep_result.get('verified_success', [])
            if new_fills:
                logger.info(f"Pre-Sweep Check: Found {len(new_fills)} new fills. Updating state.")
                all_verified_fills.extend(new_fills)
                state.temp_order_cache.setdefault(trade_uid, []).extend(new_fills)
                app_order_id_to_uid_map = {
                    str(o.get('app_order_id') or o.get('order_id')): o.get('uid')
                    for o in all_successful_orders
                }
                sweep_fills_with_uid = []
                for fill in new_fills:
                    app_oid = str(
                        fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid')
                    )
                    if app_oid in app_order_id_to_uid_map:
                        fill['OrderUniqueIdentifier'] = app_order_id_to_uid_map[app_oid]
                    if fill.get('OrderUniqueIdentifier'):
                        if 'order_unique_id' not in fill:
                            fill['order_unique_id'] = fill.get('OrderUniqueIdentifier')
                        sweep_fills_with_uid.append(fill)
                if sweep_fills_with_uid:
                    state.db.insert_orders_bulk(sweep_fills_with_uid)
                    logger.info(f"Bulk inserted {len(sweep_fills_with_uid)} pre-sweep fills.")

        if not all_verified_fills:
            logger.error(
                f"Build failed for {trade_uid}. "
                "All placed orders failed verification (no fills)."
            )
            if all_successful_orders:
                logger.warning(
                    f"Orders were placed for {trade_uid} but verification failed. "
                    "Setting status to PARTIAL."
                )
                await loop.run_in_executor(
                    None, state.db.update_straddle_status, trade_uid, 'PARTIAL'
                )
            if trade_uid in state.temp_order_cache:
                del state.temp_order_cache[trade_uid]
            return None

        total_ce_filled = sum(
            int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0)
            for f in all_verified_fills if int(f.get('ExchangeInstrumentID') or 0) == ce_token
        )
        total_pe_filled = sum(
            int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0)
            for f in all_verified_fills if int(f.get('ExchangeInstrumentID') or 0) == pe_token
        )
        unfilled_ce        = max(0, ce_contracts - total_ce_filled)
        unfilled_pe        = max(0, pe_contracts - total_pe_filled)
        total_unfilled_qty = unfilled_ce + unfilled_pe

        pending_verification_ids = placed_ids - filled_ids - {
            str(o.get('app_order_id') or o.get('order_id')) for o in all_failed_orders
        }
        if pending_verification_ids:
            logger.warning(
                f"SAFETY ABORT: Cannot sweep for {trade_uid} because "
                f"{len(pending_verification_ids)} orders are placed but unverified. "
                "Proceeding with sweep risks double execution."
            )
            total_unfilled_qty = 0

        max_sweep_attempts = 3
        sweep_attempt      = 0

        while total_unfilled_qty > 0 and not build_aborted and sweep_attempt < max_sweep_attempts:
            sweep_attempt += 1
            sweep_multiplier = sweep_attempt + 1
            sweep_buffer     = sell_buffer * sweep_multiplier

            logger.info(
                f"Final Sweep (Attempt {sweep_attempt}/{max_sweep_attempts}): "
                f"CE unfilled={unfilled_ce}, PE unfilled={unfilled_pe} | "
                f"buffer={sweep_multiplier}x ({sweep_buffer:.1f})"
            )

            sweep_legs = []
            if unfilled_ce > 0:
                sweep_legs.append({
                    'token': ce_token, 'option_type': 'CE', 'action': 'SELL',
                    'total_lots': int(unfilled_ce / lot_size), 'lot_size': lot_size,
                    'expected_price': ce_ltp, 'exchange_segment': exchange_segment,
                    'product_type': product_type
                })
            if unfilled_pe > 0:
                sweep_legs.append({
                    'token': pe_token, 'option_type': 'PE', 'action': 'SELL',
                    'total_lots': int(unfilled_pe / lot_size), 'lot_size': lot_size,
                    'expected_price': pe_ltp, 'exchange_segment': exchange_segment,
                    'product_type': product_type
                })

            if sweep_legs:
                sweep_chunks = generate_chunked_orders(
                    trade_uid_prefix    = f"BUI_{trade_uid}_SW{sweep_attempt}",
                    legs_data           = sweep_legs,
                    base_lots_for_trade = lots,
                    max_order_qty       = max_order_qty,
                    order_lots_per_call = order_lots_per_call,
                )

                for chunk in sweep_chunks:
                    for order in chunk:
                        order['limit_order_buffer'] = sweep_buffer
                        order['limit_price'] = 0.0

                    logger.info(f"Executing Sweep {sweep_attempt} chunk with {len(chunk)} orders...")
                    sweep_result = await executor.execute_batch(
                        chunk, f"BUI_{trade_uid}_SWEEP{sweep_attempt}"
                    )

                    sweep_app_order_id_to_uid_map = {
                        str(o.get('app_order_id')): o.get('uid')
                        for o in sweep_result.get('successful_orders', [])
                    }

                    if sweep_result.get('successful_orders'):
                        sweep_placed_batch = [
                            {
                                'AppOrderID': str(o.get('app_order_id')),
                                'OrderUniqueIdentifier': o.get('uid'),
                                'order_unique_id': o.get('uid'),
                                'ExchangeInstrumentID': o.get('token'),
                                'OrderSide': o.get('action'),
                                'OrderQuantity': o.get('quantity'),
                                'LeavesQuantity': o.get('quantity'),
                                'CumulativeQuantity': 0,
                                'OrderStatus': 'OPEN',
                                'ProductType': o.get('product_type', 'MIS'),
                                'trade_uid': trade_uid
                            }
                            for o in sweep_result['successful_orders']
                        ]
                        try:
                            await loop.run_in_executor(None, state.db.insert_orders_bulk, sweep_placed_batch)
                        except Exception as ins_e:
                            logger.error(f"Failed to bulk-persist sweep placed orders: {ins_e}")

                    sweep_success_ids = [
                        str(o.get('app_order_id'))
                        for o in sweep_result.get('successful_orders', [])
                        if o.get('app_order_id')
                    ]
                    if sweep_success_ids:
                        sweep_verify = await executor.verify_orders_bulk(
                            sweep_success_ids,
                            f"BUI_{trade_uid}_SWEEP{sweep_attempt}_VERIFY",
                            trade_uid=trade_uid
                        )
                        sweep_verified_fills = sweep_verify.get('verified_success', [])

                        for fill in sweep_verified_fills:
                            app_oid = str(
                                fill.get('AppOrderID') or fill.get('app_order_id') or
                                fill.get('apporderid')
                            )
                            if app_oid in sweep_app_order_id_to_uid_map:
                                fill['OrderUniqueIdentifier'] = sweep_app_order_id_to_uid_map[app_oid]
                                fill['order_unique_id']       = fill['OrderUniqueIdentifier']

                        all_verified_fills.extend(sweep_verified_fills)
                        if sweep_verified_fills:
                            state.temp_order_cache.setdefault(trade_uid, []).extend(sweep_verified_fills)
                            sweep_vf_with_uid = [f for f in sweep_verified_fills if f.get('OrderUniqueIdentifier')]
                            if sweep_vf_with_uid:
                                state.db.insert_orders_bulk(sweep_vf_with_uid)

            await asyncio.sleep(0.3)

            total_ce_filled = sum(
                int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0)
                for f in all_verified_fills if int(f.get('ExchangeInstrumentID') or 0) == ce_token
            )
            total_pe_filled = sum(
                int(f.get('CumulativeQuantity') or f.get('filled_qty') or 0)
                for f in all_verified_fills if int(f.get('ExchangeInstrumentID') or 0) == pe_token
            )
            unfilled_ce        = max(0, ce_contracts - total_ce_filled)
            unfilled_pe        = max(0, pe_contracts - total_pe_filled)
            total_unfilled_qty = unfilled_ce + unfilled_pe

            if total_unfilled_qty == 0:
                logger.info(f"Fully filled after sweep attempt {sweep_attempt}!")
            else:
                logger.warning(
                    f"After sweep {sweep_attempt}: "
                    f"still unfilled CE={unfilled_ce}, PE={unfilled_pe}"
                )

        unfilled_count = total_unfilled_qty

        if unfilled_count == 0 and not all_failed_orders:
            logger.info(f"All orders filled for {trade_uid}. Status -> ACTIVE")
            final_status = 'ACTIVE'
        else:
            logger.warning(
                f"Partial build for {trade_uid}: {len(all_verified_fills)} fills, "
                f"{unfilled_count} qty unfilled, {len(all_failed_orders)} failed to place. "
                "Treating as ACTIVE."
            )
            for f_order in all_failed_orders:
                logger.warning(f"  - Failed UID: {f_order.get('uid')}, Reason: {f_order.get('error')}")
            final_status = 'ACTIVE'

        if is_first_fill_processed:
            await loop.run_in_executor(
                None, state.db.update_straddle_status, trade_uid, final_status
            )

        all_fills_to_process = all_verified_fills
        if not all_fills_to_process and all_successful_orders:
            logger.warning(
                f"No verified fills found in cache for {trade_uid}. "
                "Final calculations will be based on zero filled quantity."
            )
            all_fills_to_process = []

        ce_total_value = 0.0; ce_filled_qty = 0
        pe_total_value = 0.0; pe_filled_qty = 0

        for fill in all_fills_to_process:
            avg_price = float(
                fill.get('OrderAverageTradedPrice') or fill.get('fill_price') or
                fill.get('expected_price') or 0.0
            )
            qty = int(
                fill.get('CumulativeQuantity') or fill.get('filled_qty') or
                fill.get('quantity') or 0
            )
            token_val = fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id')
            if not token_val:
                logger.error(f"Final Calc Error: Fill data missing instrument token: {fill}")
                continue
            token = int(token_val)
            if token == ce_token and avg_price > 0 and qty > 0:
                ce_total_value += avg_price * qty; ce_filled_qty += qty
            elif token == pe_token and avg_price > 0 and qty > 0:
                pe_total_value += avg_price * qty; pe_filled_qty += qty

        avg_ce_fill        = (ce_total_value / ce_filled_qty) if ce_filled_qty > 0 else ce_ltp
        avg_pe_fill        = (pe_total_value / pe_filled_qty) if pe_filled_qty > 0 else pe_ltp
        executed_ce_qty    = ce_filled_qty
        executed_pe_qty    = pe_filled_qty
        executed_total_qty = executed_ce_qty + executed_pe_qty
        executed_ce_lots   = executed_ce_qty // lot_size if lot_size > 0 else 0
        executed_pe_lots   = executed_pe_qty // lot_size if lot_size > 0 else 0

        straddle_data = {
            'straddle_id': trade_uid,
            'trade_uid':   trade_uid,
            'symbol':      symbol,
            'strike':      atm,
            'expiry':      chain_data['expiry'],
            'expiry_date': chain_data.get('expiry_date'),
            'exchange_segment': exchange_segment,
            'exchange_name':    exchange_name,
            'product_type':     product_type,
            'lot_size': lot_size,
            'lots':     lots,
            'initial_pe_quantity': pe_contracts,
            'initial_ce_quantity': ce_contracts,
            'pe_lots':     executed_pe_lots,
            'ce_lots':     executed_ce_lots,
            'pe_quantity': executed_pe_qty,
            'ce_quantity': executed_ce_qty,
            'quantity':    0,
            'total_quantity': executed_total_qty,
            'ce_token':  ce_token,
            'ce_symbol': ce_symbol,
            'ce_entry_price': avg_ce_fill,
            'ce_delta': ce_delta, 'ce_gamma': ce_row.get('ce_gamma', 0),
            'ce_theta': ce_row.get('ce_theta', 0), 'ce_vega': ce_row.get('ce_vega', 0),
            'ce_iv':    ce_row.get('ce_iv', 0),
            'pe_token':  pe_token,
            'pe_symbol': pe_symbol,
            'pe_entry_price': avg_pe_fill,
            'pe_delta': pe_delta, 'pe_gamma': pe_row.get('pe_gamma', 0),
            'pe_theta': pe_row.get('pe_theta', 0), 'pe_vega': pe_row.get('pe_vega', 0),
            'pe_iv':    pe_row.get('pe_iv', 0),
            'net_delta':    net_delta,
            'delta_neutral': delta_neutral,
            'total_premium': (avg_ce_fill * executed_ce_qty) + (avg_pe_fill * executed_pe_qty),
            'status':         final_status,
            'execution_time': total_execution_time,
            'entry_spot':     chain_data['fut_ltp'],
            'spot_price':     chain_data['fut_ltp'],
            'fut_token':      chain_data.get('fut_token'),
            'entry_timestamp': get_ist_now().isoformat(),
            'closed_at': None,
            'config':    trade_config or {},
            'ce_orders': [o for o in all_fills_to_process if o.get('ExchangeInstrumentID') == ce_token],
            'pe_orders': [o for o in all_fills_to_process if o.get('ExchangeInstrumentID') == pe_token],
        }

        ce_order_ids = [
            str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid'))
            for o in straddle_data['ce_orders']
        ]
        pe_order_ids = [
            str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid'))
            for o in straddle_data['pe_orders']
        ]
        straddle_data['ce_order_id']     = ','.join(ce_order_ids) if ce_order_ids else ''
        straddle_data['ce_app_order_id'] = ','.join(ce_order_ids) if ce_order_ids else ''
        straddle_data['pe_order_id']     = ','.join(pe_order_ids) if pe_order_ids else ''
        straddle_data['pe_app_order_id'] = ','.join(pe_order_ids) if pe_order_ids else ''

        for order in all_fills_to_process:
            order_id = order.get('app_order_id') or order.get('order_id')
            if order_id:
                state.map_order_to_trade(str(order_id), trade_uid)

        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
            logger.info(f"Straddle saved: {trade_uid}")
        except Exception as e:
            logger.error(f"Failed to save straddle to DB: {e}")

        if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
            del state.temp_order_cache[trade_uid]
            logger.info(f"Cleared temp order cache for {trade_uid} after build.")

        logger.info("Instrument subscription handled by option chain builder.")

        end_time   = get_ist_now()
        total_time = (end_time - start_time).total_seconds()

        logger.info("=" * 100)
        logger.info(
            f"[{exchange_name}] BUILD_{trade_uid} COMPLETE | "
            f"Status: {final_status} | Time: {total_time:.2f}s"
        )
        logger.info(
            f"Executed: PE={executed_pe_qty}, CE={executed_ce_qty} | "
            f"Target Net={net_delta:.4f}"
        )
        logger.info(f"Total Premium: {straddle_data['total_premium']:,.2f}")
        logger.info(f"   CE: {executed_ce_qty} @ {avg_ce_fill:.2f}")
        logger.info(f"   PE: {executed_pe_qty} @ {avg_pe_fill:.2f}")
        logger.info("=" * 100)

        if final_status in ['ACTIVE', 'PARTIAL']:
            logger.info(f"Spawning dedicated process for trade {trade_uid} (status={final_status})...")
            from trading.trade_process import trade_process_worker_entry
            command_q = multiprocessing.Queue()
            process = multiprocessing.Process(
                target=trade_process_worker_entry,
                args=(
                    trade_uid,
                    straddle_data,
                    command_q,
                    dict(state.option_chains) if state.option_chains else {},
                    getattr(state, 'trade_data_cache', None) or {},
                    all_verified_fills
                ),
                daemon=True,
                name=f"trade-{trade_uid}"
            )
            process.start()
            state.trade_processes[trade_uid] = {'process': process, 'command_q': command_q}
            logger.info(f"Process for {trade_uid} started (PID: {process.pid}) and registered.")

        return {
            "success":       final_status == 'ACTIVE',
            "straddle_data": straddle_data,
            "message":       f"Straddle build completed with status: {final_status}"
        }

    except Exception as e:
        logger.error(f"Build failed: {e}", exc_info=True)
        if 'trade_uid' in locals() and trade_uid:
            try:
                loop = asyncio.get_event_loop()
                trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                if trade and trade.get('status') == 'BUILDING':
                    await loop.run_in_executor(
                        None, state.db.update_straddle_status, trade_uid, 'PARTIAL'
                    )
                    logger.warning(
                        f"Build failed with exception. Reverted status to PARTIAL for "
                        f"{trade_uid} as it may be partially built."
                    )
                executor = get_order_executor()
                if executor:
                    await executor.cancel_all_open_orders_for_trade(trade_uid)
            except Exception as db_e:
                logger.error(
                    f"CRITICAL: Failed to revert status or cancel orders for {trade_uid} after build exception. "
                    f"DB Error: {db_e}"
                )
    finally:
        if 'trade_uid' in locals() and trade_uid:
            if hasattr(state, 'temp_order_cache') and trade_uid in state.temp_order_cache:
                del state.temp_order_cache[trade_uid]
                logger.info(
                    f"Final cleanup: Cleared temp order cache for {trade_uid} "
                    "after build attempt."
                )


async def get_correct_lot_size(straddle_data: Dict) -> int:
    symbol = straddle_data.get("symbol", "NIFTY").upper()
    option_chain = state.get_option_chain(symbol)
    if option_chain and option_chain.get('lot_size', 0) > 0:
        return option_chain['lot_size']
    try:
        fresh_chain = await get_option_chain_from_service(symbol)
        if fresh_chain and fresh_chain.get('lot_size', 0) > 0:
            state.update_option_chain(symbol, fresh_chain)
            return fresh_chain['lot_size']
    except Exception:
        pass
    db_lot_size = straddle_data.get('lot_size', 0)
    if db_lot_size > 0:
        return db_lot_size
    base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol), None)
    if base_symbol:
        return SYMBOL_CONFIG.get(base_symbol, {}).get('lot_size', 65)
    return 65


async def build_multi_straddle(
    symbol: str,
    lots: int,
    count: int = 1,
    delta_neutral: bool = True,
    product_type: str = "MIS",
    strike_range: int = 15,
    delay_seconds: float = 1.0,
    trade_config: Dict = None
) -> List[Dict]:
    straddles = []
    logger.info("=" * 100)
    logger.info(f"BUILDING {count} STRADDLES")
    logger.info("=" * 100)

    for i in range(count):
        logger.info(f"Building straddle {i + 1}/{count}")
        straddle = await build_straddle(
            symbol=symbol, lots=lots, delta_neutral=delta_neutral,
            product_type=product_type, strike_range=strike_range, trade_config=trade_config
        )
        if straddle:
            straddles.append(straddle)
            logger.info(f"Straddle {i + 1}/{count} built: {straddle['trade_uid']}")
        else:
            logger.error(f"Straddle {i + 1}/{count} failed")

        if i < count - 1 and delay_seconds > 0:
            logger.info(f"Waiting {delay_seconds}s before next straddle...")
            await asyncio.sleep(delay_seconds)

    logger.info("=" * 100)
    logger.info(f"Built {len(straddles)}/{count} straddles successfully")
    logger.info("=" * 100)
    return straddles


def validate_straddle_data(straddle_data: Dict) -> bool:
    try:
        required_fields = [
            'straddle_id', 'trade_uid', 'symbol', 'strike', 'expiry',
            'ce_token', 'pe_token', 'ce_quantity', 'pe_quantity'
        ]
        for field in required_fields:
            if field not in straddle_data or straddle_data[field] is None:
                logger.error(f"Missing required field: {field}")
                return False
        if straddle_data['ce_quantity'] <= 0 or straddle_data['pe_quantity'] <= 0:
            logger.error(f"Invalid quantities: CE={straddle_data['ce_quantity']}, PE={straddle_data['pe_quantity']}")
            return False
        if straddle_data.get('ce_entry_price', 0) <= 0 or straddle_data.get('pe_entry_price', 0) <= 0:
            logger.error(f"Invalid entry prices: CE={straddle_data.get('ce_entry_price')}, PE={straddle_data.get('pe_entry_price')}")
            return False
        logger.debug(f"Straddle data validation passed for {straddle_data['trade_uid']}")
        return True
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False


async def manual_sync_trade_orders(trade_uid: str) -> Dict:
    logger.info(f"MANUAL SYNC initiated for trade {trade_uid}")
    loop     = asyncio.get_event_loop()
    executor = get_order_executor()
    if not executor:
        return {"success": False, "error": "OrderExecutor not initialized"}

    db_orders_for_trade = await loop.run_in_executor(
        None, state.db.get_orders_by_trade_id, trade_uid
    )
    db_order_map = {
        str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')): o
        for o in db_orders_for_trade
    }
    logger.info(f"Found {len(db_order_map)} orders for {trade_uid} in the database.")

    try:
        if executor.client_id:
            order_book_func = functools.partial(
                executor.xt_i.get_order_book, clientID=executor.client_id
            )
        else:
            order_book_func = executor.xt_i.get_order_book
        broker_order_book = await loop.run_in_executor(None, order_book_func)
        if not broker_order_book or broker_order_book.get('type') != 'success':
            error_msg = broker_order_book.get('description', 'Unknown error')
            logger.error(f"Manual Sync: Order book fetch failed: {error_msg}")
            return {"success": False, "error": f"Broker order book fetch failed: {error_msg}"}
        broker_orders    = broker_order_book.get('result', [])
        broker_order_map = {str(o.get('AppOrderID')): o for o in broker_orders if o.get('AppOrderID')}
        logger.info(f"Fetched {len(broker_orders)} total orders from broker.")
    except Exception as e:
        logger.error(f"Manual Sync: Exception during order book fetch: {e}", exc_info=True)
        return {"success": False, "error": f"Exception during order book fetch: {e}"}

    newly_found_orders = []
    orders_to_update   = []

    for broker_order in broker_orders:
        app_order_id = str(broker_order.get('AppOrderID'))
        if not app_order_id:
            continue
        is_trade_order  = False
        uid_from_broker = broker_order.get('OrderUniqueIdentifier', '')
        if trade_uid in uid_from_broker:
            is_trade_order = True
        elif app_order_id in db_order_map:
            is_trade_order = True
        if not is_trade_order:
            continue

        broker_status = str(broker_order.get('OrderStatus', 'UNKNOWN')).upper()
        if app_order_id not in db_order_map:
            if broker_status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED',
                                  'CANCELLED', 'CANCELED', 'REJECTED']:
                logger.info(
                    f"SYNC: Found new order for {trade_uid} at broker: "
                    f"ID {app_order_id}, Status: {broker_status}"
                )
                newly_found_orders.append(broker_order)
        else:
            db_order  = db_order_map[app_order_id]
            db_status = str(db_order.get('order_status', 'UNKNOWN')).upper()
            if broker_status != db_status and broker_status in [
                'FILLED', 'COMPLETE', 'TRADED', 'EXECUTED',
                'CANCELLED', 'CANCELED', 'REJECTED'
            ]:
                logger.info(
                    f"SYNC: Status mismatch for {trade_uid} ID {app_order_id}. "
                    f"DB: {db_status}, Broker: {broker_status}. Marking for update."
                )
                orders_to_update.append(broker_order)

    NON_TERMINAL_STATUSES = [
        'PENDINGNEW', 'NEW', 'OPEN', 'REPLACED',
        'PENDINGCANCEL', 'PENDINGREPLACE', 'PARTIALLYFILLED'
    ]
    for app_order_id, db_order in db_order_map.items():
        if app_order_id not in broker_order_map:
            db_status = str(db_order.get('order_status', 'UNKNOWN')).upper()
            if db_status in NON_TERMINAL_STATUSES:
                logger.warning(
                    f"SYNC: Ghost order found for {trade_uid}. "
                    f"ID {app_order_id} is in DB as '{db_status}' but NOT in broker order book. "
                    "Marking as REJECTED."
                )
                updated_ghost_order = dict(db_order)
                updated_ghost_order['OrderStatus']        = 'REJECTED'
                updated_ghost_order['CancelRejectReason'] = (
                    'Not found in broker order book during manual sync'
                )
                orders_to_update.append(updated_ghost_order)

    if newly_found_orders:
        logger.info(f"Inserting {len(newly_found_orders)} newly found orders into DB for {trade_uid}.")
        for order_data in newly_found_orders:
            order_data['order_unique_id'] = order_data.get('OrderUniqueIdentifier')
            await loop.run_in_executor(None, state.db.insert_order, order_data)

    if orders_to_update:
        logger.info(f"Updating {len(orders_to_update)} orders with status changes in DB for {trade_uid}.")
        for order_data in orders_to_update:
            if 'order_unique_id' not in order_data:
                order_data['order_unique_id'] = order_data.get('OrderUniqueIdentifier')
            await loop.run_in_executor(None, state.db.insert_order, order_data)

    if not newly_found_orders and not orders_to_update:
        logger.info(f"Manual Sync for {trade_uid}: No discrepancies found. Database is up to date.")
        return {"success": True, "message": "No discrepancies found."}

    logger.info(f"Triggering snapshot for {trade_uid} after manual sync.")
    await trigger_snapshot_and_broadcast(trade_uid, bypass_debounce=True)

    return {
        "success": True,
        "message": (
            f"Sync complete. Found {len(newly_found_orders)} new orders, "
            f"updated {len(orders_to_update)} orders."
        )
    }


async def get_straddle_pnl(trade_uid: str) -> Optional[Dict]:
    try:
        straddle = state.db.get_straddle(trade_uid)
        if not straddle:
            logger.error(f"Straddle {trade_uid} not found")
            return None

        ce_token = straddle['ce_token']
        pe_token = straddle['pe_token']
        ce_ltp   = state.get_price(ce_token) or 0.0
        pe_ltp   = state.get_price(pe_token) or 0.0

        if ce_ltp <= 0 or pe_ltp <= 0:
            logger.warning(f"Invalid current prices for {trade_uid}: CE={ce_ltp}, PE={pe_ltp}")
            return None

        ce_entry = straddle['ce_entry_price']
        pe_entry = straddle['pe_entry_price']
        ce_qty   = straddle['ce_quantity']
        pe_qty   = straddle['pe_quantity']

        ce_pnl    = (ce_entry - ce_ltp) * ce_qty
        pe_pnl    = (pe_entry - pe_ltp) * pe_qty
        total_pnl = ce_pnl + pe_pnl

        total_premium = straddle['total_premium']
        pnl_percent   = (total_pnl / total_premium * 100) if total_premium > 0 else 0

        return {
            'trade_uid':     trade_uid,
            'ce_ltp':        ce_ltp,
            'pe_ltp':        pe_ltp,
            'ce_pnl':        ce_pnl,
            'pe_pnl':        pe_pnl,
            'total_pnl':     total_pnl,
            'pnl_percent':   pnl_percent,
            'total_premium': total_premium
        }
    except Exception as e:
        logger.error(f"Error calculating P&L for {trade_uid}: {e}")
        return None


async def resume_pending_trade(trade_uid: str) -> None:
    try:
        loop = asyncio.get_running_loop()
        trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)

        if not trade:
            logger.error(f"Resume failed: Trade {trade_uid} not found in DB.")
            return

        if trade.get('status') != 'PENDING':
            logger.warning(f"Trade {trade_uid} is {trade.get('status')}, not PENDING. Skipping resume.")
            return

        logger.info(f"Resuming PENDING trade {trade_uid}...")

        config = trade.get('config', {})
        entry_time_str = config.get('entry_time')

        if entry_time_str:
            now = get_ist_now()
            try:
                entry_dt = datetime.combine(now.date(), datetime.strptime(entry_time_str, "%H:%M:%S").time())
            except ValueError:
                try:
                    entry_dt = datetime.combine(now.date(), datetime.strptime(entry_time_str, "%H:%M").time())
                except ValueError:
                    entry_dt = now

            wait_seconds = (entry_dt - now).total_seconds()
            if wait_seconds > 0:
                logger.info(f"Trade {trade_uid} scheduled for {entry_time_str}. Waiting {wait_seconds:.1f}s...")
                await asyncio.sleep(wait_seconds)
            else:
                logger.info(f"Trade {trade_uid} schedule ({entry_time_str}) is in the past. Executing NOW.")

        symbol        = trade.get('symbol')
        lots          = trade.get('lots')
        delta_neutral = trade.get('delta_neutral', True)
        product_type  = trade.get('product_type', 'MIS')

        await build_straddle(
            symbol=symbol, lots=lots, trade_uid=trade_uid,
            delta_neutral=delta_neutral, product_type=product_type,
            trade_config=config
        )

    except Exception as e:
        logger.error(f"Error resuming trade {trade_uid}: {e}", exc_info=True)
