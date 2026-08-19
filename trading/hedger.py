"""
Hedger - Synthetic Future Delta Hedging
Uses same UID as parent trade
"""
import asyncio
import time
import functools
from datetime import datetime
from typing import Dict, List, Optional

from utils.logger import logger
from models.state import state
from market_data import SYMBOL_CONFIG, get_spot_details
from trading.order_executor import get_order_executor
from trading.order_batching_utils import generate_chunked_orders
from trading.data_client import get_option_chain_from_service


async def hedge_position(
    trade_uid: str,
    hedge_orders: List[Dict],
    hedge_type: str = "DELTA"
) -> Optional[Dict]:
    """
    ⚡ HEDGE POSITION - Generic batch executor

    Args:
        trade_uid: Parent trade UID
        hedge_orders: List of hedge orders (a single chunk)
        hedge_type: DELTA/GAMMA/VEGA/PRE_SQF_NEUTRAL

    Returns:
        Hedge result from OrderExecutor
    """
    if not hasattr(state, 'trade_fill_cache') or state.trade_fill_cache is None:
        state.trade_fill_cache = {}

    try:
        executor = get_order_executor()
        if not executor:
            logger.error("❌ OrderExecutor not initialized")
            return None

        logger.debug("=" * 100)
        logger.info(f"🛡️  HEDGE {hedge_type} | Trade UID: {trade_uid}")
        logger.debug("=" * 100)

        orders_to_process_in_batch = list(hedge_orders)
        max_execution_retries = 2
        execution_attempt = 0

        for order in orders_to_process_in_batch:
            if 'base_buffer' not in order:
                order['base_buffer'] = order.get('limit_order_buffer', 2.0)

        all_verified_fills_for_batch = []
        all_successful_placements_for_batch = []
        all_failed_placements_for_batch = []
        all_verification_failures = []
        unverified_order_ids = []

        while orders_to_process_in_batch and execution_attempt <= max_execution_retries:
            if execution_attempt > 0:
                multiplier = execution_attempt + 1
                logger.info(
                    f"🔄 Re-executing {len(orders_to_process_in_batch)} orders for HEDGE "
                    f"{hedge_type} (Attempt {execution_attempt + 1}) | buffer={multiplier}x..."
                )
                await asyncio.sleep(0.5)

                for order in orders_to_process_in_batch:
                    base = order.get('base_buffer', 2.0)
                    order['limit_order_buffer'] = base * multiplier
                    order['limit_price'] = 0.0
                    logger.info(
                        f"   -> For UID {order['uid']}, new buffer is "
                        f"{order['limit_order_buffer']:.1f}"
                    )

            result = await executor.execute_batch(
                orders_to_process_in_batch,
                f"HEDGE_{hedge_type}_{trade_uid}_EXEC{execution_attempt + 1}"
            )

            successful_placements = result.get('successful_orders', [])
            all_successful_placements_for_batch.extend(successful_placements)
            all_failed_placements_for_batch.extend(result.get('failed_orders', []))

            if successful_placements:
                # --- FIX: Corrected list comprehension syntax ---
                placed_order_ids = [
                    str(o.get('order_id') or o.get('app_order_id'))
                    for o in successful_placements
                    if o.get('order_id') or o.get('app_order_id')
                ]
            # --- END FIX ---
            app_order_id_to_uid_map = {
                str(o.get('app_order_id')): o.get('uid')
                for o in successful_placements
            }

            verified_fills_for_attempt = []
            unverified_order_ids = list(placed_order_ids)
            max_verification_attempts = 2
            orders_to_reexecute = []
            newly_failed = []
            terminal_failure_statuses = {
                'REJECTED', 'CANCELLED', 'CANCELED',
                'NOT_FOUND_ON_RETRY', 'CANCEL_FAILED', 'MODIFY_FAILED'
            }

            for v_attempt in range(max_verification_attempts):
                if not unverified_order_ids:
                    break

                logger.info(
                    f"📊 Verifying HEDGE batch, execution {execution_attempt + 1}, "
                    f"verification {v_attempt + 1}/{max_verification_attempts} for "
                    f"{len(unverified_order_ids)} orders..."
                )

                verification_result = await executor.verify_orders_bulk(
                    unverified_order_ids,
                    f"HEDGE_{hedge_type}_{trade_uid}_VERIFY{execution_attempt + 1}.{v_attempt + 1}",
                    trade_uid=trade_uid
                )

                if verification_result:
                    newly_verified = verification_result.get('verified_success', [])
                    newly_failed = verification_result.get('verified_failed', [])
                    verified_fills_for_attempt.extend(newly_verified)

                    verified_ids = {
                        str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid'))
                        for o in newly_verified
                    }
                    reexecute_ids = {
                        str(o.get('order_id'))
                        for o in newly_failed
                        if o.get('status') == 'REEXECUTE_NEEDED'
                    }
                    failed_ids = {
                        str(o.get('order_id'))
                        for o in newly_failed
                        if o.get('status') in terminal_failure_statuses
                    }
                    resolved_ids = verified_ids.union(failed_ids).union(reexecute_ids)

                    unverified_order_ids = [
                        oid for oid in unverified_order_ids if oid not in resolved_ids
                    ]

                if unverified_order_ids:
                    try:
                        db_orders = await asyncio.get_event_loop().run_in_executor(
                            None, state.db.get_orders_by_trade_id, trade_uid
                        )
                        db_order_map = {
                            str(
                                o.get('AppOrderID')
                                or o.get('app_order_id')
                                or o.get('apporderid')
                            ): o
                            for o in db_orders
                        }

                        still_unverified = []
                        for oid in unverified_order_ids:
                            db_order = db_order_map.get(str(oid))
                            if db_order:
                                status = str(
                                    db_order.get('order_status')
                                    or db_order.get('OrderStatus')
                                    or ''
                                ).upper()
                                if status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']:
                                    logger.info(f"✅ Found hedge order {oid} as {status} in DB.")
                                    fill = {
                                        'AppOrderID': oid,
                                        'OrderUniqueIdentifier': db_order.get('order_unique_id') or db_order.get('OrderUniqueIdentifier'),
                                        'order_unique_id': db_order.get('order_unique_id') or db_order.get('OrderUniqueIdentifier'),
                                        'ExchangeInstrumentID': db_order.get('exchange_instrument_id') or db_order.get('ExchangeInstrumentID'),
                                        'CumulativeQuantity': db_order.get('cumulative_quantity') or db_order.get('CumulativeQuantity'),
                                        'OrderAverageTradedPrice': db_order.get('order_avg_price') or db_order.get('OrderAverageTradedPrice'),
                                        'OrderSide': db_order.get('order_side') or db_order.get('OrderSide'),
                                        'OrderStatus': status,
                                    }
                                    verified_fills_for_attempt.append(fill)
                                else:
                                    still_unverified.append(oid)
                            else:
                                still_unverified.append(oid)

                        unverified_order_ids = still_unverified
                    except Exception as e:
                        logger.error(f"❌ Hedge DB check error: {e}")

                    if unverified_order_ids:
                        logger.warning(
                            f"⚠️ {len(unverified_order_ids)} orders still pending in HEDGE batch. "
                            f"Retrying verification in 3.0s..."
                        )
                        await asyncio.sleep(3.0)

            for failed_order_info in newly_failed:
                if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                    order_id = str(failed_order_info.get('order_id'))
                    original_order_uid = app_order_id_to_uid_map.get(order_id)
                    if original_order_uid:
                        original_order_data = next(
                            (o for o in orders_to_process_in_batch if o.get('uid') == original_order_uid),
                            None
                        )
                        if original_order_data:
                            order_for_re_execution = original_order_data.copy()
                            order_for_re_execution['limit_price'] = 0.0
                            old_uid = original_order_data.get('uid', '')
                            if old_uid and old_uid[0].isalpha():
                                next_char = chr(ord(old_uid[0]) + 1)
                                if next_char > 'Z' and old_uid[0].isupper():
                                    next_char = 'A'
                                if next_char > 'z' and old_uid[0].islower():
                                    next_char = 'a'
                                new_uid = f"{next_char}{old_uid[1:]}"
                            else:
                                new_uid = f"{old_uid}R"[:20]
                            order_for_re_execution['uid'] = new_uid
                            orders_to_reexecute.append(order_for_re_execution)
                            logger.info(
                                f"🔄 Order {original_order_uid} marked for re-execution with new UID {new_uid}."
                            )

                if failed_order_info.get('status') in terminal_failure_statuses:
                    all_verification_failures.append(failed_order_info)

            all_verified_fills_for_batch.extend(verified_fills_for_attempt)
            orders_to_process_in_batch = orders_to_reexecute
            execution_attempt += 1

        if unverified_order_ids:
            logger.critical(
                f"❌ FAILED to verify {len(unverified_order_ids)} orders in HEDGE batch "
                f"'{hedge_type}' after all retries."
            )

        fills_to_process = all_verified_fills_for_batch
        if not fills_to_process and hedge_orders:
            logger.warning(
                f"⚠️ Verification for HEDGE {trade_uid} returned no fills. "
                f"Orders will not be saved to DB for this operation."
            )

        if fills_to_process:

            state.trade_fill_cache.setdefault(
                trade_uid,
                []
            ).extend(
                fills_to_process
            )

            logger.info(
                f"Cached {len(fills_to_process)} "
                f"hedge orders under key '{trade_uid}'."
            )

            # ====================================================
            # VERIFIED HEDGE FILLS -> DATABASE
            # ====================================================

            orders_inserted_count = 0

            if hasattr(
                state.db,
                "insert_order"
            ):

                # Map AppOrderID to original placement metadata.
                original_order_map = {}

                for _order in (
                    all_successful_placements_for_batch
                ):

                    _app_id = (
                        _order.get("app_order_id")
                        or _order.get("order_id")
                        or _order.get("AppOrderID")
                    )

                    if _app_id:
                        original_order_map[
                            str(_app_id)
                        ] = _order

                for fill_data in fills_to_process:

                    try:

                        app_order_id = str(
                            fill_data.get(
                                "AppOrderID"
                            )
                            or fill_data.get(
                                "app_order_id"
                            )
                            or fill_data.get(
                                "apporderid"
                            )
                            or fill_data.get(
                                "order_id"
                            )
                            or ""
                        )

                        src = original_order_map.get(
                            app_order_id,
                            {}
                        )

                        uid = (
                            fill_data.get(
                                "OrderUniqueIdentifier"
                            )
                            or fill_data.get(
                                "order_unique_id"
                            )
                            or src.get(
                                "uid"
                            )
                            or src.get(
                                "OrderUniqueIdentifier"
                            )
                            or src.get(
                                "order_unique_id"
                            )
                        )

                        if not uid:

                            logger.warning(
                                f"⚠️ Hedge fill has no UID | "
                                f"Trade={trade_uid} | "
                                f"AppOrderID={app_order_id}"
                            )

                            continue

                        # ------------------------------------------------
                        # DB identity
                        # ------------------------------------------------

                        fill_data[
                            "OrderUniqueIdentifier"
                        ] = uid

                        fill_data[
                            "order_unique_id"
                        ] = uid

                        fill_data[
                            "trade_uid"
                        ] = trade_uid

                        # ------------------------------------------------
                        # Instrument
                        # ------------------------------------------------

                        token = (
                            fill_data.get(
                                "ExchangeInstrumentID"
                            )
                            or fill_data.get(
                                "exchange_instrument_id"
                            )
                            or src.get(
                                "token"
                            )
                        )

                        fill_data[
                            "ExchangeInstrumentID"
                        ] = token

                        fill_data[
                            "exchange_instrument_id"
                        ] = token

                        # ------------------------------------------------
                        # Exchange
                        # ------------------------------------------------

                        segment = (
                            fill_data.get(
                                "ExchangeSegment"
                            )
                            or fill_data.get(
                                "exchange_segment"
                            )
                            or src.get(
                                "exchange_segment"
                            )
                        )

                        fill_data[
                            "ExchangeSegment"
                        ] = segment

                        fill_data[
                            "exchange_segment"
                        ] = segment

                        # ------------------------------------------------
                        # Side
                        # ------------------------------------------------

                        side = (
                            fill_data.get(
                                "OrderSide"
                            )
                            or fill_data.get(
                                "order_side"
                            )
                            or src.get(
                                "action"
                            )
                        )

                        fill_data[
                            "OrderSide"
                        ] = side

                        fill_data[
                            "order_side"
                        ] = side

                        # ------------------------------------------------
                        # Quantity
                        # ------------------------------------------------

                        qty = (
                            fill_data.get(
                                "CumulativeQuantity"
                            )
                            or fill_data.get(
                                "filled_qty"
                            )
                            or fill_data.get(
                                "quantity"
                            )
                            or src.get(
                                "quantity"
                            )
                            or 0
                        )

                        fill_data[
                            "CumulativeQuantity"
                        ] = qty

                        fill_data[
                            "OrderQuantity"
                        ] = (
                            fill_data.get(
                                "OrderQuantity"
                            )
                            or qty
                        )

                        fill_data[
                            "quantity"
                        ] = (
                            fill_data.get(
                                "quantity"
                            )
                            or qty
                        )

                        # ------------------------------------------------
                        # Product
                        # ------------------------------------------------

                        product = (
                            fill_data.get(
                                "ProductType"
                            )
                            or fill_data.get(
                                "product_type"
                            )
                            or src.get(
                                "product_type"
                            )
                            or "MIS"
                        )

                        fill_data[
                            "ProductType"
                        ] = product

                        fill_data[
                            "product_type"
                        ] = product

                        # ------------------------------------------------
                        # Verified status
                        # ------------------------------------------------

                        fill_data[
                            "OrderStatus"
                        ] = "FILLED"

                        fill_data[
                            "order_status"
                        ] = "FILLED"

                        # ------------------------------------------------
                        # Persist into DB
                        # ------------------------------------------------

                        state.db.insert_order(
                            fill_data
                        )

                        orders_inserted_count += 1

                        logger.info(
                            f"💾 HEDGE FILL PERSISTED | "
                            f"Trade={trade_uid} | "
                            f"AppOrderID={app_order_id} | "
                            f"Token={token} | "
                            f"Side={side} | "
                            f"Qty={qty}"
                        )

                    except Exception as insert_err:

                        logger.exception(
                            f"❌ HEDGE DB INSERT FAILED | "
                            f"Trade={trade_uid} | "
                            f"AppOrderID={app_order_id} | "
                            f"Error={insert_err}"
                        )

                logger.info(
                    f"✅ HEDGE DB PERSIST COMPLETE | "
                    f"Trade={trade_uid} | "
                    f"Inserted={orders_inserted_count} | "
                    f"Verified={len(fills_to_process)}"
                )

            else:

                logger.error(
                    f"❌ state.db.insert_order() unavailable | "
                    f"Trade={trade_uid}"
                )

        success_flag = (
            len(all_failed_placements_for_batch) == 0 and
            len(unverified_order_ids) == 0 and
            len(all_verification_failures) == 0
        )
        logger.info(
            f"Hedge position debug: success={success_flag}, "
            f"failed_placements={len(all_failed_placements_for_batch)}, "
            f"unverified={len(unverified_order_ids)}, "
            f"verification_failures={len(all_verification_failures)}"
        )

        return {
            "successful_orders": fills_to_process,
            "failed_orders": all_failed_placements_for_batch,
            "successful_count": len(fills_to_process),
            "failed_count": (
                len(all_failed_placements_for_batch)
                + len(unverified_order_ids)
                + len(all_verification_failures)
            ),
            "total_orders": len(hedge_orders),
            "execution_time": result.get('execution_time', 0.0) if 'result' in locals() else 0.0,
            "success": success_flag
        }

    except Exception as e:
        logger.error(f"❌ Hedge failed: {e}", exc_info=True)
        return None


async def execute_synthetic_hedge(
    trade_uid: str,
    net_delta: float,
    target_delta_reduction: Optional[float] = None,
    hedge_type: str = "DELTA", # Can also be "PRE_SQF_NEUTRAL"
    atm_strike_override: Optional[int] = None, # NEW
    uid_prefix_override: Optional[str] = None # NEW
) -> Dict:
    try:
        executor = get_order_executor()
        if not executor:
            raise RuntimeError("OrderExecutor not available for hedging.")
        """
        ⚡ ULTRA-FAST synthetic future hedge for delta neutralization.
        Executes in two 50% batches with verification in between.

        Formula:
            target_reduction = round(hedge_fraction * |delta|, lot_size)
            quantity_needed = target_reduction contracts per leg
            lots = quantity_needed / lot_size_

        Args:
            trade_uid: Trade to hedge
            net_delta: Current net delta from HedgeMonitor
            target_delta_reduction: Precomputed target (from HedgeMonitor), or full |delta|
            hedge_type: "DELTA" (default)
            uid_prefix_override: Force a specific prefix for the OrderUniqueIdentifier.

        Returns:
            Execution result with meta
        """
        start_time = time.time()

        # --- FIX: Add a critical defensive check to prevent hedging a trade that is being closed ---
        # This prevents a race condition where a hedge event is processed for a trade
        # that has just been marked for square-off by another process (e.g., SL monitor).
        if hedge_type != "PRE_SQF_NEUTRAL" and hasattr(state, 'closing_trades') and trade_uid in state.closing_trades:
            logger.error(f"❌ Hedge ({hedge_type}) for {trade_uid} aborted. Trade is currently being squared off.")
            return {"success": False, "error": "Trade is being squared off."}
        # --- END FIX ---

        # Get trade data
        trade = state.db.get_straddle_by_id(trade_uid)
        if not trade:
            return {"success": False, "error": f"Trade not found: {trade_uid}"}

        symbol = trade.get("symbol", "NIFTY")
        lot_size = int(trade.get("lot_size", 0) or 0)  # Get from DB first

        # --- ROBUSTNESS FIX: Re-derive exchange segment from symbol ---
        # This prevents using a stale segment from an old DB record.
        symbol_upper = symbol.upper()
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        
        derived_segment = SYMBOL_CONFIG.get(base_symbol, {}).get('segment') if base_symbol else None
        if derived_segment:
            segment = derived_segment
        else:
            segment = trade.get("exchange_segment", 2)  # Fallback to DB or default
            logger.warning(f"Could not derive segment for {symbol_upper} from SYMBOL_CONFIG. Falling back to DB value: {segment}.")
        # --- END FIX ---

        # --- RESILIENCY: If lot_size from DB is invalid, fetch it fresh ---
        if lot_size <= 0:
            logger.warning(f"⚠️ Invalid lot_size={lot_size} from DB for {trade_uid}. Attempting to fetch fresh lot size.")
            # --- FIX: Allow network fallback for spot_details if cache is empty ---
            spot_details = await get_spot_details(symbol, use_cache_only=False)
            # --- END FIX ---
            if spot_details and spot_details.get('lot_size', 0) > 0:
                lot_size = spot_details['lot_size']
                logger.info(f"✅ Successfully fetched fresh lot_size={lot_size} for {symbol}. Using this for current hedge operation.")
            else:
                # If we still don't have a valid lot size, we cannot proceed.
                error_msg = f"Invalid lot_size={lot_size} for {trade_uid} ({symbol}) and could not fetch a new one."
                logger.error(f"❌ {error_msg}")
                return {"success": False, "error": error_msg}
        # --- END RESILIENCY ---

        delta = float(net_delta or 0.0)
        logger.info(f"📊 [{trade_uid}] Net Δ: {delta:.2f} | lot_size: {lot_size}")

        # Delta too small?
        if abs(delta) < 1.0:
            logger.warning(f"⚠️ Delta too small |Δ|={abs(delta):.2f} < 1.0")
            return {"success": False, "error": f"Delta too small: {delta:.2f}"}

        # Target reduction: monitor-provided or full neutralization
        if target_delta_reduction is None:
            delta_change_magnitude = abs(delta)
            logger.info(f"📊 Full neutralization target magnitude: {delta_change_magnitude:.2f}")
        else:
            # The monitor sends a signed value, but we only care about the magnitude for quantity calculation.
            delta_change_magnitude = abs(float(target_delta_reduction))
            logger.info(f"📊 Monitor target magnitude: {delta_change_magnitude:.2f}")

        # Round the number of contracts to the nearest lot size.
        contracts_to_hedge = round(delta_change_magnitude / lot_size) * lot_size
        if contracts_to_hedge == 0 and delta_change_magnitude > 0:
            contracts_to_hedge = lot_size # Hedge at least one lot if any hedge is needed.

        total_quantity_needed = int(contracts_to_hedge)
        rounded_lots = total_quantity_needed // lot_size if lot_size > 0 else 0

        logger.info(
            f"📊 [{trade_uid}] Target Δ Reduction: {total_quantity_needed:.2f}"
        )
        logger.info(f"📊 Quantity Needed: {total_quantity_needed} per leg")
        logger.info(f"📊 Lots to execute: {rounded_lots}")

        # --- HEDGE AT LATEST ATM STRIKE ---
        option_chain = state.option_chains.get(symbol.upper())
        
        # --- FIX: Fallback to REST API if cache is empty ---
        if not option_chain:
            logger.info(f"🛡️ Hedge: Cache miss for {symbol}. Fetching from service...")
            option_chain = await get_option_chain_from_service(symbol.upper())
            if option_chain:
                state.publish_option_chain(symbol.upper(), option_chain)
        # --- END FIX ---
        
        if not option_chain or not option_chain.get('chain'):
            error_msg = f"Option chain for {symbol} not available for ATM hedge."
            logger.error(f"❌ {error_msg}")
            return {"success": False, "error": error_msg}

        if atm_strike_override:
            atm_strike = atm_strike_override
            logger.info(f"🎯 [{trade_uid}] Hedging with provided ATM strike override: {atm_strike}")
        else:
            atm_strike = option_chain.get('atm')
            logger.info(f"🎯 [{trade_uid}] Hedging with latest cached ATM strike: {atm_strike}")

        atm_row = next((row for row in option_chain['chain'] if row['strike'] == atm_strike), None)

        if not atm_row:
            # Fallback to original strike if ATM not found (should be rare)
            logger.warning(f"⚠️ Could not find ATM strike {atm_strike} in chain. Falling back to original trade strike.")
            original_strike = trade.get("strike")
            atm_row = next((row for row in option_chain['chain'] if row['strike'] == original_strike), None)
            if not atm_row:
                error_msg = f"Could not find original or ATM strike in option chain for {symbol}."
                logger.error(f"❌ {error_msg}")
                return {"success": False, "error": error_msg}
            atm_strike = original_strike

        ce_token = atm_row.get("ce_token")
        pe_token = atm_row.get("pe_token")
        # Get LTPs for accurate limit order conversion and logging
        ce_ltp = atm_row.get('ce_ltp', 0.0)
        pe_ltp = atm_row.get('pe_ltp', 0.0)
        
        original_trade_strike = trade.get("strike")
        if original_trade_strike != atm_strike:
            logger.debug(f"🎯 Original Strike: {original_trade_strike} -> Hedging at current ATM Strike: {atm_strike}")
        else:
            logger.debug(f"🎯 Hedging at current ATM Strike: {atm_strike}")

        if not ce_token or not pe_token:
            error_msg = f"Missing CE/PE tokens for ATM strike {atm_strike} for {trade_uid}"
            logger.error(error_msg)
            return {"success": False, "error": error_msg}

        # Hedge direction
        if delta < 0:
            # Short delta → bullish synthetic (BUY CE + SELL PE)
            ce_side = "BUY"
            pe_side = "SELL"
            hedge_dir = "BULLISH"
        else:
            # Long delta → bearish synthetic (SELL CE + BUY PE)
            ce_side = "SELL"
            pe_side = "BUY"
            hedge_dir = "BEARISH"

        # --- NEW: Use generate_chunked_orders for consistency ---
        logger.info(f"🎯 [{trade_uid}] Direction: {hedge_dir}")
        logger.debug(f"📍 Strike {atm_strike} | CE({ce_token}): {ce_side} | PE({pe_token}): {pe_side}")

        # Determine prefix for order UIDs
        uid_prefix = uid_prefix_override if uid_prefix_override else f"H{trade_uid}"

        symbol_upper = symbol.upper()
        base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper), None)
        max_order_qty = SYMBOL_CONFIG.get(base_symbol, {}).get('max_order_qty', 1800) if base_symbol else 1800

        # Get order chunking config from the trade
        config = trade.get('config', {})
        order_lots_per_call = config.get('order_lots_per_call', 20) # Use new default of 20

        # Build legs data for chunk generator
        legs_data_for_batching = []
        if rounded_lots > 0:
            legs_data_for_batching.append({
                'token': ce_token, 'option_type': 'CE', 'action': ce_side,
                'total_lots': rounded_lots, 'lot_size': lot_size,
                'expected_price': ce_ltp,
                'exchange_segment': segment, 'product_type': 'MIS'
            })
            legs_data_for_batching.append({
                'token': pe_token, 'option_type': 'PE', 'action': pe_side,
                'total_lots': rounded_lots, 'lot_size': lot_size,
                'expected_price': pe_ltp, # Pass LTP for LIMIT order conversion
                'exchange_segment': segment, 'product_type': 'MIS'
            })

        # Generate all orders, chunked logically
        all_chunks = generate_chunked_orders(
            trade_uid_prefix=uid_prefix,
            legs_data=legs_data_for_batching,
            base_lots_for_trade=rounded_lots, # Use rounded_lots to calculate min_lots_per_order
            chunk_divisor=10, # This will be ignored
            max_order_qty=max_order_qty,
            order_lots_per_call=None, # Ignore manual build limits for hedging
            aggressive=True           # Force max lots per order for fast execution
        )

        # --- Inject limit order buffer from config into each order ---
        default_buffer = 8.0 if "SENSEX" in symbol.upper() else 4.0
        buy_buffer = float(config.get('buy_buffer', default_buffer))
        sell_buffer = float(config.get('sell_buffer', default_buffer))
        for chunk in all_chunks:
            for order in chunk:
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer
        # --- END INJECTION ---
        
        all_hedge_orders = [order for chunk in all_chunks for order in chunk]
        logger.info(f"Generated {len(all_hedge_orders)} individual orders for hedge execution, to be split into 2 batches.")

        if not all_hedge_orders:
            logger.warning(f"⚠️ No hedge orders generated for {trade_uid}. Nothing to execute.")
            return {"success": True, "error": "No hedge orders to execute."}

        # --- NEW: Split execution into two 50% batches ---
        total_orders_to_place = len(all_hedge_orders)
        split_point = (total_orders_to_place + 1) // 2
        batch_1_orders = all_hedge_orders[:split_point]
        batch_2_orders = all_hedge_orders[split_point:]

        all_successful_orders = []
        all_failed_orders = []
        total_execution_time = 0.0

        # --- CANCELLATION CHECK ---
        loop = asyncio.get_event_loop() # Need the loop to update status
        if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
            logger.warning(f"🛑 Hedge for {trade_uid} cancelled by user before execution.")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
            return {'success': False, 'error': 'Cancelled by user before execution'}
        # --- END CANCELLATION CHECK ---
        
        # --- FIX: Ensure no stale open orders before starting execution ---
        try:
            logger.info(f"Executing pre-hedge safety cancellation for {trade_uid}...")
            await executor.cancel_all_open_orders_for_trade(trade_uid)
            await asyncio.sleep(0.2) # Brief pause for cancellations to process
        except Exception as cancel_e:
            logger.error(f"Pre-hedge safety cancellation failed for {trade_uid}: {cancel_e}")
        # --- END FIX ---

        # Execute Batch 1
        if batch_1_orders:
            logger.info(f"Executing HEDGE Batch 1/2 for {trade_uid} with {len(batch_1_orders)} orders...")
            batch_1_result = await hedge_position(trade_uid, batch_1_orders, f"{hedge_type}_B1")
            if batch_1_result:
                all_successful_orders.extend(batch_1_result.get('successful_orders', []))
                all_failed_orders.extend(batch_1_result.get('failed_orders', []))
                total_execution_time += batch_1_result.get('execution_time', 0.0)
                logger.info(f"HEDGE Batch 1/2 complete. Success: {batch_1_result.get('successful_count', 0)}, Failed: {batch_1_result.get('failed_count', 0)}")
                # If the first batch was not successful, abort the second batch.
                if not batch_1_result.get('success'):
                    logger.error(f"❌ HEDGE Batch 1/2 was not successful. Aborting Batch 2.")
                    batch_2_orders = [] # Clear the second batch
            else:
                logger.error(f"❌ HEDGE Batch 1/2 failed for {trade_uid}.")
                all_failed_orders.extend(batch_1_orders)

        # --- CANCELLATION CHECK (between batches) ---
        if hasattr(state, 'cancellation_flags') and state.cancellation_flags.get(trade_uid):
            logger.warning(f"🛑 Hedge for {trade_uid} cancelled by user after first batch.")
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'ACTIVE')
            if trade_uid in state.cancellation_flags: del state.cancellation_flags[trade_uid]
            # Return partial success since batch 1 may have executed
            return {'success': False, 'error': 'Cancelled by user after first batch', 'partial_success': True}
        # --- END CANCELLATION CHECK ---

        # --- FIX: Cancel any resting orders from Batch 1 before executing Batch 2 ---
        try:
            logger.info(f"Executing mid-hedge safety cancellation for {trade_uid} before Batch 2...")
            await executor.cancel_all_open_orders_for_trade(trade_uid)
            await asyncio.sleep(0.2) # Brief pause for cancellations to process
        except Exception as cancel_e:
            logger.error(f"Mid-hedge safety cancellation failed for {trade_uid}: {cancel_e}")
        # --- END CANCELLATION CHECK ---

        # Execute Batch 2
        if batch_2_orders:
            logger.info(f"Executing HEDGE Batch 2/2 for {trade_uid} with {len(batch_2_orders)} orders...")
            batch_2_result = await hedge_position(trade_uid, batch_2_orders, f"{hedge_type}_B2")
            if batch_2_result:
                all_successful_orders.extend(batch_2_result.get('successful_orders', []))
                all_failed_orders.extend(batch_2_result.get('failed_orders', []))
                total_execution_time += batch_2_result.get('execution_time', 0.0)
                logger.info(f"HEDGE Batch 2/2 complete. Success: {batch_2_result.get('successful_count', 0)}, Failed: {batch_2_result.get('failed_count', 0)}")
            else:
                logger.error(f"❌ HEDGE Batch 2/2 failed for {trade_uid}.")
                all_failed_orders.extend(batch_2_orders)

        successful_count = len(all_successful_orders)
        failed_count = len(all_failed_orders)

        if successful_count == total_orders_to_place:
            logger.info(f"✅ Hedge batch execution complete for {trade_uid}")
        else:
            logger.warning(f"⚠️  Partial hedge for {trade_uid}: {successful_count}/{total_orders_to_place} succeeded.")

        total_time = time.time() - start_time

        # If no orders were successfully placed, it's a failure.
        if successful_count == 0 and total_orders_to_place > 0:
            logger.error(f"❌ Synthetic hedge failed for {trade_uid}: No orders were successful.")
            return {"success": False, "error": "Hedge execution failed completely", "successful_count": 0, "failed_count": failed_count}

        # Success meta
        result = {
            "success": failed_count == 0, # True only if all orders succeeded
            "trade_uid": trade_uid,
            "hedge_type": hedge_type,
            "net_delta_before": round(delta, 2),
            "target_delta_reduction": round(target_delta_reduction, 2),
            "lots": rounded_lots,
            "lot_size": lot_size,
            "quantity_per_leg": total_quantity_needed, # This is the target, not necessarily what was filled
            "orders": total_orders_to_place, # Total individual orders placed
            "successful_count": successful_count,
            "failed_count": failed_count,
            "execution_time_ms": int(total_execution_time * 1000), # Sum of batch execution times
            "orders_per_second": round(total_orders_to_place / total_time, 1) if total_time > 0 else 0,
        }

        logger.debug("=" * 100)
        logger.info(f"✅ SYNTHETIC HEDGE COMPLETE [{trade_uid}]")
        logger.info(f"   Δ Before: {result['net_delta_before']:+.2f}")
        logger.info(f"   Target Reduction: {result['target_delta_reduction']:.2f}")
        logger.info(f"   Lots: {result['lots']} × {result['lot_size']} = {result['quantity_per_leg']}")
        logger.info(f"   Speed: {result['orders_per_second']:.1f} orders/sec")
        logger.debug("=" * 100)

        return result
    finally:
        try:
            executor = get_order_executor()
            if executor:
                await executor.cancel_all_open_orders_for_trade(trade_uid)
        except Exception:
            pass
        # --- FIX: Do NOT clear the cache here. The calling function (e.g., square_off) needs it. ---
        if 'trade_uid' in locals() and trade_uid:
            # Only clear for regular delta hedges. PRE_SQF_NEUTRAL must leave the cache for square_off.
            if hedge_type not in ["PRE_SQF_NEUTRAL", "BUI_HEDGE"]:
                if hasattr(state, 'trade_fill_cache') and trade_uid in state.trade_fill_cache:
                    del state.trade_fill_cache[trade_uid]
                    logger.info(f"🧹 Cleared temp order cache for {trade_uid} after DELTA hedge operation.")
        # --- END FIX ---
