from trading.straddle_price_guard_runtime import build_chunk_price_allowed

def _verify_build_price_safety(initial_price, current_price, buffer_bps=0.0):
    """
    Returns True if safe to continue building (price has not dropped below target).
    """
    if current_price < initial_price:
        return False
    return True

def _verify_square_off_price_safety(initial_price, current_price):
    """
    Returns True if safe to continue squaring off (price has not risen by >= 1 bps).
    """
    # If price increased by even 1 bps (0.01%), stop squaring off remaining chunks
    if current_price > initial_price * 1.0001:
        return False
    return True

"""
Position Builder - Build ATM Straddles (Delta-Neutral)
Supports: NSE F&O, BSE F&O, and other segments dynamically
Uses same UID for entire trade lifecycle
Verification runs as background task
"""

import asyncio
import functools
import multiprocessing
import time
from typing import Dict, Optional, List
from datetime import datetime, timedelta

from utils.logger import logger
from models.state import state
from trading.order_batching_utils import generate_chunked_orders
from utils.helpers import get_ist_now, get_synthetic_reference_spot
from market_data import get_option_chain, SYMBOL_CONFIG
from trading.order_executor import get_order_executor
from trading.delta_neutral_utils import calculate_delta_neutral_quantities
from background.tasks import broadcast_log, trigger_snapshot_and_broadcast
from trading.data_client import get_option_chain_from_service
import config


def _norm_int(v, default=0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _norm_str(v) -> str:
    return str(v).strip() if v is not None else ""


def _get_fill_order_id(fill: Dict) -> str:
    return _norm_str(
        fill.get('AppOrderID') or fill.get('app_order_id') or
        fill.get('apporderid') or fill.get('order_id')
    )


def _get_fill_token(fill: Dict) -> int:
    return _norm_int(fill.get('ExchangeInstrumentID') or fill.get('exchange_instrument_id'))


def _get_fill_qty(fill: Dict) -> int:
    return _norm_int(fill.get('CumulativeQuantity') or fill.get('filled_qty') or fill.get('quantity'))


def _get_fill_avg_price(fill: Dict) -> float:
    try:
        return float(
            fill.get('OrderAverageTradedPrice') or
            fill.get('fill_price') or
            fill.get('expected_price') or 0.0
        )
    except (TypeError, ValueError):
        return 0.0


def _seed_trade_fills(trade_uid: str, new_fills: List[Dict]) -> None:
    existing = state.get_trade_fills(trade_uid) if hasattr(state, 'get_trade_fills') else []
    existing.extend(new_fills)
    if hasattr(state, 'seed_trade_fills'):
        state.seed_trade_fills(trade_uid, existing)


def _clear_trade_fills(trade_uid: str) -> None:
    if hasattr(state, 'clear_trade_fills'):
        state.clear_trade_fills(trade_uid)


def _publish_chain_if_needed(symbol_upper: str, chain_data: Optional[Dict]) -> Optional[Dict]:
    if not chain_data:
        return None
    if hasattr(state, 'publish_option_chain'):
        return state.publish_option_chain(symbol_upper, chain_data)
    return chain_data


def _classify_order_role(order: Dict, ce_token: int, pe_token: int) -> str:
    token = _norm_int(order.get('token') or order.get('ExchangeInstrumentID'))
    explicit = _norm_str(order.get('build_role') or order.get('order_role')).upper()
    if explicit in {"BUILD_CE", "BUILD_PE", "TEMP_HEDGE"}:
        return explicit
    if token == ce_token:
        return "BUILD_CE"
    if token == pe_token:
        return "BUILD_PE"
    return "TEMP_HEDGE"


def _attach_fill_metadata(
    fill: Dict,
    app_order_id_to_meta_map: Dict[str, Dict],
    ce_token: int,
    pe_token: int,
) -> Dict:
    app_order_id = _get_fill_order_id(fill)
    meta = app_order_id_to_meta_map.get(app_order_id, {}) if app_order_id else {}

    if meta.get('uid'):
        fill['OrderUniqueIdentifier'] = meta['uid']
        fill['order_unique_id'] = meta['uid']

    role = meta.get('build_role')
    if not role:
        role = _classify_order_role(fill, ce_token, pe_token)

    fill['build_role'] = role
    fill['order_role'] = role
    fill['trade_uid'] = meta.get('trade_uid') or fill.get('trade_uid')

    return fill


def _is_build_ce_fill(fill: Dict) -> bool:
    role = _norm_str(fill.get('build_role') or fill.get('order_role')).upper()
    return role == "BUILD_CE"


def _is_build_pe_fill(fill: Dict) -> bool:
    role = _norm_str(fill.get('build_role') or fill.get('order_role')).upper()
    return role == "BUILD_PE"


def _is_build_fill(fill: Dict) -> bool:
    return _is_build_ce_fill(fill) or _is_build_pe_fill(fill)


def _is_temp_hedge_fill(fill: Dict) -> bool:
    role = _norm_str(fill.get('build_role') or fill.get('order_role')).upper()
    return role == "TEMP_HEDGE"


def _compute_build_fill_stats(
    fills: List[Dict],
    ce_token: int,
    pe_token: int,
    ce_fallback_price: float,
    pe_fallback_price: float,
) -> Dict:
    ce_total_value = 0.0
    ce_filled_qty = 0
    pe_total_value = 0.0
    pe_filled_qty = 0
    hedge_fill_count = 0

    build_ce_orders = []
    build_pe_orders = []

    for fill in fills:
        qty = _get_fill_qty(fill)
        avg_price = _get_fill_avg_price(fill)
        token = _get_fill_token(fill)

        if _is_temp_hedge_fill(fill):
            hedge_fill_count += 1

        if not token or qty <= 0:
            continue

        if _is_build_ce_fill(fill) and token == ce_token:
            if avg_price > 0:
                ce_total_value += avg_price * qty
            ce_filled_qty += qty
            build_ce_orders.append(fill)

        elif _is_build_pe_fill(fill) and token == pe_token:
            if avg_price > 0:
                pe_total_value += avg_price * qty
            pe_filled_qty += qty
            build_pe_orders.append(fill)

    avg_ce_fill = (ce_total_value / ce_filled_qty) if ce_filled_qty > 0 else ce_fallback_price
    avg_pe_fill = (pe_total_value / pe_filled_qty) if pe_filled_qty > 0 else pe_fallback_price

    return {
        "ce_total_value": ce_total_value,
        "ce_filled_qty": ce_filled_qty,
        "pe_total_value": pe_total_value,
        "pe_filled_qty": pe_filled_qty,
        "avg_ce_fill": avg_ce_fill,
        "avg_pe_fill": avg_pe_fill,
        "build_ce_orders": build_ce_orders,
        "build_pe_orders": build_pe_orders,
        "hedge_fill_count": hedge_fill_count,
    }

def _get_fill_side(fill: Dict) -> str:
    return _norm_str(
        fill.get('OrderSide') or
        fill.get('order_side') or
        fill.get('action') or
        fill.get('TransactionType')
    ).upper()


def _compute_net_short_inventory(
    fills: List[Dict],
    ce_token: int,
    pe_token: int,
) -> Dict:
    ce_net_short_qty = 0
    pe_net_short_qty = 0

    for fill in fills:
        qty = _get_fill_qty(fill)
        token = _get_fill_token(fill)
        side = _get_fill_side(fill)

        if not token or qty <= 0:
            continue

        signed_qty = 0
        if side == 'SELL':
            signed_qty = qty
        elif side == 'BUY':
            signed_qty = -qty
        else:
            continue

        if token == ce_token:
            ce_net_short_qty += signed_qty
        elif token == pe_token:
            pe_net_short_qty += signed_qty

    return {
        "ce_net_short_qty": max(0, ce_net_short_qty),
        "pe_net_short_qty": max(0, pe_net_short_qty),
    }


def _get_chunk_target_quantities(
    chunk_orders: List[Dict],
    ce_token: int,
    pe_token: int,
) -> Dict:
    ce_target_qty = 0
    pe_target_qty = 0

    for order in chunk_orders:
        role = _classify_order_role(order, ce_token, pe_token)
        action = _norm_str(order.get('action') or order.get('OrderSide')).upper()
        qty = _norm_int(order.get('quantity') or order.get('OrderQuantity'))

        if action != 'SELL' or qty <= 0:
            continue

        if role == "BUILD_CE":
            ce_target_qty += qty
        elif role == "BUILD_PE":
            pe_target_qty += qty

    return {
        "ce_target_qty": ce_target_qty,
        "pe_target_qty": pe_target_qty,
    }


def _make_remainder_order(
    *,
    token: int,
    option_type: str,
    quantity: int,
    expected_price: float,
    exchange_segment: int,
    product_type: str,
    build_role: str,
    trade_uid: str,
    limit_order_buffer: float,
    uid_seed: str,
) -> Optional[Dict]:
    if quantity <= 0:
        return None

    return {
        'token': token,
        'option_type': option_type,
        'action': 'SELL',
        'quantity': int(quantity),
        'expected_price': expected_price,
        'exchange_segment': exchange_segment,
        'product_type': product_type,
        'build_role': build_role,
        'order_role': build_role,
        'trade_uid': trade_uid,
        'limit_order_buffer': limit_order_buffer,
        'limit_price': 0.0,
        'uid': uid_seed[:20],
    }


def _dedupe_retry_orders(orders: List[Dict]) -> List[Dict]:
    deduped = []
    seen = set()

    for order in orders:
        key = (
            _norm_int(order.get('token')),
            _norm_str(order.get('action')).upper(),
            _norm_int(order.get('quantity')),
            _norm_str(order.get('build_role') or order.get('order_role')).upper(),
            _norm_str(order.get('uid')),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(order)

    return deduped

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


async def get_correct_lot_size(straddle_data: Dict) -> int:
    symbol = straddle_data.get("symbol", "NIFTY").upper()

    option_chain = None
    if hasattr(state, 'get_published_option_chain'):
        option_chain = state.get_published_option_chain(symbol)

    if option_chain and option_chain.get('lot_size', 0) > 0:
        return option_chain['lot_size']

    try:
        fresh_chain = await get_option_chain_from_service(symbol)
        if fresh_chain and fresh_chain.get('lot_size', 0) > 0:
            published = _publish_chain_if_needed(symbol, fresh_chain)
            if published and published.get('lot_size', 0) > 0:
                return published['lot_size']
            return fresh_chain['lot_size']
    except Exception:
        pass

    db_lot_size = straddle_data.get('lot_size', 0)
    if db_lot_size > 0:
        return db_lot_size

    base_symbol = next(
        (key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol),
        None
    )
    if base_symbol:
        return SYMBOL_CONFIG.get(base_symbol, {}).get('lot_size', 65)

    return 65


async def execute_prepared_build(trade_uid: str):
    loop = asyncio.get_running_loop()
    trade_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)

    if not trade_data:
        logger.error(f"[{trade_uid}] Execute live build failed: Trade not found in DB.")
        return

    logger.info(f"[{trade_uid}] Live execution triggered! Calculating delta-neutral and executing now.")
    trade_config = trade_data.get("config", {})
    if "entry_at_straddle" in trade_config:
        del trade_config["entry_at_straddle"]

    symbol = trade_data.get("symbol", "NIFTY")
    lots = trade_data.get("lots", 1)
    delta_neutral = trade_data.get("delta_neutral", True)
    product_type = trade_data.get("product_type", "MIS")

    try:
        await build_straddle(
            symbol=symbol,
            lots=lots,
            trade_uid=trade_uid,
            delta_neutral=delta_neutral,
            product_type=product_type,
            trade_config=trade_config
        )
    except Exception as e:
        logger.error(f"[{trade_uid}] Error during live build execution: {e}", exc_info=True)
        try:
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'FAILED_BUILD')
        except Exception as db_e:
            logger.error(f"[{trade_uid}] CRITICAL: Failed to update status: {db_e}")

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
    logger.debug("=" * 100)
    logger.info(f"BUILDING {count} STRADDLES")
    logger.debug("=" * 100)

    for i in range(count):
        logger.info(f"Building straddle {i + 1}/{count}")
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
            logger.info(f"Straddle {i + 1}/{count} built: {straddle['trade_uid']}")
        else:
            logger.error(f"Straddle {i + 1}/{count} failed")

        if i < count - 1 and delay_seconds > 0:
            logger.info(f"Waiting {delay_seconds}s before next straddle...")
            await asyncio.sleep(delay_seconds)

    logger.debug("=" * 100)
    logger.info(f"Built {len(straddles)}/{count} straddles successfully")
    logger.debug("=" * 100)
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
            logger.error(
                f"Invalid quantities: CE={straddle_data['ce_quantity']}, "
                f"PE={straddle_data['pe_quantity']}"
            )
            return False

        if straddle_data.get('ce_entry_price', 0) <= 0 or straddle_data.get('pe_entry_price', 0) <= 0:
            logger.error(
                f"Invalid entry prices: CE={straddle_data.get('ce_entry_price')}, "
                f"PE={straddle_data.get('pe_entry_price')}"
            )
            return False

        logger.debug(f"Straddle data validation passed for {straddle_data['trade_uid']}")
        return True
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False


async def manual_sync_trade_orders(trade_uid: str) -> Dict:
    logger.info(f"MANUAL SYNC initiated for trade {trade_uid}")
    loop = asyncio.get_running_loop()
    executor = get_order_executor()
    if not executor:
        return {"success": False, "error": "OrderExecutor not initialized"}

    db_orders_for_trade = await loop.run_in_executor(None, state.db.get_orders_by_trade_id, trade_uid)
    db_order_map = {
        str(o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')): o
        for o in db_orders_for_trade
    }
    logger.info(f"Found {len(db_order_map)} orders for {trade_uid} in the database.")

    try:
        if executor.client_id:
            order_book_func = functools.partial(
                executor.xt_i.get_order_book,
                clientID=executor.client_id
            )
        else:
            order_book_func = executor.xt_i.get_order_book

        broker_order_book = await loop.run_in_executor(None, order_book_func)
        if not broker_order_book or broker_order_book.get('type') != 'success':
            error_msg = broker_order_book.get('description', 'Unknown error')
            logger.error(f"Manual Sync: Order book fetch failed: {error_msg}")
            return {"success": False, "error": f"Broker order book fetch failed: {error_msg}"}

        broker_orders = broker_order_book.get('result', [])
        broker_order_map = {str(o.get('AppOrderID')): o for o in broker_orders if o.get('AppOrderID')}
        logger.info(f"Fetched {len(broker_orders)} total orders from broker.")
    except Exception as e:
        logger.error(f"Manual Sync: Exception during order book fetch: {e}", exc_info=True)
        return {"success": False, "error": f"Exception during order book fetch: {e}"}

    newly_found_orders = []
    orders_to_update = []

    for broker_order in broker_orders:
        app_order_id = str(broker_order.get('AppOrderID'))
        if not app_order_id:
            continue

        is_trade_order = False
        uid_from_broker = broker_order.get('OrderUniqueIdentifier', '')
        if trade_uid in uid_from_broker:
            is_trade_order = True
        elif app_order_id in db_order_map:
            is_trade_order = True

        if not is_trade_order:
            continue

        broker_status = str(broker_order.get('OrderStatus', 'UNKNOWN')).upper()
        if app_order_id not in db_order_map:
            if broker_status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED', 'CANCELLED', 'CANCELED', 'REJECTED']:
                logger.info(
                    f"SYNC: Found new order for {trade_uid} at broker: "
                    f"ID {app_order_id}, Status: {broker_status}"
                )
                newly_found_orders.append(broker_order)
        else:
            db_order = db_order_map[app_order_id]
            db_status = str(db_order.get('order_status', 'UNKNOWN')).upper()
            if broker_status != db_status and broker_status in [
                'FILLED', 'COMPLETE', 'TRADED', 'EXECUTED', 'CANCELLED', 'CANCELED', 'REJECTED'
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
                updated_ghost_order['OrderStatus'] = 'REJECTED'
                updated_ghost_order['CancelRejectReason'] = 'Not found in broker order book during manual sync'
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
        ce_ltp = state.get_price(ce_token) or 0.0
        pe_ltp = state.get_price(pe_token) or 0.0

        if ce_ltp <= 0 or pe_ltp <= 0:

            logger.warning(f"Invalid current prices for {trade_uid}: CE={ce_ltp}, PE={pe_ltp}")
            return None

        ce_entry = straddle['ce_entry_price']
        pe_entry = straddle['pe_entry_price']
        ce_qty = straddle['ce_quantity']
        pe_qty = straddle['pe_quantity']

        ce_pnl = (ce_entry - ce_ltp) * ce_qty
        pe_pnl = (pe_entry - pe_ltp) * pe_qty
        total_pnl = ce_pnl + pe_pnl

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

        trade_cfg = trade.get('config', {})
        entry_time_str = trade_cfg.get('entry_time')

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

        symbol = trade.get('symbol')
        lots = trade.get('lots')
        delta_neutral = trade.get('delta_neutral', True)
        product_type = trade.get('product_type', 'MIS')

        await build_straddle(
            symbol=symbol,
            lots=lots,
            trade_uid=trade_uid,
            delta_neutral=delta_neutral,
            product_type=product_type,
            trade_config=trade_cfg
        )

    except Exception as e:
        logger.error(f"Error resuming trade {trade_uid}: {e}", exc_info=True)


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
    """
    start_time = get_ist_now()

    try:
        if trade_config is None:
            trade_config = {}

        trade_config.setdefault("entry_at_straddle", None)
        trade_config.setdefault("exit_at_straddle", None)


        symbol_upper = symbol.upper()
        loop = asyncio.get_running_loop()

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
                "NIFTY": "ny",
                "SENSEX": "sx",
                "BANKNIFTY": "bn",
                "FINNIFTY": "fn",
                "MIDCPNIFTY": "mc",
            }
            sorted_keys = sorted(SYMBOL_PREFIXES.keys(), key=len, reverse=True)
            prefix = next(
                (SYMBOL_PREFIXES[key] for key in sorted_keys if key in symbol_upper),
                symbol[:2].lower()
            )
            base_trade_uid = f"{prefix}{timestamp}"

            suffix_counter = 0
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

        logger.debug("=" * 100)
        logger.info(f"BUILD STRADDLE | Trade UID: {trade_uid}")
        if delta_neutral:
            logger.info("DELTA-NEUTRAL MODE: Calculating unequal PE/CE quantities")
        logger.debug("=" * 100)

        if target_expiry:
            logger.info(f"Building a specific chain for target expiry: {target_expiry}")
            chain_data = await loop.run_in_executor(None, get_option_chain, symbol, strike_range, target_expiry)
            chain_data = _publish_chain_if_needed(symbol_upper, chain_data)
        else:
            chain_data = None
            if hasattr(state, 'get_published_option_chain'):
                chain_data = state.get_published_option_chain(symbol_upper)

            if not chain_data:
                logger.info(f"Published chain cache miss for {symbol_upper}. Fetching from service...")
                fetched_chain = await get_option_chain_from_service(symbol_upper)
                if fetched_chain:
                    chain_data = _publish_chain_if_needed(symbol_upper, fetched_chain)

        if not chain_data:
            logger.error(
                f"Published option chain for {symbol_upper} not found. "
                "The builder has no canonical snapshot to work from. Aborting build."
            )
            return None

        chain_publish_seq = chain_data.get('publish_seq')
        chain_published_at = chain_data.get('published_at')

        logger.info(
            f"[{trade_uid}] Using published chain for {symbol_upper} | "
            f"publish_seq={chain_publish_seq}, published_at={chain_published_at}"
        )

        exchange_segment = chain_data.get('exchange_segment', config.EXCHANGE_NSEFO)
        exchange_name = {2: "NSE", 12: "BSE", 1: "NSECM", 11: "BSECM"}.get(
            exchange_segment, f"SEG{exchange_segment}"
        )
        logger.info(f"Exchange: {exchange_name} (Segment: {exchange_segment})")

        is_custom_strike = ce_strike_price is not None and pe_strike_price is not None

        if is_custom_strike:
            logger.info(
                f"Building custom position with CE Strike: {ce_strike_price} "
                f"and PE Strike: {pe_strike_price}"
            )
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
            atm_row = next((row for row in chain_data['chain'] if row.get('is_atm')), None)
            if not atm_row:
                atm_row = next((row for row in chain_data['chain'] if row.get('strike') == atm), None)
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

        if (ce_token and not isinstance(ce_token, int)) or (pe_token and not isinstance(pe_token, int)):
            logger.critical(
                f"CRITICAL DATA CORRUPTION DETECTED in option chain for {symbol}. "
                f"CE Token: '{ce_token}' (type: {type(ce_token)}), "
                f"PE Token: '{pe_token}' (type: {type(pe_token)}). "
                "This should be an integer. Aborting build."
            )
            return None

        ce_ltp = ce_row.get('ce_ltp', 0.0)
        pe_ltp = pe_row.get('pe_ltp', 0.0)

        current_straddle = ce_ltp + pe_ltp

        entry_target = trade_config.get("entry_at_straddle")
        if entry_target in ("", None, 0, "0"):
            entry_target = None
        elif entry_target is not None:
            try:
                entry_target = float(entry_target)
            except Exception:
                entry_target = None

        if entry_target is not None:
            logger.debug("=" * 100)
            logger.info(f"[ENTRY CHECK] Current={current_straddle:.2f} | Target={entry_target:.2f}")
            logger.info(f"Current < Target  : {current_straddle < entry_target}")
            logger.debug("=" * 100)

            if current_straddle < entry_target:
                logger.info(f"[{trade_uid}] [WAITING FOR ENTRY] Current {current_straddle:.2f} < target {entry_target:.2f}. Preparing trade.")
                
                # FIX 1: Added all required fields to prevent KeyError: 'strike'
                pending_data = {'trade_uid': trade_uid, 'straddle_id': trade_uid, 'symbol': symbol, 'strike': atm, 'expiry': chain_data.get('expiry', ''), 'expiry_date': chain_data.get('expiry_date'), 'chain_publish_seq': chain_data.get('publish_seq', 0), 'chain_published_at': chain_data.get('published_at', ''), 'exchange_segment': exchange_segment, 'exchange_name': exchange_name, 'product_type': product_type, 'lot_size': lot_size, 'lots': lots, 'initial_pe_quantity': 0, 'initial_ce_quantity': 0, 'pe_lots': 0, 'ce_lots': 0, 'pe_quantity': 0, 'ce_quantity': 0, 'quantity': 0, 'total_quantity': 0, 'ce_token': ce_token, 'ce_symbol': ce_symbol, 'ce_entry_price': 0.0, 'ce_delta': 0.5, 'ce_gamma': 0.0, 'ce_theta': 0.0, 'ce_vega': 0.0, 'ce_iv': 0.0, 'pe_token': pe_token, 'pe_symbol': pe_symbol, 'pe_entry_price': 0.0, 'pe_delta': -0.5, 'pe_gamma': 0.0, 'pe_theta': 0.0, 'pe_vega': 0.0, 'pe_iv': 0.0, 'net_delta': 0.0, 'delta_neutral': delta_neutral, 'total_premium': 0.0, 'status': 'PENDING_ENTRY', 'execution_time': 0.0, 'entry_spot': 0.0, 'spot_price': 0.0, 'fut_token': chain_data.get('fut_token'), 'entry_timestamp': get_ist_now().isoformat(), 'closed_at': None, 'config': trade_config or {}}
                
                await loop.run_in_executor(None, state.db.insert_straddle, pending_data)

                from trading.trade_process import trade_process_worker_entry
                command_q = multiprocessing.Queue()
                process = multiprocessing.Process(
                    target=trade_process_worker_entry,
                    args=(trade_uid, pending_data, command_q, getattr(state, 'trade_data_cache', None) or {}, []),
                    daemon=True, name=f"trade-{trade_uid}"
                )
                process.start()
                
                # FIX 2: Do NOT put the process object in the shared dictionary
                state.trade_processes[trade_uid] = {'pid': process.pid, 'status': 'PENDING_ENTRY'}
                state.local_process_refs[trade_uid] = process
                state.local_command_queues[trade_uid] = command_q
                
                logger.info(f"Process for {trade_uid} started in PENDING_ENTRY mode (PID: {process.pid}).")

                return {
                    "success": False,
                    "pending_entry": True,
                    "trade_uid": trade_uid,
                    "current_straddle": current_straddle,
                    "target_straddle": entry_target,
                }
            else:
                logger.info("[ENTRY STRADDLE] Target satisfied. Proceeding to calculate live spot and delta.")

        logger.info("="*100)
        logger.info("[ENTRY STRADDLE INPUT]")
        entry_target = trade_config.get("entry_at_straddle")
        logger.info(f"Config Value : {entry_target}")
        logger.info(f"Current      : {current_straddle}")
        logger.info("="*100)

        if ce_ltp <= 0 or pe_ltp <= 0:
            logger.error(f"Invalid LTP: CE={ce_ltp}, PE={pe_ltp}")
            return None

        ce_delta = ce_row.get('ce_delta', 0.5)
        pe_delta = pe_row.get('pe_delta', -0.5)

        base_symbol = next(
            (key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol_upper),
            None
        )
        sym_config = SYMBOL_CONFIG.get(base_symbol, {})
        max_order_qty = sym_config.get('max_order_qty', config.MAX_ORDER_QTY)
        logger.info(f"Using MaxOrderQty: {max_order_qty} for {symbol}")

        if delta_neutral and ce_delta != 0 and pe_delta != 0:
            logger.debug("=" * 100)
            logger.info("DELTA-NEUTRAL CALCULATION")
            logger.debug("=" * 100)
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
            synthetic_spot = float(chain_data.get("synthetic_spot") or 0.0)
            fut_ltp = float(chain_data.get("fut_ltp") or 0.0)
            reference_spot = synthetic_spot if synthetic_spot > 0 else fut_ltp

            logger.debug("=" * 100)
            logger.info("DELTA-NEUTRAL ALLOCATION:")
            logger.info(f"   Synthetic Spot: {synthetic_spot:.2f}" if synthetic_spot > 0 else "   Synthetic Spot: N/A")
            logger.info(f"   Fut LTP: {fut_ltp:.2f}" if fut_ltp > 0 else "   Fut LTP: N/A")
            logger.info(f"   Reference Spot Used: {reference_spot:.2f}" if reference_spot > 0 else "   Reference Spot Used: N/A")
            logger.info(f"   PE: {pe_contracts} contracts ({pe_lots} lots)")
            logger.info(f"   CE: {ce_contracts} contracts ({ce_lots} lots)")
            logger.info(f"   Net Delta: {net_delta:.4f}")
            logger.debug("=" * 100)
        else:
            pe_lots = lots
            ce_lots = lots
            pe_contracts = lots * lot_size
            ce_contracts = lots * lot_size
            net_delta = 0.0
            logger.debug("=" * 100)
            logger.info("EQUAL ALLOCATION:")
            logger.info(f"   PE: {pe_contracts} contracts ({pe_lots} lots)")
            logger.info(f"   CE: {ce_contracts} contracts ({ce_lots} lots)")
            logger.debug("=" * 100)

        target_ce_qty = int(ce_contracts)
        target_pe_qty = int(pe_contracts)

        total_quantity = target_pe_qty + target_ce_qty
        logger.info(f"[{exchange_name}] {symbol} ATM {atm} | Total Qty: {total_quantity}")
        logger.debug(f"CE: {ce_token} @ {ce_ltp:.2f} x {target_ce_qty}")
        logger.debug(f"PE: {pe_token} @ {pe_ltp:.2f} x {target_pe_qty}")

        legs_data_for_batching = []
        if ce_lots > 0:
            legs_data_for_batching.append({
                'token': ce_token,
                'option_type': 'CE',
                'action': 'SELL',
                'total_lots': ce_lots,
                'lot_size': lot_size,
                'expected_price': ce_ltp,
                'exchange_segment': exchange_segment,
                'product_type': product_type,
                'build_role': 'BUILD_CE',
                'trade_uid': trade_uid,
            })
        if pe_lots > 0:
            legs_data_for_batching.append({
                'token': pe_token,
                'option_type': 'PE',
                'action': 'SELL',
                'total_lots': pe_lots,
                'lot_size': lot_size,
                'expected_price': pe_ltp,
                'exchange_segment': exchange_segment,
                'product_type': product_type,
                'build_role': 'BUILD_PE',
                'trade_uid': trade_uid,
            })

        order_lots_per_call = trade_config.get('order_lots_per_call') if trade_config else None
        logger.info(
            "BUILD chunking: "
            f"{'MANUAL order_lots_per_call=' + str(order_lots_per_call) if order_lots_per_call else 'RANGE-AUTO ceil(' + str(lots) + '/100)'} "
            f"| lots={lots}"
        )

        all_chunks = generate_chunked_orders(
            trade_uid_prefix=f"B{trade_uid}",
            legs_data=legs_data_for_batching,
            base_lots_for_trade=lots,
            max_order_qty=max_order_qty,
            order_lots_per_call=order_lots_per_call,
        )

        default_buffer = 6.0 if "SENSEX" in symbol_upper else 2.0
        buy_buffer = float(trade_config.get('buy_buffer', default_buffer)) if trade_config else default_buffer
        sell_buffer = float(trade_config.get('sell_buffer', default_buffer)) if trade_config else default_buffer

        for chunk in all_chunks:
            for order in chunk:
                order['build_role'] = _classify_order_role(order, ce_token, pe_token)
                order['order_role'] = order['build_role']
                order['trade_uid'] = trade_uid
                if order.get('action', '').upper() == 'BUY':
                    order['limit_order_buffer'] = buy_buffer
                else:
                    order['limit_order_buffer'] = sell_buffer
                    
        chunk_targets = [
            _get_chunk_target_quantities(chunk, ce_token, pe_token)
            for chunk in all_chunks
        ]

        for idx, tgt in enumerate(chunk_targets, start=1):
            logger.info(
                f"[{trade_uid}] Chunk target {idx}/{len(chunk_targets)} | "
                f"CE={tgt['ce_target_qty']}, PE={tgt['pe_target_qty']}"
            )

        

        straddle_price_filter = float(trade_config.get('straddle_filter', 0.0)) if trade_config else 0.0

        stop_pct_raw = trade_config.get('straddle_stop_loss_pct', 0.0) if trade_config else 0.0
        try:
            straddle_stop_pct = float(stop_pct_raw) if stop_pct_raw != "" else 0.0
        except (ValueError, TypeError):
            straddle_stop_pct = 0.0

        stop_price_threshold = 0.0
        if straddle_price_filter > 0:
            stop_price_threshold = straddle_price_filter * (1 - (straddle_stop_pct / 100.0))
            logger.info(f"Straddle price stop threshold enabled: < ₹{stop_price_threshold:.2f}")

        logger.info(f"Generated {len(all_chunks)} chunks for execution.")

        entry_target = trade_config.get("entry_at_straddle")

        if entry_target in ("", None, 0, "0"):
            entry_target = None

        if entry_target not in (None, "", 0):

            try:
                entry_target = float(entry_target)
            except Exception:
                entry_target = None

        
        if entry_target is None:

            logger.info("[ENTRY STRADDLE] No target configured. Entering immediately.")

        else:

            logger.debug("=" * 100)
            logger.info(
                f"[ENTRY CHECK] "
                f"Current={current_straddle:.2f} | "
                f"Target={entry_target:.2f}"
            )
            logger.info(
                f"Current >= Target : {current_straddle >= entry_target}"
            )
            logger.info(
                f"Current < Target  : {current_straddle < entry_target}"
            )
            logger.debug("=" * 100)

            #
            # Wait until premium REACHES the desired entry.
            #
            

            logger.info("[ENTRY STRADDLE] Target satisfied. Executing build.")


        all_successful_orders = []
        all_failed_orders = []
        all_verified_fills = []
        build_aborted = False
        is_first_fill_processed = False
        total_execution_time = 0.0

        chunk_idx = 0
        cumulative_net_before_chunk = {"ce_net_short_qty": 0, "pe_net_short_qty": 0}

        while chunk_idx < len(all_chunks):
            base_chunk_orders = all_chunks[chunk_idx]
            chunk_target = chunk_targets[chunk_idx]
            chunk_target_ce = chunk_target["ce_target_qty"]
            chunk_target_pe = chunk_target["pe_target_qty"]

            if not base_chunk_orders:
                chunk_idx += 1
                continue

            chunk_verified_fills = []
            orders_to_process = list(base_chunk_orders)
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

            if stop_price_threshold > 0:
                current_chain_data = None
                if hasattr(state, 'get_published_option_chain'):
                    current_chain_data = state.get_published_option_chain(symbol_upper)

                if not current_chain_data:
                    fresh_chain_data = await get_option_chain_from_service(symbol_upper)
                    if fresh_chain_data:
                        current_chain_data = _publish_chain_if_needed(symbol_upper, fresh_chain_data)

                if not current_chain_data:
                    current_chain_data = chain_data

                current_atm_strike = current_chain_data.get('atm')
                current_atm_row = next(
                    (row for row in current_chain_data.get('chain', []) if row.get('strike') == current_atm_strike),
                    None
                )

                if current_atm_row:
                    ce_token_atm = current_atm_row.get('ce_token')
                    pe_token_atm = current_atm_row.get('pe_token')

                    current_ce_ltp = state.get_price(int(ce_token_atm)) if ce_token_atm else 0.0
                    current_pe_ltp = state.get_price(int(pe_token_atm)) if pe_token_atm else 0.0

                    if not current_ce_ltp:
                        current_ce_ltp = current_atm_row.get('ce_ltp', 0)
                    if not current_pe_ltp:
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
                    logger.info(
                        f"Retrying {len(orders_to_process)} orders in CHUNK {chunk_idx + 1} "
                        f"(Attempt {retry_iter+1}) with {buffer_multiplier}x buffer..."
                    )
                    for order in orders_to_process:
                        action = order.get('action', '').upper()
                        base_buffer = buy_buffer if action == 'BUY' else sell_buffer
                        order['limit_order_buffer'] = base_buffer * buffer_multiplier
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

                        order['build_role'] = _classify_order_role(order, ce_token, pe_token)
                        order['order_role'] = order['build_role']
                        order['trade_uid'] = trade_uid

                    await asyncio.sleep(0)

                logger.info(
                    f"Executing BUILD chunk {chunk_idx + 1}/{len(all_chunks)} "
                    f"(Iter {retry_iter+1}) with {len(orders_to_process)} orders."
                )

                # ============================================================
                # MANUAL_ENTRY_PRICE_GUARD_V2
                #
                # Check immediately BEFORE EVERY BUILD CHUNK SUBMISSION.
                #
                # target <= 0 / None / "" / "0":
                #     guard bypassed; normal behavior.
                #
                # target > 0:
                #     current straddle >= entry target -> submit
                #     current straddle <  entry target -> WAIT
                #
                # This is deliberately inside the chunk/retry loop so a
                # price drop AFTER chunk N cannot allow chunk N+1 to sell.
                # ============================================================
                while True:
                    build_price_allowed = await build_chunk_price_allowed(
                        trade_uid=trade_uid,
                        symbol=symbol,
                        target_entry_price=entry_target,
                    )

                    if build_price_allowed:
                        logger.info(
                            f"[{trade_uid}] ENTRY PRICE GUARD PASS | "
                            f"BUILD chunk {chunk_idx + 1}/{len(all_chunks)} "
                            f"may be submitted."
                        )
                        break

                    logger.warning(
                        f"[{trade_uid}] ENTRY PRICE GUARD BLOCK | "
                        f"BUILD chunk {chunk_idx + 1}/{len(all_chunks)} "
                        f"NOT submitted. Waiting for entry_at_straddle "
                        f"condition."
                    )

                    await asyncio.sleep(1.0)

                chunk_result = await executor.execute_batch(orders_to_process, current_chunk_uid)
                total_execution_time += chunk_result.get('execution_time', 0.0)

                successful_in_chunk = chunk_result.get('successful_orders', [])
                failed_placements = chunk_result.get('failed_orders', [])

                for o in successful_in_chunk:
                    o['build_role'] = _classify_order_role(o, ce_token, pe_token)
                    o['order_role'] = o['build_role']
                    o['trade_uid'] = trade_uid

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
                            'trade_uid': trade_uid,
                            'build_role': o.get('build_role'),
                            'order_role': o.get('order_role'),
                        }
                        for o in successful_in_chunk
                    ]
                    try:
                        await loop.run_in_executor(None, state.db.insert_orders_bulk, db_orders_batch)
                    except Exception as ins_e:
                        logger.error(
                            f"Failed to bulk-persist {len(db_orders_batch)} placed orders "
                            f"for chunk {chunk_idx+1}: {ins_e}"
                        )

                chunk_order_ids = [
                    str(o.get('order_id') or o.get('app_order_id'))
                    for o in successful_in_chunk
                    if o.get('order_id') or o.get('app_order_id')
                ]
                app_order_id_to_uid_map = {
                    str(o.get('app_order_id')): o.get('uid')
                    for o in successful_in_chunk
                }
                app_order_id_to_meta_map = {
                    str(o.get('app_order_id')): {
                        'uid': o.get('uid'),
                        'build_role': o.get('build_role'),
                        'trade_uid': trade_uid,
                    }
                    for o in successful_in_chunk
                    if o.get('app_order_id')
                }

                verified_fills_for_chunk = []
                unverified_order_ids = list(chunk_order_ids)
                max_verification_attempts = 3
                newly_failed = []
                orders_to_retry_now = []

                for attempt in range(max_verification_attempts):
                    if not unverified_order_ids:
                        break

                    logger.info(
                        f"Verifying BUILD chunk {chunk_idx + 1}, attempt {attempt + 1}/"
                        f"{max_verification_attempts} for {len(unverified_order_ids)} orders..."
                    )
                    verification_result = await executor.verify_orders_bulk(
                        unverified_order_ids,
                        f"BUI_{trade_uid}_CHUNK{chunk_idx+1}_ITER{retry_iter+1}_VER{attempt+1}",
                        trade_uid=trade_uid
                    )

                    if verification_result:
                        newly_verified = verification_result.get('verified_success', [])
                        newly_failed = verification_result.get('verified_failed', [])

                        for fill in newly_verified:
                            _attach_fill_metadata(fill, app_order_id_to_meta_map, ce_token, pe_token)
                        for fill in newly_failed:
                            _attach_fill_metadata(fill, app_order_id_to_meta_map, ce_token, pe_token)

                        verified_fills_for_chunk.extend(newly_verified)

                        verified_ids = {_get_fill_order_id(o) for o in newly_verified}

                        if attempt < max_verification_attempts - 1:
                            terminal_statuses = {
                                'REJECTED', 'CANCELLED', 'CANCELED',
                                'REEXECUTE_NEEDED', 'NOT_FOUND_ON_RETRY'
                            }
                            failed_ids = {
                                _get_fill_order_id(o)
                                for o in newly_failed
                                if str(o.get('status')).upper() in terminal_statuses
                            }
                        else:
                            failed_ids = {_get_fill_order_id(o) for o in newly_failed}

                        resolved_ids = verified_ids.union(failed_ids)
                        unverified_order_ids = [oid for oid in unverified_order_ids if oid not in resolved_ids]

                    if unverified_order_ids:
                        logger.warning(
                            f"{len(unverified_order_ids)} orders still pending in BUILD chunk "
                            f"{chunk_idx + 1}. Retrying in 0.5s..."
                        )
                        await asyncio.sleep(0.5)

                ids_to_remove_from_successful = set()
                if newly_failed:
                    for failed_order_info in newly_failed:
                        if failed_order_info.get('status') == 'REEXECUTE_NEEDED':
                            order_id = str(failed_order_info.get('order_id'))
                            ids_to_remove_from_successful.add(order_id)
                            original_order_uid = app_order_id_to_uid_map.get(order_id)
                            if original_order_uid:
                                original_order_data = next(
                                    (o for o in orders_to_process if o['uid'] == original_order_uid),
                                    None
                                )
                                if original_order_data:
                                    orders_to_retry_now.append(original_order_data)
                                    logger.info(
                                        f"Order {original_order_uid} marked for re-execution "
                                        f"in current chunk (Iter {retry_iter+1})."
                                    )

                if ids_to_remove_from_successful:
                    all_successful_orders = [
                        o for o in all_successful_orders
                        if str(o.get('app_order_id') or o.get('order_id')) not in ids_to_remove_from_successful
                    ]
                    logger.info(
                        f"Corrected success tracking: Removed {len(ids_to_remove_from_successful)} "
                        f"orders that were cancelled for re-execution."
                    )

                all_verified_fills.extend(verified_fills_for_chunk)

                fills_to_process_for_chunk = verified_fills_for_chunk
                if fills_to_process_for_chunk:
                    _seed_trade_fills(trade_uid, fills_to_process_for_chunk)
                    logger.info(
                        f"Cached {len(fills_to_process_for_chunk)} build fills "
                        f"for trade '{trade_uid}' from chunk {chunk_idx + 1}."
                    )

                    fills_with_uid = []
                    for fill_data in fills_to_process_for_chunk:
                        if fill_data.get('OrderUniqueIdentifier'):
                            if 'order_unique_id' not in fill_data:
                                fill_data['order_unique_id'] = fill_data.get('OrderUniqueIdentifier')
                            fills_with_uid.append(fill_data)

                    if fills_with_uid:
                        await loop.run_in_executor(None, state.db.insert_orders_bulk, fills_with_uid)
                        logger.info(
                            f"Bulk inserted {len(fills_with_uid)} verified fills "
                            f"from chunk {chunk_idx + 1}."
                        )

                if all_verified_fills:
                    stats_live = _compute_build_fill_stats(
                        fills=all_verified_fills,
                        ce_token=ce_token,
                        pe_token=pe_token,
                        ce_fallback_price=ce_ltp,
                        pe_fallback_price=pe_ltp,
                    )

                    ce_filled_qty = stats_live["ce_filled_qty"]
                    pe_filled_qty = stats_live["pe_filled_qty"]
                    avg_ce_fill = stats_live["avg_ce_fill"]
                    avg_pe_fill = stats_live["avg_pe_fill"]
                    reference_spot = get_synthetic_reference_spot(chain_data)
                    if reference_spot <= 0:
                        logger.error(
                            f"No synthetic_spot available for {trade_uid or symbol}. Aborting build insert."
                        )
                        return None
                    current_straddle_data = {
                        'straddle_id': trade_uid,
                        'trade_uid': trade_uid,
                        'symbol': symbol,
                        'strike': atm,
                        'expiry': chain_data['expiry'],
                        'expiry_date': chain_data.get('expiry_date'),
                        'chain_publish_seq': chain_publish_seq,
                        'chain_published_at': chain_published_at,
                        'exchange_segment': exchange_segment,
                        'exchange_name': exchange_name,
                        'product_type': product_type,
                        'lot_size': lot_size,
                        'lots': lots,
                        'initial_pe_quantity': target_pe_qty,
                        'initial_ce_quantity': target_ce_qty,
                        'pe_lots': pe_filled_qty // lot_size,
                        'ce_lots': ce_filled_qty // lot_size,
                        'pe_quantity': pe_filled_qty,
                        'ce_quantity': ce_filled_qty,
                        'total_quantity': ce_filled_qty + pe_filled_qty,
                        'ce_token': ce_token,
                        'ce_symbol': ce_symbol,
                        'ce_entry_price': avg_ce_fill,
                        'pe_token': pe_token,
                        'pe_symbol': pe_symbol,
                        'pe_entry_price': avg_pe_fill,
                        'status': 'BUILDING',
                        'config': trade_config or {},
                        'entry_spot': reference_spot,
                        'ce_delta': ce_delta,
                        'pe_delta': pe_delta,
                        'net_delta': net_delta,
                        'delta_neutral': delta_neutral,
                    }

                    await loop.run_in_executor(None, state.db.insert_straddle, current_straddle_data)

                    if not is_first_fill_processed:
                        logger.info(
                            f"First chunk verified for {trade_uid}. "
                            "Trade is now live with status 'BUILDING'."
                        )
                        is_first_fill_processed = True

                    snapshot = await _get_fresh_snapshot(trade_uid)

                    try:
                        if snapshot:
                            logger.info(f"Performing in-build HEDGE check for {trade_uid}...")
                            pts_out = snapshot.get('pts_out', 0.0)
                            points_allowed = snapshot.get('points_allowed', float('inf'))
                            snapshot_net_delta = snapshot.get('net_delta', 0.0)

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
                                    trade_uid=trade_uid,
                                    net_delta=snapshot_net_delta,
                                    target_delta_reduction=-snapshot_net_delta,
                                    hedge_type="BUI_HEDGE",
                                    uid_prefix_override=f"I{trade_uid}"
                                )
                                if hedge_result and hedge_result.get('success'):
                                    logger.info(
                                        f"In-build hedge for {trade_uid} completed successfully. "
                                        "Hedge inventory WILL be included in remaining chunk target accounting."
                                    )
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
                            total_pnl = snapshot.get('total_pnl', 0.0)
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
                                    trade_uid=trade_uid,
                                    straddle_data=current_straddle_data,
                                    reason='SL'
                                )
                                if sqf_result and sqf_result.get('success'):
                                    logger.info(
                                        f"Square-off for {trade_uid} completed successfully "
                                        f"after in-build SL hit."
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

                current_global_net = _compute_net_short_inventory(
                    fills=all_verified_fills,
                    ce_token=ce_token,
                    pe_token=pe_token,
                )

                chunk_net_ce = max(
                    0,
                    current_global_net["ce_net_short_qty"] - cumulative_net_before_chunk["ce_net_short_qty"]
                )
                chunk_net_pe = max(
                    0,
                    current_global_net["pe_net_short_qty"] - cumulative_net_before_chunk["pe_net_short_qty"]
                )

                remaining_ce = max(0, chunk_target_ce - chunk_net_ce)
                remaining_pe = max(0, chunk_target_pe - chunk_net_pe)

                logger.info(
                    f"[{trade_uid}] Chunk {chunk_idx + 1} progress after iter {retry_iter + 1} | "
                    f"target_ce={chunk_target_ce}, target_pe={chunk_target_pe}, "
                    f"net_ce={chunk_net_ce}, net_pe={chunk_net_pe}, "
                    f"remaining_ce={remaining_ce}, remaining_pe={remaining_pe}"
                )

                unresolved_open_orders = len(unverified_order_ids) > 0

                remainder_orders = []
                if not unresolved_open_orders:
                    ce_remainder_order = _make_remainder_order(
                        token=ce_token,
                        option_type='CE',
                        quantity=remaining_ce,
                        expected_price=ce_ltp,
                        exchange_segment=exchange_segment,
                        product_type=product_type,
                        build_role='BUILD_CE',
                        trade_uid=trade_uid,
                        limit_order_buffer=sell_buffer * (retry_iter + 2),
                        uid_seed=f"RCE{trade_uid}{chunk_idx+1}{retry_iter+1}",
                    )
                    if ce_remainder_order:
                        remainder_orders.append(ce_remainder_order)

                    pe_remainder_order = _make_remainder_order(
                        token=pe_token,
                        option_type='PE',
                        quantity=remaining_pe,
                        expected_price=pe_ltp,
                        exchange_segment=exchange_segment,
                        product_type=product_type,
                        build_role='BUILD_PE',
                        trade_uid=trade_uid,
                        limit_order_buffer=sell_buffer * (retry_iter + 2),
                        uid_seed=f"RPE{trade_uid}{chunk_idx+1}{retry_iter+1}",
                    )
                    if pe_remainder_order:
                        remainder_orders.append(pe_remainder_order)
                else:
                    logger.warning(
                        f"[{trade_uid}] Chunk {chunk_idx + 1}: "
                        "skipping fresh remainder generation because some placed orders are still unresolved."
                    )

                if remaining_ce == 0 and remaining_pe == 0:
                    logger.info(
                        f"[{trade_uid}] Chunk {chunk_idx + 1} target achieved "
                        "after hedge-adjusted net inventory accounting."
                    )
                    orders_to_process = []
                else:
                    orders_to_process = _dedupe_retry_orders(
                        orders_to_retry_now + placement_failures_to_retry + remainder_orders
                    )
                retry_iter += 1

            current_global_net = _compute_net_short_inventory(
                fills=all_verified_fills,
                ce_token=ce_token,
                pe_token=pe_token,
            )

            chunk_net_ce = max(
                0,
                current_global_net["ce_net_short_qty"] - cumulative_net_before_chunk["ce_net_short_qty"]
            )
            chunk_net_pe = max(
                0,
                current_global_net["pe_net_short_qty"] - cumulative_net_before_chunk["pe_net_short_qty"]
            )

            final_chunk_remaining_ce = max(0, chunk_target_ce - chunk_net_ce)
            final_chunk_remaining_pe = max(0, chunk_target_pe - chunk_net_pe)

            if final_chunk_remaining_ce > 0 or final_chunk_remaining_pe > 0:
                logger.error(
                    f"After all retries, chunk {chunk_idx + 1} is still short of target | "
                    f"remaining_ce={final_chunk_remaining_ce}, remaining_pe={final_chunk_remaining_pe}"
                )
                all_failed_orders.extend(orders_to_process)

            cumulative_net_before_chunk = current_global_net
            chunk_idx += 1

        if build_aborted:
            final_straddle_data_check = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            current_status = final_straddle_data_check.get('status') if final_straddle_data_check else 'UNKNOWN'
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
        filled_ids = {_get_fill_order_id(o) for o in all_verified_fills}
        ids_to_check = list(placed_ids - filled_ids)

        if ids_to_check:
            logger.info(
                f"Pre-Sweep Check: Re-verifying {len(ids_to_check)} pending orders "
                "before calculating unfilled quantity..."
            )

            async def _pre_sweep():
                return await executor.verify_orders_bulk(
                    ids_to_check,
                    f"BUI_{trade_uid}_PRE_SWEEP",
                    trade_uid=trade_uid,
                    timeout=3.0
                )

            async def _status_update():
                if is_first_fill_processed:
                    await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'BUILDING')

            pre_sweep_result, _ = await asyncio.gather(_pre_sweep(), _status_update())

            new_fills = pre_sweep_result.get('verified_success', [])
            if new_fills:
                logger.info(f"Pre-Sweep Check: Found {len(new_fills)} new fills. Updating state.")

                app_order_id_to_uid_map = {
                    str(o.get('app_order_id') or o.get('order_id')): o.get('uid')
                    for o in all_successful_orders
                }
                app_order_id_to_meta_map = {
                    str(o.get('app_order_id') or o.get('order_id')): {
                        'uid': o.get('uid'),
                        'build_role': _norm_str(o.get('build_role') or o.get('order_role')).upper() or
                                      _classify_order_role(o, ce_token, pe_token),
                        'trade_uid': trade_uid,
                    }
                    for o in all_successful_orders
                    if (o.get('app_order_id') or o.get('order_id'))
                }

                sweep_fills_with_uid = []
                for fill in new_fills:
                    _attach_fill_metadata(fill, app_order_id_to_meta_map, ce_token, pe_token)
                    app_oid = _get_fill_order_id(fill)
                    if app_oid in app_order_id_to_uid_map:
                        fill['OrderUniqueIdentifier'] = app_order_id_to_uid_map[app_oid]
                    if fill.get('OrderUniqueIdentifier'):
                        if 'order_unique_id' not in fill:
                            fill['order_unique_id'] = fill.get('OrderUniqueIdentifier')
                        sweep_fills_with_uid.append(fill)

                all_verified_fills.extend(new_fills)
                _seed_trade_fills(trade_uid, new_fills)

                if sweep_fills_with_uid:
                    await loop.run_in_executor(None, state.db.insert_orders_bulk, sweep_fills_with_uid)
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
                await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'PARTIAL')

            _clear_trade_fills(trade_uid)
            return None

        build_stats = _compute_build_fill_stats(
            fills=all_verified_fills,
            ce_token=ce_token,
            pe_token=pe_token,
            ce_fallback_price=ce_ltp,
            pe_fallback_price=pe_ltp,
        )

        live_net_inventory = _compute_net_short_inventory(
            fills=all_verified_fills,
            ce_token=ce_token,
            pe_token=pe_token,
        )

        total_ce_filled = build_stats["ce_filled_qty"]
        total_pe_filled = build_stats["pe_filled_qty"]

        unfilled_ce = max(0, target_ce_qty - live_net_inventory["ce_net_short_qty"])
        unfilled_pe = max(0, target_pe_qty - live_net_inventory["pe_net_short_qty"])
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
        sweep_attempt = 0

        while total_unfilled_qty > 0 and not build_aborted and sweep_attempt < max_sweep_attempts:
            sweep_attempt += 1
            sweep_multiplier = sweep_attempt + 1
            sweep_buffer = sell_buffer * sweep_multiplier

            logger.info(
                f"Final Sweep (Attempt {sweep_attempt}/{max_sweep_attempts}): "
                f"CE unfilled={unfilled_ce}, PE unfilled={unfilled_pe} | "
                f"buffer={sweep_multiplier}x ({sweep_buffer:.1f})"
            )

            sweep_legs = []
            if unfilled_ce > 0:
                sweep_legs.append({
                    'token': ce_token,
                    'option_type': 'CE',
                    'action': 'SELL',
                    'total_lots': int(unfilled_ce / lot_size),
                    'lot_size': lot_size,
                    'expected_price': ce_ltp,
                    'exchange_segment': exchange_segment,
                    'product_type': product_type,
                    'build_role': 'BUILD_CE',
                    'trade_uid': trade_uid,
                })
            if unfilled_pe > 0:
                sweep_legs.append({
                    'token': pe_token,
                    'option_type': 'PE',
                    'action': 'SELL',
                    'total_lots': int(unfilled_pe / lot_size),
                    'lot_size': lot_size,
                    'expected_price': pe_ltp,
                    'exchange_segment': exchange_segment,
                    'product_type': product_type,
                    'build_role': 'BUILD_PE',
                    'trade_uid': trade_uid,
                })

            if sweep_legs:
                sweep_prefix_char = chr(87 + sweep_attempt) if sweep_attempt <= 3 else 'W'
                sweep_chunks = generate_chunked_orders(
                    trade_uid_prefix=f"{sweep_prefix_char}{trade_uid}",
                    legs_data=sweep_legs,
                    base_lots_for_trade=lots,
                    max_order_qty=max_order_qty,
                    order_lots_per_call=order_lots_per_call,
                )

                for chunk in sweep_chunks:
                    for order in chunk:
                        order['build_role'] = _classify_order_role(order, ce_token, pe_token)
                        order['order_role'] = order['build_role']
                        order['trade_uid'] = trade_uid
                        order['limit_order_buffer'] = sweep_buffer
                        order['limit_price'] = 0.0

                    logger.info(f"Executing Sweep {sweep_attempt} chunk with {len(chunk)} orders...")
                    sweep_result = await executor.execute_batch(chunk, f"W_{trade_uid}_SWEEP{sweep_attempt}")

                    for o in sweep_result.get('successful_orders', []):
                        o['build_role'] = _classify_order_role(o, ce_token, pe_token)
                        o['order_role'] = o['build_role']
                        o['trade_uid'] = trade_uid

                    sweep_app_order_id_to_uid_map = {
                        str(o.get('app_order_id')): o.get('uid')
                        for o in sweep_result.get('successful_orders', [])
                    }
                    sweep_app_order_id_to_meta_map = {
                        str(o.get('app_order_id')): {
                            'uid': o.get('uid'),
                            'build_role': o.get('build_role'),
                            'trade_uid': trade_uid,
                        }
                        for o in sweep_result.get('successful_orders', [])
                        if o.get('app_order_id')
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
                                'trade_uid': trade_uid,
                                'build_role': o.get('build_role'),
                                'order_role': o.get('order_role'),
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
                            _attach_fill_metadata(fill, sweep_app_order_id_to_meta_map, ce_token, pe_token)

                        all_verified_fills.extend(sweep_verified_fills)
                        if sweep_verified_fills:
                            _seed_trade_fills(trade_uid, sweep_verified_fills)
                            sweep_vf_with_uid = [f for f in sweep_verified_fills if f.get('OrderUniqueIdentifier')]
                            if sweep_vf_with_uid:
                                await loop.run_in_executor(None, state.db.insert_orders_bulk, sweep_vf_with_uid)

            await asyncio.sleep(0.3)

            build_stats = _compute_build_fill_stats(
                fills=all_verified_fills,
                ce_token=ce_token,
                pe_token=pe_token,
                ce_fallback_price=ce_ltp,
                pe_fallback_price=pe_ltp,
            )

            live_net_inventory = _compute_net_short_inventory(
                fills=all_verified_fills,
                ce_token=ce_token,
                pe_token=pe_token,
            )

            total_ce_filled = build_stats["ce_filled_qty"]
            total_pe_filled = build_stats["pe_filled_qty"]

            unfilled_ce = max(0, target_ce_qty - live_net_inventory["ce_net_short_qty"])
            unfilled_pe = max(0, target_pe_qty - live_net_inventory["pe_net_short_qty"])
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
            await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, final_status)

        all_fills_to_process = all_verified_fills
        if not all_fills_to_process and all_successful_orders:
            logger.warning(
                f"No verified fills found in cache for {trade_uid}. "
                "Final calculations will be based on zero filled quantity."
            )
            all_fills_to_process = []

        final_build_stats = _compute_build_fill_stats(
            fills=all_fills_to_process,
            ce_token=ce_token,
            pe_token=pe_token,
            ce_fallback_price=ce_ltp,
            pe_fallback_price=pe_ltp,
        )

        avg_ce_fill = final_build_stats["avg_ce_fill"]
        avg_pe_fill = final_build_stats["avg_pe_fill"]
        executed_ce_qty = final_build_stats["ce_filled_qty"]
        executed_pe_qty = final_build_stats["pe_filled_qty"]
        executed_total_qty = executed_ce_qty + executed_pe_qty
        executed_ce_lots = executed_ce_qty // lot_size if lot_size > 0 else 0
        executed_pe_lots = executed_pe_qty // lot_size if lot_size > 0 else 0

        build_ce_orders = final_build_stats["build_ce_orders"]
        build_pe_orders = final_build_stats["build_pe_orders"]

        final_live_net_inventory = _compute_net_short_inventory(
            fills=all_fills_to_process,
            ce_token=ce_token,
            pe_token=pe_token,
        )

        logger.info(
            f"[{trade_uid}] Final build accounting | "
            f"target_ce={target_ce_qty}, target_pe={target_pe_qty}, "
            f"executed_ce={executed_ce_qty}, executed_pe={executed_pe_qty}, "
            f"net_ce={final_live_net_inventory['ce_net_short_qty']}, "
            f"net_pe={final_live_net_inventory['pe_net_short_qty']}, "
            f"temp_hedge_fill_count={final_build_stats['hedge_fill_count']}"
        )

        reference_spot = get_synthetic_reference_spot(chain_data)
        if reference_spot <= 0:
            logger.error(f"No synthetic_spot available for {trade_uid}. Aborting final straddle save.")
            return None
        straddle_data = {
            'straddle_id': trade_uid,
            'trade_uid': trade_uid,
            'symbol': symbol,
            'strike': atm,
            'expiry': chain_data['expiry'],
            'expiry_date': chain_data.get('expiry_date'),
            'chain_publish_seq': chain_publish_seq,
            'chain_published_at': chain_published_at,
            'exchange_segment': exchange_segment,
            'exchange_name': exchange_name,
            'product_type': product_type,
            'lot_size': lot_size,
            'lots': lots,
            'initial_pe_quantity': target_pe_qty,
            'initial_ce_quantity': target_ce_qty,
            'pe_lots': executed_pe_lots,
            'ce_lots': executed_ce_lots,
            'pe_quantity': executed_pe_qty,
            'ce_quantity': executed_ce_qty,
            'quantity': 0,
            'total_quantity': executed_total_qty,
            'ce_token': ce_token,
            'ce_symbol': ce_symbol,
            'ce_entry_price': avg_ce_fill,
            'ce_delta': ce_delta,
            'ce_gamma': ce_row.get('ce_gamma', 0),
            'ce_theta': ce_row.get('ce_theta', 0),
            'ce_vega': ce_row.get('ce_vega', 0),
            'ce_iv': ce_row.get('ce_iv', 0),
            'pe_token': pe_token,
            'pe_symbol': pe_symbol,
            'pe_entry_price': avg_pe_fill,
            'pe_delta': pe_delta,
            'pe_gamma': pe_row.get('pe_gamma', 0),
            'pe_theta': pe_row.get('pe_theta', 0),
            'pe_vega': pe_row.get('pe_vega', 0),
            'pe_iv': pe_row.get('pe_iv', 0),
            'net_delta': net_delta,
            'delta_neutral': delta_neutral,
            'total_premium': (avg_ce_fill * executed_ce_qty) + (avg_pe_fill * executed_pe_qty),
            'status': final_status,
            'execution_time': total_execution_time,
            'entry_spot': reference_spot,
            'spot_price': float(chain_data.get('fut_ltp') or 0.0),
            'fut_token': chain_data.get('fut_token'),
            'entry_timestamp': get_ist_now().isoformat(),
            'closed_at': None,
            'config': trade_config or {},
            'ce_orders': build_ce_orders,
            'pe_orders': build_pe_orders,
            'all_verified_orders': all_verified_fills,
        }

        ce_order_ids = [_get_fill_order_id(o) for o in straddle_data['ce_orders']]
        pe_order_ids = [_get_fill_order_id(o) for o in straddle_data['pe_orders']]

        straddle_data['ce_order_id'] = ','.join([x for x in ce_order_ids if x]) if ce_order_ids else ''
        straddle_data['ce_app_order_id'] = ','.join([x for x in ce_order_ids if x]) if ce_order_ids else ''
        straddle_data['pe_order_id'] = ','.join([x for x in pe_order_ids if x]) if pe_order_ids else ''
        straddle_data['pe_app_order_id'] = ','.join([x for x in pe_order_ids if x]) if pe_order_ids else ''

        for order in all_fills_to_process:
            order_id = (
                order.get('app_order_id') or
                order.get('order_id') or
                order.get('AppOrderID')
            )
            if order_id:
                state.map_order_to_trade(str(order_id), trade_uid)

        try:
            await loop.run_in_executor(None, state.db.insert_straddle, straddle_data)
            logger.info(f"Straddle saved: {trade_uid}")
        except Exception as e:
            logger.error(f"Failed to save straddle to DB: {e}")

        _clear_trade_fills(trade_uid)
        logger.info(f"Cleared trade fill cache for {trade_uid} after build.")

        logger.info("Instrument subscription handled by option chain builder.")

        end_time = get_ist_now()
        total_time = (end_time - start_time).total_seconds()

        logger.debug("=" * 100)
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
        logger.debug("=" * 100)

        if final_status in ['ACTIVE', 'PARTIAL']:
            logger.info(f"Spawning dedicated process for trade {trade_uid} (status={final_status})...")
            from trading.trade_process import trade_process_worker_entry
    
            command_q = getattr(state, 'local_command_queues', {}).get(trade_uid) or multiprocessing.Queue()
            process = multiprocessing.Process(
                    target=trade_process_worker_entry,
                    args=(
                        trade_uid,
                        straddle_data,
                        command_q,
                        getattr(state, 'trade_data_cache', None) or {},
                        all_verified_fills
                    ),
                    daemon=True,
                    name=f"trade-{trade_uid}"
                )
            process.start()
    
            if trade_uid not in state.trade_processes:
                    state.trade_processes[trade_uid] = {}
            state.trade_processes[trade_uid]['pid'] = process.pid
            state.trade_processes[trade_uid]['status'] = final_status
            
            state.local_process_refs[trade_uid] = process
            state.local_command_queues[trade_uid] = command_q
    
            logger.info(f"Process for {trade_uid} started (PID: {process.pid}) and registered.")

        return {
            "success": final_status == 'ACTIVE',
            "straddle_data": straddle_data,
            "message": f"Straddle build completed with status: {final_status}"
        }

    except Exception as e:
        logger.error(f"Build failed: {e}", exc_info=True)
        if 'trade_uid' in locals() and trade_uid:
            try:
                loop = asyncio.get_running_loop()
                trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                if trade and trade.get('status') == 'BUILDING':
                    await loop.run_in_executor(None, state.db.update_straddle_status, trade_uid, 'PARTIAL')
                    logger.warning(
                        f"Build failed with exception. Reverted status to PARTIAL for "
                        f"{trade_uid} as it may be partially built."
                    )

                executor = get_order_executor()
                if executor:
                    await executor.cancel_all_open_orders_for_trade(trade_uid)

            except Exception as db_e:
                logger.error(
                    f"CRITICAL: Failed to revert status or cancel orders for {trade_uid} "
                    f"after build exception. DB Error: {db_e}"
                )
    finally:
        if 'trade_uid' in locals() and trade_uid:
            _clear_trade_fills(trade_uid)
            logger.info(
                f"Final cleanup: Cleared trade fill cache for {trade_uid} "
                "after build attempt."
            )


    async def verify_and_execute_exit_chunk(self, initial_price, current_price, *args, **kwargs):
        if not _verify_square_off_price_safety(initial_price, current_price):
            from utils.logger import logger
            logger.warning(f"[{getattr(self, 'trade_uid', 'UNKNOWN')}] ⚠️ Square-off halted: Straddle price bounced up by >= 1 bps from initial exit trigger ({initial_price} -> {current_price}). Managing remaining positions.")
            return False
        return True
