import math
import time
import asyncio
import functools
import os
import httpx
import re
import sqlite3
import json
import cred
from concurrent.futures import ThreadPoolExecutor
from Connect import XTSConnect
from typing import Dict, List, Optional, Any
from datetime import datetime
from models import state
from utils.logger import logger
from market_data import get_market_depth, get_bulk_market_depth


# ─────────────────────────────────────────────────────────────────────────────
# Exchange Segment Mappings
# ─────────────────────────────────────────────────────────────────────────────

EXCHANGE_SEGMENT_MAP = {
    2: "NSEFO",
    12: "BSEFO",
    1: "NSECM",
    11: "BSECM",
    "NSEFO": "NSEFO",
    "BSEFO": "BSEFO",
    "NSECM": "NSECM",
    "BSECM": "BSECM",
    "NSECD": "NSECD",
    "NSECO": "NSECO",
    "BSECD": "BSECD",
    "NCDEX": "NCDEX",
    "MSECM": "MSECM",
    "MSEFO": "MSEFO",
    "MSECD": "MSECD",
    "MCXFO": "MCXFO"
}

REVERSE_EXCHANGE_SEGMENT_MAP = {v: k for k, v in EXCHANGE_SEGMENT_MAP.items() if isinstance(k, int)}

EXCHANGE_DISPLAY_NAME_MAP = {
    2: "NSE", 12: "BSE", 1: "NSECM", 11: "BSECM",
    "NSEFO": "NSE", "BSEFO": "BSE", "NSECM": "NSECM", "BSECM": "BSECM"
}


# ─────────────────────────────────────────────────────────────────────────────
# OrderExecutor
# ─────────────────────────────────────────────────────────────────────────────

class OrderExecutor:
    """⚡ ULTRA-FAST PARALLEL ORDER EXECUTION ENGINE"""

    def __init__(self, xt_interactive, max_concurrent: int = 50, client_id: str = None):
        self.xt_i = xt_interactive
        self.max_concurrent = max_concurrent
        self.client_id = client_id
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent + 10)
        self._instrument_tick_cache: Dict = {}
        logger.info(f"✅ OrderExecutor initialized (max_concurrent={max_concurrent})")

    # ─────────────────────────────────────────────────────────────────────────
    # Helpers
    # ─────────────────────────────────────────────────────────────────────────

    def get_timestamp(self) -> str:
        return str(int(time.time() * 1_000_000))

    async def cancel_all_open_orders_for_trade(self, trade_uid: str):
        """Cancels all open orders belonging to a specific trade_uid to prevent rogue fills."""
        loop = asyncio.get_event_loop()
        try:
            if self.client_id:
                order_book_func = functools.partial(self.xt_i.get_order_book, clientID=self.client_id)
            else:
                order_book_func = self.xt_i.get_order_book

            order_book_resp = await loop.run_in_executor(self.executor, order_book_func)

            if order_book_resp and order_book_resp.get('type') == 'success':
                result = order_book_resp.get('result', {})
                if isinstance(result, dict):
                    broker_orders = result.get('orderList', []) or result.get('OrderList', [])
                elif isinstance(result, list):
                    broker_orders = result
                else:
                    broker_orders = []

                open_statuses = {'OPEN', 'NEW', 'REPLACED', 'PENDINGNEW', 'PARTIALLYFILLED', 'PENDINGREPLACE'}
                cancel_tasks = []

                for o in broker_orders:
                    status = str(o.get('OrderStatus', '')).upper()
                    ouid = str(o.get('OrderUniqueIdentifier', ''))
                    app_id = str(o.get('AppOrderID', ''))

                    if status in open_statuses and trade_uid in ouid:
                        logger.info(f"🛑 Cancelling stray open order {app_id} ({status}) for {trade_uid}.")
                        cancel_func = functools.partial(
                            self.xt_i.cancel_order,
                            appOrderID=app_id,
                            orderUniqueIdentifier=ouid,
                            clientID=self.client_id
                        )
                        cancel_tasks.append(loop.run_in_executor(self.executor, cancel_func))

                if cancel_tasks:
                    await asyncio.gather(*cancel_tasks, return_exceptions=True)
                    logger.info(f"✅ Cancelled {len(cancel_tasks)} stray open orders for {trade_uid}.")
                    await asyncio.sleep(1.0)
        except Exception as e:
            logger.error(f"⚠️ Error checking/cancelling stray open orders for {trade_uid}: {e}")

    def _get_instrument_tick_size(self, token: int, exchange_segment) -> float:
        """Hardcoded to 0.05 (most common for options) — API call removed to avoid failures."""
        return 0.05

    def _parse_order_book_response(self, order_book) -> List[Dict]:
        """Parse order book API response into a flat list regardless of response format."""
        if isinstance(order_book, dict):
            if order_book.get('type') == 'success':
                result = order_book.get('result', {})
                if isinstance(result, dict):
                    return result.get('orderList', []) or result.get('OrderList', [])
                elif isinstance(result, list):
                    return result
            return []
        elif isinstance(order_book, list):
            return order_book
        return []

    def _build_order_map(self, order_list: List[Dict]) -> Dict[str, Dict]:
        """Build O(1) lookup map from an order list using AppOrderID."""
        order_map = {}
        for order in order_list:
            oid = (
                order.get('AppOrderID') or order.get('appOrderID') or
                order.get('OrderID') or order.get('orderID')
            )
            if oid:
                order_map[str(oid)] = order
        return order_map

    def _classify_order_symbol(self, trading_symbol: str, order_side: str) -> tuple:
        """Returns (option_type, order_type_label) from a trading symbol."""
        if 'CE' in trading_symbol:
            return 'CE', f'CE {order_side}'
        elif 'PE' in trading_symbol:
            return 'PE', f'PE {order_side}'
        return 'UNKNOWN', order_side

    def _resolve_exchange_name(self, exchange_segment) -> str:
        return EXCHANGE_DISPLAY_NAME_MAP.get(exchange_segment, f"SEG{exchange_segment}")

    # ─────────────────────────────────────────────────────────────────────────
    # Single Order Placement
    # ─────────────────────────────────────────────────────────────────────────

    async def place_single_order(
        self,
        token: int,
        option_type: str,
        action: str,
        quantity: int,
        uid: str,
        limit_price: float,
        exchange_segment: int,
        product_type: str,
        expected_price: float = 0.0,
        order_type: str = "LIMIT",
        stop_price: float = 0.0,
        buy_buffer: float = 2.0,
        sell_buffer: float = 2.0
    ) -> Dict:
        """Place a single order. Limit price must be pre-calculated in execute_batch."""
        async with self.semaphore:
            loop = asyncio.get_event_loop()
            try:
                if exchange_segment is None:
                    exchange_segment = getattr(self.xt_i, 'EXCHANGE_NSEFO', 2)

                exchange_code = EXCHANGE_SEGMENT_MAP.get(exchange_segment, "NSEFO")
                exchange_name = self._resolve_exchange_name(exchange_segment)

                if product_type is None:
                    product_type_value = self.xt_i.PRODUCT_MIS
                else:
                    product_map = {
                        "MIS": self.xt_i.PRODUCT_MIS,
                        "NRML": self.xt_i.PRODUCT_NRML,
                        "CNC": getattr(self.xt_i, 'PRODUCT_CNC', 'CNC')
                    }
                    product_type_value = product_map.get(product_type.upper(), self.xt_i.PRODUCT_MIS)

                final_limit_price = float(limit_price)
                if final_limit_price <= 0.0:
                    raise RuntimeError(
                        f"Invalid or missing limit price ({final_limit_price:.2f}) for order {uid}. "
                        "Price must be pre-calculated in execute_batch."
                    )

                order_params = {
                    'exchangeSegment':       exchange_code,
                    'exchangeInstrumentID':  int(token),
                    'productType':           product_type_value,
                    'orderType':             self.xt_i.ORDER_TYPE_LIMIT,
                    'orderSide': (
                        self.xt_i.TRANSACTION_TYPE_BUY
                        if action.upper() == "BUY"
                        else self.xt_i.TRANSACTION_TYPE_SELL
                    ),
                    'timeInForce':           self.xt_i.VALIDITY_DAY,
                    'disclosedQuantity':     0,
                    'orderQuantity':         int(quantity),
                    'limitPrice':            final_limit_price,
                    'stopPrice':             float(stop_price),
                    'orderUniqueIdentifier': str(uid)[:20],
                    'clientID':              str(self.client_id),
                }

                logger.debug(f"🔄 [{exchange_name}] {action} {option_type} {quantity} @ token={token}")

                future = loop.run_in_executor(
                    self.executor,
                    self._place_order_sync,
                    order_params, exchange_name, option_type,
                    action, token, uid, expected_price, exchange_segment
                )
                try:
                    response = await asyncio.wait_for(future, timeout=2.0)
                except asyncio.TimeoutError:
                    logger.error(f"❌ Order placement for {uid} timed out after 2 seconds.")
                    raise RuntimeError("Order placement timed out.")

                return response

            except Exception as e:
                logger.error(
                    f"❌ {option_type} {action} FAILED | token={token} uid={uid} | {e}",
                    exc_info=True
                )
                return {
                    "success":          False,
                    "option_type":      option_type,
                    "action":           action,
                    "token":            token,
                    "uid":              uid,
                    "error":            str(e),
                    "exchange_segment": exchange_segment if exchange_segment is not None else 2
                }

    def _place_order_sync(
        self,
        order_params: Dict,
        exchange_name: str,
        option_type: str,
        action: str,
        token: int,
        uid: str,
        expected_price: float,
        exchange_segment
    ) -> Dict:
        """Synchronous order placement — runs inside thread pool."""
        try:
            response = self.xt_i.place_order(**order_params)

            if not isinstance(response, dict):
                raise RuntimeError(f"Invalid broker response: {response}")

            if "Order quantity is not a multiple of lot size" in response.get("description", ""):
                raise RuntimeError("Broker rejected: Quantity not a multiple of lot size.")

            if response.get("type") != "success":
                error_desc = response.get("description", "Unknown error")
                if "Token/Authorization not found" in error_desc or "token" in error_desc.lower():
                    logger.warning("🔄 Token expired, attempting refresh...")
                    try:
                        from cred import API_KEY_I, API_SECRET_I
                        new_xt_i = XTSConnect(API_KEY_I, API_SECRET_I, "WEBAPI")
                        login_response = new_xt_i.interactive_login()

                        if login_response.get('type') == 'success':
                            logger.info("✅ Token refreshed successfully")
                            self.xt_i = new_xt_i
                            response = self.xt_i.place_order(**order_params)

                            if not isinstance(response, dict):
                                raise RuntimeError(f"Invalid broker response after retry: {response}")
                            if response.get("type") != "success":
                                raise RuntimeError(
                                    f"Order failed after token refresh: "
                                    f"{response.get('description', 'Unknown error')}"
                                )
                        else:
                            raise RuntimeError(
                                f"Token refresh failed: {login_response.get('description', 'Login failed')}"
                            )
                    except Exception as refresh_error:
                        logger.error(f"❌ Token refresh failed: {refresh_error}")
                        raise RuntimeError(f"Token expired and refresh failed: {error_desc}")
                else:
                    raise RuntimeError(f"Broker rejected order: {error_desc}")

            result = response.get("result", {})
            app_order_id = result.get("AppOrderID")
            if not app_order_id:
                raise RuntimeError("No AppOrderID in broker response")

            logger.info(f"✅ [{exchange_name}] {action} {option_type}: OrderID={app_order_id}")

            return {
                "success":          True,
                "option_type":      option_type,
                "action":           action,
                "token":            token,
                "quantity":         order_params.get('orderQuantity', 0),
                "order_id":         str(app_order_id),
                "app_order_id":     app_order_id,
                "uid":              uid,
                "fill_price":       expected_price,
                "expected_price":   expected_price,
                "exchange_segment": exchange_segment,
                "exchange_name":    exchange_name
            }

        except Exception as e:
            logger.error(f"❌ Order placement failed: {e}")
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # Order Modification / Chase
    # ─────────────────────────────────────────────────────────────────────────

    async def modify_and_chase_order(
        self,
        pending_order_data: Dict,
        attempt_number: int = 0,
        trade_uid: str = None
    ) -> Dict:
        """
        Modifies a pending limit order with an escalating aggressive price.

        FIX: If market depth is unavailable (subscription lag / new token),
        the order is LEFT OPEN and a retry signal is returned instead of
        cancelling a perfectly valid resting order.
        Cancellation on depth failure only fires for OPEN/REPLACED (stale reprices).
        """
        app_order_id     = pending_order_data.get('AppOrderID')
        original_uid     = pending_order_data.get('OrderUniqueIdentifier')
        token            = pending_order_data.get('ExchangeInstrumentID')
        order_side       = pending_order_data.get('OrderSide')
        exchange_segment = pending_order_data.get('ExchangeSegment')
        order_status     = str(pending_order_data.get('OrderStatus', '')).upper()

        if not all([app_order_id, original_uid, token, order_side, exchange_segment]):
            msg = f"Missing required data for order modification: {app_order_id}"
            logger.error(msg)
            return {'success': False, 'error': msg}

        try:
            loop = asyncio.get_event_loop()

            buy_buffer = sell_buffer = 2.0
            if trade_uid and hasattr(state, 'db'):
                trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                if trade and 'config' in trade:
                    symbol = trade.get('symbol', '').upper()
                    if "SENSEX" in symbol:
                        buy_buffer = sell_buffer = 6.0
                    buy_buffer  = float(trade['config'].get('buy_buffer', buy_buffer))
                    sell_buffer = float(trade['config'].get('sell_buffer', sell_buffer))

            depth = await get_market_depth(token, exchange_segment)

            if not (depth and depth.get('bid_price', 0) > 0 and depth.get('ask_price', 0) > 0):
                # ── FIX: Depth unavailable ────────────────────────────────────────────────
                # For NEW orders: depth subscription may not be ready yet.
                # DO NOT cancel — leave order open and let the next verification pass retry.
                # Only cancel for OPEN/REPLACED (genuinely stale limit orders needing reprice).
                # ─────────────────────────────────────────────────────────────────────────
                if order_status == 'NEW':
                    logger.warning(
                        f"⚠️ No depth for token {token} on NEW order {app_order_id}. "
                        f"Leaving open — next verification pass will retry."
                    )
                    return {
                        'success':      False,
                        'status':       'DEPTH_UNAVAILABLE_RETRY',
                        'order_id':     app_order_id,
                        'should_retry': True,
                        'error':        'No depth data yet; order left open for re-verification.'
                    }
                else:
                    # OPEN / REPLACED — these need repricing, safe to cancel
                    logger.warning(
                        f"⚠️ Could not get market depth for token {token} "
                        f"on {order_status} order {app_order_id}. Attempting to CANCEL."
                    )
                    cancel_func = functools.partial(
                        self.xt_i.cancel_order,
                        appOrderID=app_order_id,
                        orderUniqueIdentifier=original_uid,
                        clientID=self.client_id
                    )
                    cancel_response = await loop.run_in_executor(self.executor, cancel_func)

                    if cancel_response and cancel_response.get('type') == 'success':
                        logger.info(f"✅ Order {app_order_id} cancelled (failed depth fetch).")
                        return {
                            'success':  False,
                            'status':   'CANCELLED',
                            'order_id': app_order_id,
                            'error':    'Cancelled due to failed market depth fetch.'
                        }
                    else:
                        error_msg = cancel_response.get('description', 'Cancellation failed') if cancel_response else 'No response'
                        logger.error(f"❌ Failed to cancel order {app_order_id}: {error_msg}")
                        return {
                            'success':  False,
                            'status':   'CANCEL_FAILED',
                            'order_id': app_order_id,
                            'error':    error_msg
                        }

            tick_size          = self._get_instrument_tick_size(token, exchange_segment)
            base_buffer_ticks  = 2
            escalation_factor  = attempt_number + 2
            chase_buffer_ticks = base_buffer_ticks * escalation_factor

            logger.info(
                f"🔥 Chasing order {app_order_id} "
                f"(Attempt: {attempt_number + 1}, Buffer Ticks: {chase_buffer_ticks})"
            )

            if order_side == self.xt_i.TRANSACTION_TYPE_BUY:
                new_limit_price = depth['ask_price'] + (chase_buffer_ticks)
            else:
                new_limit_price = depth['bid_price'] - (chase_buffer_ticks)

            if new_limit_price <= 0:
                msg = f"Calculated chase price is not positive ({new_limit_price:.2f}). Aborting."
                logger.error(f"❌ {msg}")
                return {'success': False, 'error': msg}

            final_price = round(new_limit_price , 2)

            mod_params = {
                'appOrderID':                app_order_id,
                'modifiedProductType':       pending_order_data.get('ProductType'),
                'modifiedOrderType':         self.xt_i.ORDER_TYPE_LIMIT,
                'modifiedOrderQuantity':     pending_order_data.get('OrderQuantity'),
                'modifiedDisclosedQuantity': pending_order_data.get('OrderDisclosedQuantity'),
                'modifiedLimitPrice':        final_price,
                'modifiedStopPrice':         pending_order_data.get('OrderStopPrice'),
                'modifiedTimeInForce':       pending_order_data.get('TimeInForce'),
                'orderUniqueIdentifier':     original_uid,
                'clientID':                  self.client_id
            }

            mod_func = functools.partial(self.xt_i.modify_order, **mod_params)
            response = await loop.run_in_executor(self.executor, mod_func)

            if response and response.get('type') == 'success':
                logger.info(f"✅ Order {app_order_id} modified → ₹{final_price:.2f}")
                return {'success': True, 'order_id': app_order_id, 'response': response}
            else:
                error_msg = response.get('description', 'Modification failed') if response else 'No response'
                logger.error(f"❌ Failed to modify order {app_order_id}: {error_msg}")
                return {'success': False, 'order_id': app_order_id, 'error': error_msg}

        except Exception as e:
            logger.error(f"❌ Exception while chasing order {app_order_id}: {e}", exc_info=True)
            return {'success': False, 'order_id': app_order_id, 'error': str(e)}

    # ─────────────────────────────────────────────────────────────────────────
    # Batch Execution
    # ─────────────────────────────────────────────────────────────────────────

    async def execute_batch(
        self,
        orders: List[Dict],
        batch_name: str = "BATCH",
        progress_interval: int = 5,
        max_retries: int = 2,
        retry_delay: float = 1.0
    ) -> Dict:
        """
        Execute a batch of orders with automatic retry on failure.
        Limit prices are calculated via bulk market depth before dispatch.
        Verification must be done separately via verify_orders_bulk().
        """
        execution_start = time.time()
        logger.info("=" * 100)
        logger.info(f"⚡ {batch_name} | Total: {len(orders)} orders | Max Retries: {max_retries}")
        logger.info("=" * 100)

        all_successful_orders: List[Dict] = []
        final_failed_orders:   List[Dict] = []
        orders_to_attempt = list(orders)

        for attempt in range(max_retries + 1):
            if not orders_to_attempt:
                break

            current_batch_name = f"{batch_name} (Attempt {attempt + 1}/{max_retries + 1})"
            if attempt > 0:
                logger.info(f"⏳ Waiting {retry_delay}s before retrying {len(orders_to_attempt)} orders...")
                await asyncio.sleep(retry_delay)
                logger.info(f"🔄 Retrying → {current_batch_name}")

            # ── Bulk Price Calculation ──────────────────────────────────────
            orders_needing_price = [o for o in orders_to_attempt if o.get('limit_price', 0.0) <= 0.0]
            if orders_needing_price:
                logger.info(
                    f"🔄 Calculating limit prices for {len(orders_needing_price)} orders "
                    f"in '{current_batch_name}'..."
                )
                instruments_for_depth = []
                for o in orders_needing_price:
                    seg = o.get('exchange_segment')
                    if isinstance(seg, str):
                        seg = REVERSE_EXCHANGE_SEGMENT_MAP.get(seg.upper(), 2)
                    elif seg is None:
                        seg = 2
                    instruments_for_depth.append({
                        'exchangeSegment':      seg,
                        'exchangeInstrumentID': int(o['token'])
                    })

                depth_map = await get_bulk_market_depth(instruments_for_depth)

                for order in orders_needing_price:
                    calc_price = 0.0
                    tick_size  = self._get_instrument_tick_size(order['token'], order.get('exchange_segment'))
                    buffer     = order.get('limit_order_buffer', 2.0)
                    action     = order.get('action') or order.get('order_side', '')

                    depth = depth_map.get(int(order['token']))
                    if depth and depth.get('bid_price', 0) > 0 and depth.get('ask_price', 0) > 0:
                        bid_price = depth['bid_price']
                        ask_price = depth['ask_price']
                        if action.upper() == "BUY":
                            calc_price = ask_price + (buffer )
                        else:
                            calc_price = bid_price - (buffer)
                            if calc_price <= 0:
                                calc_price = bid_price
                        logger.info(
                            f"✅ Price [{order['uid']}] {action} | "
                            f"Bid={bid_price:.2f} Ask={ask_price:.2f} "
                            f"Buffer={buffer} → {calc_price:.2f}"
                        )
                    else:
                        logger.warning(
                            f"⚠️ Depth failed for token {order['token']}. "
                            f"Falling back to expected_price for [{order['uid']}]."
                        )
                        expected_price = order.get('expected_price', 0.0)
                        if expected_price > 0:
                            if action.upper() == "BUY":
                                calc_price = expected_price + (buffer )
                            else:
                                calc_price = expected_price - (buffer)
                                if calc_price <= 0:
                                    calc_price = buffer
                            logger.info(
                                f"✅ Fallback price [{order['uid']}] | "
                                f"Expected={expected_price:.2f} → {calc_price:.2f}"
                            )
                        else:
                            logger.error(
                                f"❌ No depth and no expected_price for token {order['token']} "
                                f"[{order['uid']}]. Order will fail."
                            )

                    if calc_price > 0:
                        rounded = calc_price
                        order['limit_price'] = round(rounded, 2)
                        logger.info(f"   → Final price [{order['uid']}]: {order['limit_price']:.2f}")
                    else:
                        order['limit_price'] = 0

            # ── Task Dispatch ───────────────────────────────────────────────
            tasks:       List       = []
            temp_failed: List[Dict] = []

            for order in orders_to_attempt:
                action = order.get('action') or order.get('order_side')
                if not action:
                    err = "Order failed pre-flight: Missing 'action' or 'order_side' key."
                    logger.error(f"❌ {err} | Order: {order}")
                    temp_failed.append({"success": False, "error": err, **order})
                    continue

                tasks.append(self.place_single_order(
                    token=            order['token'],
                    option_type=      order.get('option_type', 'UNKNOWN'),
                    action=           action,
                    quantity=         order['quantity'],
                    uid=              order['uid'],
                    limit_price=      order.get('limit_price', 0.0),
                    exchange_segment= order.get('exchange_segment'),
                    product_type=     order.get('product_type'),
                    expected_price=   order.get('expected_price', 0.0),
                    stop_price=       order.get('stop_price', 0.0)
                ))

            # ── Collect Results ─────────────────────────────────────────────
            completed        = 0
            total            = len(orders_to_attempt)
            temp_successful: List[Dict] = []

            for coro in asyncio.as_completed(tasks):
                result = await coro
                completed += 1

                if result.get("success"):
                    temp_successful.append(result)
                else:
                    temp_failed.append(result)
                    logger.error(
                        f"❌ [{current_batch_name}] {result.get('option_type')} "
                        f"{result.get('action')}: {result.get('error')}"
                    )

                if completed % progress_interval == 0 or completed == total:
                    logger.info(
                        f"⚡ [{current_batch_name}] {completed}/{total} | "
                        f"✅ {len(temp_successful)} | ❌ {len(temp_failed)}"
                    )

            all_successful_orders.extend(temp_successful)

            if temp_failed and attempt < max_retries:
                failed_uids       = {f['uid'] for f in temp_failed}
                orders_to_attempt = [o for o in orders_to_attempt if o['uid'] in failed_uids]
            else:
                final_failed_orders = temp_failed
                orders_to_attempt   = []

        # ── Final Stats ─────────────────────────────────────────────────────
        execution_time = time.time() - execution_start
        stats: Dict = {}

        for result in all_successful_orders:
            key = f"{result.get('exchange_name', 'NSE')}_{result['action']}_{result['option_type']}"
            if key not in stats:
                stats[key] = {'count': 0, 'quantity': 0, 'fill_prices': []}
            stats[key]['count']    += 1
            stats[key]['quantity'] += result['quantity']
            stats[key]['fill_prices'].append(result['fill_price'])

        for key, data in stats.items():
            data['avg_fill_price'] = (
                sum(data['fill_prices']) / len(data['fill_prices'])
                if data['fill_prices'] else 0.0
            )

        logger.info("=" * 100)
        logger.info(f"✅ {batch_name} FINAL | Time: {execution_time:.2f}s")
        logger.info(
            f"✅ Success: {len(all_successful_orders)}/{len(orders)} | "
            f"❌ Failed: {len(final_failed_orders)}"
        )
        for key, data in stats.items():
            logger.info(f"   {key}: {data['quantity']} @ ₹{data['avg_fill_price']:.2f}")
        logger.info("=" * 100)

        return {
            'success':           len(final_failed_orders) == 0,
            'total_orders':      len(orders),
            'successful_count':  len(all_successful_orders),
            'failed_count':      len(final_failed_orders),
            'successful_orders': all_successful_orders,
            'failed_orders':     final_failed_orders,
            'stats':             stats,
            'execution_time':    execution_time,
            'batch_name':        batch_name
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Bulk Order Verification
    # ─────────────────────────────────────────────────────────────────────────

    async def verify_orders_bulk(
        self,
        order_ids: List[str],
        batch_name: str = "VERIFY",
        trade_uid: str = None,
        timeout: float = 5.0
    ) -> Dict:
        """
        Bulk verification: fetch the order book ONCE and verify all order IDs in memory.

        Status triage:
          - FILLED / COMPLETE / TRADED / EXECUTED  → verified_success
          - REJECTED / CANCELLED / CANCELED        → verified_failed (terminal)
          - NEW (pass 1)                           → wait one free cycle
          - NEW (pass 2+)                          → cancel + REEXECUTE_NEEDED
          - OPEN / REPLACED                        → cancel + REEXECUTE_NEEDED
          - PARTIALLYFILLED                        → chase via modify_and_chase_order()
          - PENDINGCANCEL / PENDINGREPLACE /
            PENDINGNEW                             → wait (still_pending)
          - not found in order book yet            → still_pending (re-poll)
          - timeout                                → verified_failed (TIMEOUT)
        """
        verified_success: List[Dict] = []
        verified_failed:  List[Dict] = []

        pending_ids = set(str(oid) for oid in order_ids)
        start_time  = time.time()

        FILLED_STATUSES        = {'FILLED', 'COMPLETE', 'TRADED', 'EXECUTED'}
        TERMINAL_FAIL_STATUSES = {'REJECTED', 'CANCELLED', 'CANCELED'}
        CANCEL_AND_RETRY       = {'OPEN', 'REPLACED'}
        WAIT_FOR_FILL_STATUSES = {'NEW'}
        MODIFY_STATUSES        = {'PARTIALLYFILLED'}
        WAIT_STATUSES          = {'PENDINGCANCEL', 'PENDINGREPLACE', 'PENDINGNEW'}

        try:
            if not self.xt_i:
                logger.error("❌ Cannot verify — XTS connection not available")
                return {'verified_success': [], 'verified_failed': []}

            loop = asyncio.get_event_loop()

            async def _fetch_order_book() -> List[Dict]:
                try:
                    if self.client_id:
                        fn = functools.partial(self.xt_i.get_order_book, clientID=self.client_id)
                    else:
                        fn = self.xt_i.get_order_book
                    raw = await loop.run_in_executor(self.executor, fn)
                    return self._parse_order_book_response(raw)
                except Exception as e:
                    logger.error(f"❌ Order book fetch failed: {e}")
                    return []

            logger.info(f"📊 Polling verification for {len(order_ids)} orders (Timeout: {timeout}s)...")

            # ── CHANGE 1: counters for NEW sighting tracking ──────────────────
            new_sighting_count: Dict[str, int] = {}
            MAX_NEW_WAIT_CYCLES = 1  # 1 free pass, then cancel+reexec on 2nd sighting
            # ─────────────────────────────────────────────────────────────────

            while pending_ids and (time.time() - start_time < timeout):
                order_list = await _fetch_order_book()
                if not order_list:
                    await asyncio.sleep(0.5)
                    continue

                order_map   = self._build_order_map(order_list)
                still_pending = set()

                for order_id in list(pending_ids):
                    if order_id not in order_map:
                        # Not in order book yet — propagation lag, keep waiting
                        still_pending.add(order_id)
                        continue

                    broker_order   = order_map[order_id]
                    status         = str(broker_order.get('OrderStatus', 'UNKNOWN')).upper()
                    order_side     = broker_order.get('OrderSide', 'UNKNOWN')
                    filled_qty     = int(broker_order.get('CumulativeQuantity', 0) or 0)
                    order_qty      = int(broker_order.get('OrderQuantity', 0) or 0)
                    avg_price      = float(broker_order.get('OrderAverageTradedPrice', 0) or 0)
                    exchange_seg   = broker_order.get('ExchangeSegment', 0)
                    trading_symbol = broker_order.get('TradingSymbol', '')
                    exchange_name  = self._resolve_exchange_name(exchange_seg)
                    option_type, order_type_label = self._classify_order_symbol(trading_symbol, order_side)

                    if status in FILLED_STATUSES:
                        logger.info(f"✅ [{batch_name}] FILLED: {order_id} @ ₹{avg_price:.2f}")
                        verified_success.append({
                            'order_id': order_id, 'status': status,
                            'option_type': option_type, 'action': order_side,
                            'order_type': order_type_label, 'order_side': order_side,
                            'filled_qty': filled_qty, 'quantity': filled_qty,
                            'order_qty': order_qty, 'avg_price': avg_price,
                            'fill_price': avg_price, 'exchange_segment': exchange_seg,
                            'exchange_name': exchange_name, 'trading_symbol': trading_symbol,
                            **broker_order
                        })
                        pending_ids.remove(order_id)

                    elif status in TERMINAL_FAIL_STATUSES:
                        reason = broker_order.get('OrderRejectionReason', 'Unknown')
                        logger.error(f"❌ [{batch_name}] {status}: {order_id} - {reason}")
                        verified_failed.append({
                            'order_id': order_id, 'status': status,
                            'order_type': order_type_label, 'reason': reason,
                            'exchange_segment': exchange_seg
                        })
                        pending_ids.remove(order_id)

                    elif status in WAIT_FOR_FILL_STATUSES:  # NEW
                        # ── CHANGE 2: counter-based NEW handling ──────────────
                        new_sighting_count[order_id] = new_sighting_count.get(order_id, 0) + 1

                        if new_sighting_count[order_id] <= MAX_NEW_WAIT_CYCLES:
                            # First sighting — just placed, give one free pass
                            logger.info(
                                f"⏳ [{batch_name}] NEW: {order_id} — "
                                f"waiting for exchange fill (pass {new_sighting_count[order_id]})..."
                            )
                            still_pending.add(order_id)
                        else:
                            # Stuck in NEW — stale price or closed market, cancel + re-exec
                            logger.warning(
                                f"⚠️ [{batch_name}] NEW→STALE: {order_id} stuck for "
                                f"{new_sighting_count[order_id]} polls. Cancelling for re-exec."
                            )
                            original_uid = broker_order.get('OrderUniqueIdentifier')
                            if original_uid:
                                try:
                                    cancel_func = functools.partial(
                                        self.xt_i.cancel_order,
                                        appOrderID=order_id,
                                        orderUniqueIdentifier=original_uid,
                                        clientID=self.client_id
                                    )
                                    cancel_response = await loop.run_in_executor(self.executor, cancel_func)

                                    if cancel_response and cancel_response.get('type') == 'success':
                                        # Cancel confirmed by broker — safe to re-execute
                                        logger.info(f"✅ Stale NEW order {order_id} cancelled — will re-execute.")
                                        verified_failed.append({
                                            'order_id': order_id,
                                            'status': 'REEXECUTE_NEEDED',
                                            'reason': f'NEW for {new_sighting_count[order_id]} polls, cancelled for re-exec'
                                        })
                                        pending_ids.remove(order_id)
                                    else:
                                        # Cancel rejected — order likely filled just before cancel arrived
                                        # Do NOT re-execute. Put back in pending for one more poll to confirm fill.
                                        err_desc = cancel_response.get('description', 'Unknown') if cancel_response else 'No response'
                                        logger.warning(
                                            f"⚠️ Cancel REJECTED for stale NEW order {order_id} "
                                            f"({err_desc}). Possible race: filled before cancel. "
                                            f"Re-polling to confirm fill — NOT re-executing."
                                        )
                                        new_sighting_count[order_id] = 0  # reset grace period
                                        still_pending.add(order_id)
                                        pending_ids.remove(order_id)
                                        
                                except Exception as e:
                                    logger.error(f"❌ Cancel failed for stale NEW order {order_id}: {e}")
                                    verified_failed.append({
                                        'order_id': order_id,
                                        'status': 'CANCEL_FAILED',
                                        'reason': str(e)
                                    })
                                    pending_ids.remove(order_id)
                            else:
                                verified_failed.append({
                                    'order_id': order_id,
                                    'status': 'REEXECUTE_NEEDED',
                                    'reason': 'Stale NEW, no UID to cancel'
                                })
                                pending_ids.remove(order_id)
                        # ─────────────────────────────────────────────────────

                    elif status in CANCEL_AND_RETRY:
                        # OPEN / REPLACED — unfilled limit, cancel and re-exec at fresh price
                        logger.warning(f"⏳ [{batch_name}] {status}: {order_id}. Cancelling for re-exec.")
                        original_uid = broker_order.get('OrderUniqueIdentifier')
                        if original_uid:
                            try:
                                cancel_func = functools.partial(
                                    self.xt_i.cancel_order,
                                    appOrderID=order_id,
                                    orderUniqueIdentifier=original_uid,
                                    clientID=self.client_id
                                )
                                await loop.run_in_executor(self.executor, cancel_func)
                                verified_failed.append({
                                    'order_id': order_id,
                                    'status': 'REEXECUTE_NEEDED',
                                    'reason': f'Cancelled {status} for re-exec'
                                })
                                pending_ids.remove(order_id)
                            except Exception as e:
                                logger.error(f"❌ Cancel failed for {order_id}: {e}")
                                verified_failed.append({
                                    'order_id': order_id,
                                    'status': 'CANCEL_FAILED',
                                    'reason': str(e)
                                })
                                pending_ids.remove(order_id)
                        else:
                            still_pending.add(order_id)

                    elif status in MODIFY_STATUSES:
                        logger.warning(f"⏳ [{batch_name}] {status}: {order_id}. Chasing...")
                        attempt_num = 0
                        match = re.search(r'_ATTEMPT(\d+)', batch_name)
                        if match:
                            attempt_num = int(match.group(1)) - 1

                        mod_result = await self.modify_and_chase_order(
                            broker_order, attempt_number=attempt_num, trade_uid=trade_uid
                        )
                        if mod_result.get('should_retry'):
                            still_pending.add(order_id)
                        elif not mod_result.get('success'):
                            verified_failed.append({
                                'order_id': order_id, 'status': 'MODIFY_FAILED',
                                'reason': mod_result.get('error')
                            })
                            pending_ids.remove(order_id)
                        else:
                            still_pending.add(order_id)

                    elif status in WAIT_STATUSES:
                        # Broker-side pending transitions — just wait
                        still_pending.add(order_id)

                    else:
                        # Unhandled status — keep waiting until timeout
                        logger.debug(f"[{batch_name}] Unhandled status '{status}' for {order_id}. Waiting...")
                        still_pending.add(order_id)

                pending_ids = still_pending
                if pending_ids:
                    await asyncio.sleep(0.5)

            # Handle timeouts
            for oid in pending_ids:
                logger.warning(f"⚠️ [{batch_name}] Verification timed out for {oid}")
                verified_failed.append({
                    'order_id': oid, 'status': 'TIMEOUT', 'reason': 'Verification timed out'
                })

        except Exception as e:
            import traceback
            logger.error(f"❌ Bulk verification error: {e}")
            logger.error(traceback.format_exc())

        return {
            'verified_success': verified_success,
            'verified_failed':  verified_failed
        }


# ─────────────────────────────────────────────────────────────────────────────
# Global Executor Singleton
# ─────────────────────────────────────────────────────────────────────────────

global_executor: Optional[OrderExecutor] = None


def _initialize_worker_executor() -> bool:
    """
    Auto-initializes the global OrderExecutor for worker processes
    by reading the shared XTS session token from SQLite.
    """
    try:
        db_path = os.path.abspath("shared_tokens.db")
        if not os.path.exists(db_path):
            logger.error(f"❌ Worker Init: Shared token DB not found at {db_path}")
            return False

        conn   = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM tokens WHERE key = 'xts_interactive_token'")
        result = cursor.fetchone()
        conn.close()

        if not result:
            logger.error(f"❌ Worker Init: Key 'xts_interactive_token' not found in {db_path}")
            return False

        token_data  = json.loads(result[0])
        token       = token_data.get('token')
        user_id     = token_data.get('userID')
        is_investor = token_data.get('isInvestorClient')

        if not (token and user_id):
            logger.error("❌ Worker Init: Token data incomplete in DB")
            return False

        logger.info(f"🔄 Auto-initializing OrderExecutor for worker (User: {user_id})")

        xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        xt._set_common_variables(token, user_id, is_investor)
        xt.isInvestorClient = False

        client_id = getattr(cred, 'clientID', user_id)
        logger.info(f"   Using clientID: '{client_id}' for order placement in worker.")

        set_order_executor(xt, max_concurrent=20, client_id=client_id)
        return True

    except Exception as e:
        logger.error(f"❌ Failed to auto-initialize worker executor: {e}", exc_info=True)
        return False


def set_order_executor(xt_interactive, max_concurrent: int = 20, client_id: str = None) -> None:
    """Set (or replace) the global OrderExecutor instance."""
    global global_executor
    global_executor = OrderExecutor(xt_interactive, max_concurrent, client_id)
    logger.info(f"✅ Global OrderExecutor set (max_concurrent={max_concurrent})")


def get_order_executor() -> Optional[OrderExecutor]:
    """
    Returns the global OrderExecutor.
    Auto-initializes from shared_tokens.db if not yet set (worker process support).
    """
    global global_executor
    if global_executor is None:
        _initialize_worker_executor()
    return global_executor
