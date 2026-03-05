"""
Ultra-Fast Parallel Order Execution Engine
Supports: NSE F&O, BSE F&O, and other segments dynamically
Verification is separate - call verify_orders_bulk() independently
"""
import asyncio
import time  # ✅ FIX: Add missing import
import functools
import os
import httpx
import sqlite3
import json
import cred
from concurrent.futures import ThreadPoolExecutor
from Connect import XTSConnect
from typing import Dict, List, Optional
from datetime import datetime # Keep this
from models import state
from utils.logger import logger # Keep this
from market_data import get_market_depth, get_bulk_market_depth # Removed get_xts_market_api as it's not used and is a microservice internal


# ✅ Exchange segment code mapping
EXCHANGE_SEGMENT_MAP = {
    2: "NSEFO",
    12: "BSEFO",
    1: "NSECM",
    11: "BSECM",
    # String to string passthrough
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

# --- NEW: Reverse map for converting string codes back to integers ---
REVERSE_EXCHANGE_SEGMENT_MAP = {v: k for k, v in EXCHANGE_SEGMENT_MAP.items() if isinstance(k, int)}
# This will create: {'NSEFO': 2, 'BSEFO': 12, 'NSECM': 1, 'BSECM': 11}
# --- END NEW ---


class OrderExecutor:
    """⚡ ULTRA-FAST PARALLEL ORDER EXECUTION ENGINE"""
    
    def __init__(self, xt_interactive, max_concurrent: int = 50, client_id: str = None):
        self.xt_i = xt_interactive
        self.max_concurrent = max_concurrent
        self.client_id = client_id
        self.semaphore = asyncio.Semaphore(max_concurrent)
        # Initialize a dedicated thread pool with size matching max_concurrent + buffer
        # This prevents thread starvation if the default pool is smaller than max_concurrent
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent + 10)
        self._instrument_tick_cache = {}  # ✅ Cache for instrument tick sizes
        logger.info(f"✅ OrderExecutor initialized (max_concurrent={max_concurrent})")

    def get_timestamp(self) -> str:
        """Returns a high-resolution timestamp string for unique IDs."""
        return str(int(time.time() * 1_000_000))
    
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
        # The following are now ignored by this function but kept for compatibility
        buy_buffer: float = 2.0,
        sell_buffer: float = 2.0
    ) -> Dict:
        """Place a single order with dynamic exchange support"""
        async with self.semaphore:
            loop = asyncio.get_event_loop()
            try:
                # ✅ Use provided exchange_segment or default to NSEFO
                if exchange_segment is None:
                    exchange_segment = getattr(self.xt_i, 'EXCHANGE_NSEFO', 2)

                # ✅ CRITICAL: Convert numeric exchange code to string code
                exchange_code = EXCHANGE_SEGMENT_MAP.get(exchange_segment, "NSEFO")

                # ✅ Use provided product_type or default to MIS
                if product_type is None:
                    product_type_value = self.xt_i.PRODUCT_MIS
                else:
                    product_map = {
                        "MIS": self.xt_i.PRODUCT_MIS,
                        "NRML": self.xt_i.PRODUCT_NRML,
                        "CNC": getattr(self.xt_i, 'PRODUCT_CNC', 'CNC')
                    }
                    product_type_value = product_map.get(product_type.upper(), self.xt_i.PRODUCT_MIS)

                # ✅ Get exchange name for logging
                exchange_name = {
                    2: "NSE", 12: "BSE", 1: "NSECM", 11: "BSECM",
                    "NSEFO": "NSE", "BSEFO": "BSE", "NSECM": "NSECM", "BSECM": "BSECM"
                }.get(exchange_segment, f"SEG{exchange_segment}")

                # --- REFACTOR: Price calculation is now done in execute_batch. ---
                # We now expect a valid limit_price to be passed in.
                final_order_type = self.xt_i.ORDER_TYPE_LIMIT
                final_limit_price = float(limit_price)

                if final_limit_price <= 0.0:
                    error_msg = f"Invalid or missing limit price ({final_limit_price:.2f}) for order {uid}. Price must be pre-calculated in execute_batch."
                    logger.error(f"❌ {error_msg}")
                    raise RuntimeError(error_msg)

                # ✅ CRITICAL FIX: Use STRING exchange code (e.g., "NSEFO", "BSEFO")
                order_params = {
                    'exchangeSegment': exchange_code,  # ✅ MUST BE STRING CODE
                    'exchangeInstrumentID': int(token),
                    'productType': product_type_value,
                    'orderType': final_order_type,
                    'orderSide': (
                        self.xt_i.TRANSACTION_TYPE_BUY
                        if action.upper() == "BUY"
                        else self.xt_i.TRANSACTION_TYPE_SELL
                    ),
                    'timeInForce': self.xt_i.VALIDITY_DAY,
                    'disclosedQuantity': 0,
                    'orderQuantity': int(quantity),
                    'limitPrice': final_limit_price,
                    'stopPrice': float(stop_price),
                    'orderUniqueIdentifier': str(uid)[:20],
                    'clientID': str(self.client_id),
                    # 'apiOrderSource': self.xt_i.source  # ✅ ADDED: Pass the required apiOrderSource
                }

                logger.debug(f"🔄 [{exchange_name}] {action} {option_type} {quantity} @ token={token}")

                # ✅ RUN SYNCHRONOUS XTS API CALL IN THREAD POOL TO AVOID BLOCKING
                # --- FIX: Add a timeout to prevent the application from hanging on a stuck network call ---
                future = loop.run_in_executor(self.executor, self._place_order_sync, order_params, exchange_name, option_type, action, token, uid, expected_price, exchange_segment)
                try:
                    # ✅ Set a timeout (15 seconds) for the entire order placement process.
                    response = await asyncio.wait_for(future, timeout=15.0)
                except asyncio.TimeoutError:
                    logger.error(f"❌ Order placement for {uid} timed out after 15 seconds. The broker API call is likely stuck.")
                    raise RuntimeError("Order placement timed out.")

                return response

            except Exception as e:
                logger.error(
                    f"❌ {option_type} {action} FAILED | token={token} uid={uid} | {e}",
                    exc_info=True
                )
                return {
                    "success": False,
                    "option_type": option_type,
                    "action": action,
                    "token": token,
                    "uid": uid,
                    "error": str(e),
                    "exchange_segment": exchange_segment if exchange_segment else 2
                }

    def _place_order_sync(self, order_params, exchange_name, option_type, action, token, uid, expected_price, exchange_segment):
        """Synchronous order placement for thread pool execution"""
        try:
            response = self.xt_i.place_order(**order_params)

            if not isinstance(response, dict):
                raise RuntimeError(f"Invalid broker response: {response}")
            
            if "Order quantity is not a multiple of lot size" in response.get("description", ""):
                raise RuntimeError(f"Broker rejected: Quantity not a multiple of lot size.")

            if response.get("type") != "success":
                error_desc = response.get("description", "Unknown error")

                # Check if token expired - try to refresh
                if "Token/Authorization not found" in error_desc or "token" in error_desc.lower():
                    logger.warning("🔄 Token expired, attempting refresh...")
                    try:
                        from cred import API_KEY_I, API_SECRET_I
                        from Connect import XTSConnect

                        new_xt_i = XTSConnect(API_KEY_I, API_SECRET_I, "WEBAPI")
                        login_response = new_xt_i.interactive_login()

                        if login_response.get('type') == 'success':
                            logger.info("✅ Token refreshed successfully")
                            self.xt_i = new_xt_i

                            # Update product type after refresh
                            product_type_value = order_params['productType']

                            order_params['productType'] = product_type_value

                            # Retry the order with new token
                            response = self.xt_i.place_order(**order_params)

                            if not isinstance(response, dict):
                                raise RuntimeError(f"Invalid broker response after retry: {response}")

                            if response.get("type") != "success":
                                raise RuntimeError(f"Order failed after token refresh: {response.get('description', 'Unknown error')}")
                        else:
                            raise RuntimeError(f"Token refresh failed: {login_response.get('description', 'Login failed')}")
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
                "success": True,
                "option_type": option_type,
                "action": action,
                "token": token,
                "quantity": order_params.get('orderQuantity', 0),
                "order_id": str(app_order_id),  # ✅ Standardized key
                "app_order_id": app_order_id,
                "uid": uid,
                "fill_price": expected_price,
                "expected_price": expected_price,
                "exchange_segment": exchange_segment,
                "exchange_name": exchange_name
            }

        except Exception as e:
            logger.error(f"❌ Order placement failed: {e}")
            raise

    def _get_instrument_tick_size(self, token: int, exchange_segment: str) -> float:
        """
        Helper to get instrument tick size.
        MODIFIED: Hardcoded to 0.05 as per user request to remove the failing API call.
        """
        # The API call to get_master was failing with an UnboundLocalError.
        # As requested, the API call has been removed. We now use a hardcoded
        # fallback value which is the most common tick size for options.
        fallback_tick = 0.05
        return fallback_tick

    async def modify_and_chase_order(self, pending_order_data: Dict, attempt_number: int = 0, trade_uid: str = None) -> Dict:
        """
        Modifies a pending limit order with a new, more aggressive price.
        """
        app_order_id = pending_order_data.get('AppOrderID')
        original_uid = pending_order_data.get('OrderUniqueIdentifier')
        token = pending_order_data.get('ExchangeInstrumentID')
        order_side = pending_order_data.get('OrderSide')
        exchange_segment = pending_order_data.get('ExchangeSegment')

        if not all([app_order_id, original_uid, token, order_side, exchange_segment]):
            msg = f"Missing required data for order modification: {app_order_id}"
            logger.error(msg)
            return {'success': False, 'error': msg}

        try:
            loop = asyncio.get_event_loop()

            # --- Get original buffers from trade config ---
            buy_buffer = 2.0 # Default price buffer if trade config is not found
            sell_buffer = 2.0 # Default price buffer
            if trade_uid and hasattr(state, 'db'):
                trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
                if trade and 'config' in trade:
                    # Use buffers from the trade's configuration
                    buy_buffer = float(trade['config'].get('buy_buffer', buy_buffer))
                    sell_buffer = float(trade['config'].get('sell_buffer', sell_buffer))
            # ---

            # 1. Get new market depth
            # get_market_depth is now async, so await it directly
            depth = await get_market_depth(token, exchange_segment)

            if not (depth and depth.get('bid_price', 0) > 0 and depth.get('ask_price', 0) > 0):
                # --- MODIFICATION: If depth fails, cancel the order instead of failing the chase ---
                logger.warning(f"⚠️ Could not get market depth for token {token}. Attempting to CANCEL order {app_order_id}.")
                
                cancel_func = functools.partial(
                    self.xt_i.cancel_order,
                    appOrderID=app_order_id,
                    orderUniqueIdentifier=original_uid,
                    clientID=self.client_id  # ✅ FIX: Add missing clientID
                )
                cancel_response = await loop.run_in_executor(self.executor, cancel_func)

                if cancel_response and cancel_response.get('type') == 'success':
                    logger.info(f"✅ Order {app_order_id} cancelled successfully due to failed depth fetch.")
                    return {'success': False, 'status': 'CANCELLED', 'order_id': app_order_id, 'error': 'Cancelled due to failed market depth fetch.'}
                else:
                    error_msg = cancel_response.get('description', 'Cancellation failed')
                    logger.error(f"❌ Failed to cancel order {app_order_id} after depth fetch failed: {error_msg}")
                    return {'success': False, 'status': 'CANCEL_FAILED', 'order_id': app_order_id, 'error': error_msg}
                # --- END MODIFICATION ---
            # 2. Calculate new aggressive price
            # ✅ REFACTOR: Use aggressive, escalating buffer for chasing
            tick_size = self._get_instrument_tick_size(token, exchange_segment)

            # The initial buffer used during placement is typically 2 ticks.
            # We use that as the base for our escalation.
            base_buffer_ticks = 2

            # Escalation: Chase 1 (attempt_number=0) -> 2*base, Chase 2 -> 3*base, etc.
            escalation_factor = attempt_number + 2
            chase_buffer_ticks = base_buffer_ticks * escalation_factor

            logger.info(f"🔥 Chasing pending order {app_order_id} (Attempt: {attempt_number + 1}, Buffer Ticks: {chase_buffer_ticks})")

            if order_side == self.xt_i.TRANSACTION_TYPE_BUY:
                new_limit_price = depth['ask_price'] + (chase_buffer_ticks * tick_size)
            else: # SELL
                new_limit_price = depth['bid_price'] - (chase_buffer_ticks * tick_size)
            
            if new_limit_price <= 0:
                msg = f"Calculated chase price is not positive ({new_limit_price:.2f}). Aborting modification."
                logger.error(f"❌ {msg}")
                return {'success': False, 'error': msg}
            
            # Round to nearest valid tick
            rounded_price = round(new_limit_price / tick_size) * tick_size
            final_price = round(rounded_price, 2)

            # 3. Prepare modification parameters
            mod_params = {
                'appOrderID': app_order_id, 'modifiedProductType': pending_order_data.get('ProductType'),
                'modifiedOrderType': self.xt_i.ORDER_TYPE_LIMIT, 'modifiedOrderQuantity': pending_order_data.get('OrderQuantity'),
                'modifiedDisclosedQuantity': pending_order_data.get('OrderDisclosedQuantity'), 
                'modifiedLimitPrice': final_price,  # ✅ Use final rounded price
                'modifiedStopPrice': pending_order_data.get('OrderStopPrice'), 'modifiedTimeInForce': pending_order_data.get('TimeInForce'),
                'orderUniqueIdentifier': original_uid, 'clientID': self.client_id
            }

            # 4. Execute modification in a thread
            mod_func = functools.partial(self.xt_i.modify_order, **mod_params)
            response = await loop.run_in_executor(self.executor, mod_func)

            if response and response.get('type') == 'success':
                logger.info(f"✅ Order {app_order_id} modified successfully with new price ₹{final_price:.2f}.")
                return {'success': True, 'order_id': app_order_id, 'response': response}
            else:
                error_msg = response.get('description', 'Modification failed')
                logger.error(f"❌ Failed to modify order {app_order_id}: {error_msg}")
                return {'success': False, 'order_id': app_order_id, 'error': error_msg}

        except Exception as e:
            logger.error(f"❌ Exception while chasing order {app_order_id}: {e}", exc_info=True)
            return {'success': False, 'order_id': app_order_id, 'error': str(e)}

    async def execute_batch(
        self,
        orders: List[Dict],
        batch_name: str = "BATCH",
        progress_interval: int = 5,
        max_retries: int = 2,
        retry_delay: float = 3.0
    ) -> Dict:
        """
        Execute batch of orders with retries.
        Verification should be done separately.
        """
        execution_start = time.time()
        
        logger.info("="*100)
        logger.info(f"⚡ {batch_name} | Total: {len(orders)} orders | Max Retries: {max_retries}")
        logger.info("="*100)
        
        all_successful_orders = []
        final_failed_orders = []
        orders_to_attempt = list(orders)

        for attempt in range(max_retries + 1):
            if not orders_to_attempt:
                break

            current_batch_name = f"{batch_name} (Attempt {attempt + 1}/{max_retries + 1})"
            if attempt > 0:
                logger.info(f"⏳ Waiting {retry_delay}s before retrying {len(orders_to_attempt)} orders...")
                await asyncio.sleep(retry_delay)
                logger.info(f"🔄 Retrying for {current_batch_name}")

            # --- NEW: Bulk Price Calculation Step ---
            loop = asyncio.get_event_loop()
            orders_needing_price = [o for o in orders_to_attempt if o.get('limit_price', 0.0) <= 0.0]
            if orders_needing_price:
                logger.info(f"🔄 Calculating limit prices for {len(orders_needing_price)} orders in batch '{current_batch_name}'...")
                
                # --- FIX: Ensure exchangeSegment is an integer for get_quote API ---
                instruments_to_fetch_for_depth = [] # Renamed to avoid conflict with outer scope
                for o in orders_needing_price:
                    segment = o.get('exchange_segment')
                    if isinstance(segment, str):
                        # Convert string like "BSEFO" to integer 12
                        segment = REVERSE_EXCHANGE_SEGMENT_MAP.get(segment.upper(), 2)
                    elif segment is None:
                        segment = 2 # Default to NSEFO
                    
                    instruments_to_fetch_for_depth.append({
                        'exchangeSegment': segment, 
                        'exchangeInstrumentID': int(o['token'])
                    })
                # --- END FIX ---
                
                # Fetch all depths in one call
                depth_map = await get_bulk_market_depth(instruments_to_fetch_for_depth)

                # Update orders with calculated prices
                for order in orders_needing_price:
                    calc_price = 0.0
                    tick_size = self._get_instrument_tick_size(order['token'], order.get('exchange_segment'))
                    buffer = order.get('limit_order_buffer', 2.0)
                    action = order.get('action') or order.get('order_side')

                    depth = depth_map.get(int(order['token']))
                    if depth and depth.get('bid_price', 0) > 0 and depth.get('ask_price', 0) > 0:
                        # --- PRIMARY METHOD: Use live bid/ask ---
                        bid_price = depth['bid_price']
                        ask_price = depth['ask_price']

                        if action.upper() == "BUY":
                            calc_price = ask_price + buffer
                        else: # SELL
                            calc_price = bid_price - buffer
                        logger.info(f"✅ Calculated LIMIT price for {order['uid']} from depth. Side: {action}, Bid: {bid_price:.2f}, Ask: {ask_price:.2f}, Buffer: {buffer}, CalcPrice: {calc_price:.2f}")
                    else:
                        # --- FALLBACK METHOD: Use expected_price if depth fetch fails ---
                        logger.warning(f"⚠️ Depth fetch failed for token {order['token']}. Falling back to expected_price for order {order['uid']}.")
                        expected_price = order.get('expected_price', 0.0)
                        if expected_price > 0:
                            # We don't have bid/ask, so we apply the buffer directly to the expected price.
                            if action.upper() == "BUY":
                                calc_price = expected_price + buffer
                            else: # SELL
                                calc_price = expected_price - buffer
                            logger.info(f"✅ Calculated LIMIT price for {order['uid']} from fallback. Expected: {expected_price:.2f}, Buffer: {buffer}, CalcPrice: {calc_price:.2f}")
                        else:
                            logger.error(f"❌ Failed to get depth and no valid expected_price for token {order['token']} for order {order['uid']}. It will fail.")
                            calc_price = 0.0

                    if calc_price > 0:
                        # Round to nearest valid tick
                        rounded_price = round(calc_price / tick_size) * tick_size if tick_size > 0 else calc_price
                        order['limit_price'] = round(rounded_price, 2)
                        logger.info(f"   -> Final price for {order['uid']}: {order['limit_price']:.2f}")
                    else:
                        logger.error(f"❌ Calculated limit price for {order['uid']} is not positive ({calc_price:.2f}). It will fail.")
                        order['limit_price'] = 0 # Ensure it fails

            # --- END NEW ---

            tasks = []
            for order in orders_to_attempt:
                # --- ROBUSTNESS FIX: Use 'order_side' as a fallback for 'action' ---
                action = order.get('action') or order.get('order_side')
                if not action:
                    error_msg = "Order failed pre-flight: Missing 'action' or 'order_side' key."
                    logger.error(f"❌ {error_msg} Order: {order}")
                    temp_failed.append({"success": False, "error": error_msg, **order})
                    continue

                task = self.place_single_order(
                    token=order['token'],
                    option_type=order.get('option_type', 'UNKNOWN'),
                    action=action,
                    quantity=order['quantity'],
                    uid=order['uid'],
                    limit_price=order.get('limit_price', 0.0),
                    exchange_segment=order.get('exchange_segment'),
                    product_type=order.get('product_type'),
                    expected_price=order.get('expected_price', 0.0), # ✅ FIX: Pass expected_price
                    stop_price=order.get('stop_price', 0.0)
                )
                tasks.append(task)

            completed_this_attempt = 0
            total_this_attempt = len(orders_to_attempt)
            temp_successful = []
            temp_failed = []

            for coro in asyncio.as_completed(tasks):
                result = await coro
                completed_this_attempt += 1

                if result.get("success"):
                    temp_successful.append(result)
                else:
                    temp_failed.append(result)
                    logger.error(f"❌ [{current_batch_name}] {result.get('option_type')} {result.get('action')}: {result.get('error')}")

                if completed_this_attempt % progress_interval == 0 or completed_this_attempt == total_this_attempt:
                    logger.info(f"⚡ [{current_batch_name}] {completed_this_attempt}/{total_this_attempt} | ✅ {len(temp_successful)} | ❌ {len(temp_failed)}")

            all_successful_orders.extend(temp_successful)

            if temp_failed and attempt < max_retries:
                failed_uids = {f['uid'] for f in temp_failed}
                orders_to_attempt = [o for o in orders_to_attempt if o['uid'] in failed_uids]
            else:
                final_failed_orders = temp_failed
                orders_to_attempt = []
        
        execution_time = time.time() - execution_start
        
        # Calculate averages
        stats = {}
        for result in all_successful_orders:
            exchange_name = result.get('exchange_name', 'NSE')
            key = f"{exchange_name}_{result['action']}_{result['option_type']}"
            
            if key not in stats:
                stats[key] = {'count': 0, 'quantity': 0, 'fill_prices': []}
            stats[key]['count'] += 1
            stats[key]['quantity'] += result['quantity']
            stats[key]['fill_prices'].append(result['fill_price'])

        for key, data in stats.items():
            data['avg_fill_price'] = sum(data['fill_prices']) / len(data['fill_prices']) if data['fill_prices'] else 0.0
        
        logger.info("="*100)
        logger.info(f"✅ {batch_name} FINAL EXECUTION COMPLETE | Time: {execution_time:.2f}s")
        logger.info(f"✅ Success: {len(all_successful_orders)}/{len(orders)} | ❌ Failed: {len(final_failed_orders)}")
        for key, data in stats.items():
            logger.info(f"   {key}: {data['quantity']} @ ₹{data['avg_fill_price']:.2f}")
        logger.info("="*100)
        
        return {
            'success': len(final_failed_orders) == 0,
            'total_orders': len(orders),
            'successful_count': len(all_successful_orders),
            'failed_count': len(final_failed_orders),
            'successful_orders': all_successful_orders,
            'failed_orders': final_failed_orders,
            'stats': stats,
            'execution_time': execution_time,
            'batch_name': batch_name
        }
    
    async def verify_orders_bulk(self, order_ids: List[str], batch_name: str = "VERIFY") -> Dict:
        """
        ✅ BULK VERIFICATION - Fetch order book ONCE and verify all orders
        """
        verified_success = []
        verified_failed = []
        
        try:
            if not self.xt_i:
                logger.error("❌ Cannot verify - XTS connection not available")
                return {'verified_success': [], 'verified_failed': []}

            # ✅ ADDED: Wait for 1.0s before starting verification to allow orders to settle
            logger.info(f"🔍 Waiting 1.0s for {len(order_ids)} orders to settle...")
            await asyncio.sleep(1.0)

            logger.info(f"📊 Verifying {len(order_ids)} orders...")
            # ✅ FETCH ORDER BOOK ONCE (with clientID)
            order_book = None
            try:
                # Try fetching from Order Book Service first
                async with httpx.AsyncClient(timeout=2.0) as client:
                    resp = await client.get("http://localhost:8002/orderbook")
                    if resp.status_code == 200:
                        data = resp.json()
                        service_order_book = data.get("order_book", [])
                        # If service returns empty but we expect orders, treat as suspicious and fallback
                        if not service_order_book and order_ids:
                            logger.warning("⚠️ Order Book Service returned empty list. Falling back to direct fetch.")
                            order_book = None
                        else:
                            order_book = service_order_book
                    else:
                        raise Exception("Service returned non-200")
            except Exception as e:
                logger.warning(f"⚠️ Order Book Service unavailable ({e}), falling back to direct fetch.")
                order_book = None

            if order_book is None:
                # Fallback to direct fetch
                loop = asyncio.get_event_loop()
                order_book_func = functools.partial(self.xt_i.get_order_book, clientID=self.client_id) if self.client_id else self.xt_i.get_order_book
                order_book = await loop.run_in_executor(self.executor, order_book_func)

            if not order_book:
                logger.warning("⚠️  Empty order book response")
                return {'verified_success': [], 'verified_failed': []}

            # Handle different response formats
            if isinstance(order_book, dict):
                if order_book.get('type') == 'success':
                    result = order_book.get('result', {})
                    if isinstance(result, dict):
                        order_list = result.get('orderList', []) or result.get('OrderList', [])
                    elif isinstance(result, list):
                        order_list = result
                    else:
                        order_list = []
                else:
                    error_msg = order_book.get('description', 'Unknown error')
                    logger.error(f"❌ Order book fetch failed: {error_msg}")
                    return {'verified_success': [], 'verified_failed': []}
            elif isinstance(order_book, list):
                order_list = order_book
            else:
                logger.error(f"❌ Unexpected order book format: {type(order_book)}")
                return {'verified_success': [], 'verified_failed': []}

            logger.info(f"📦 Order book fetched: {len(order_list)} total orders")
            # ✅ CREATE ORDER MAP FOR O(1) LOOKUP
            order_map = {}
            for order in order_list:
                order_id = (
                    order.get('AppOrderID') or
                    order.get('appOrderID') or
                    order.get('OrderID') or
                    order.get('orderID')
                )
                if order_id:
                    order_map[str(order_id)] = order

            # ✅ VERIFY ALL ORDERS IN MEMORY
            pending_orders = []
            for idx, order_id in enumerate(order_ids, 1):
                order_id_str = str(order_id)

                if order_id_str not in order_map:
                    logger.warning(f"⚠️  [{idx}/{len(order_ids)}] Order not found: {order_id}")
                    verified_failed.append({
                        'order_id': order_id,
                        'status': 'NOT_FOUND',
                        'reason': 'Order not in order book'
                    })
                    continue

                broker_order = order_map[order_id_str]

                # ✅ FIX: Convert status to uppercase for comparison
                status = str(broker_order.get('OrderStatus', 'UNKNOWN')).upper()
                order_side = broker_order.get('OrderSide', 'UNKNOWN')
                filled_qty = int(broker_order.get('CumulativeQuantity', 0) or 0)
                order_qty = int(broker_order.get('OrderQuantity', 0) or 0)
                avg_price = float(broker_order.get('OrderAverageTradedPrice', 0) or 0)
                exchange_segment = broker_order.get('ExchangeSegment', 0)
                trading_symbol = broker_order.get('TradingSymbol', '')

                # Convert exchange segment
                if isinstance(exchange_segment, str):
                    exchange_name = {
                        "NSEFO": "NSE", "BSEFO": "BSE",
                        "NSECM": "NSECM", "BSECM": "BSECM"
                    }.get(exchange_segment, exchange_segment)
                else:
                    exchange_name = {
                        2: "NSE", 12: "BSE", 1: "NSECM", 11: "BSECM"
                    }.get(exchange_segment, f"SEG{exchange_segment}")

                # Determine option type
                if 'CE' in trading_symbol:
                    option_type = 'CE'
                    order_type = f'CE {order_side}'
                elif 'PE' in trading_symbol:
                    option_type = 'PE'
                    order_type = f'PE {order_side}'
                else:
                    option_type = 'UNKNOWN'
                    order_type = order_side

                # ✅ FIX: Check uppercase status
                if status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']:
                    logger.info(f"✅ [{idx}/{len(order_ids)}] [{exchange_name}] {order_type} OrderID={order_id} @ ₹{avg_price:.2f}")
                    verified_success.append({
                        'order_id': order_id,
                        'status': status,
                        'option_type': option_type,
                        'action': order_side,
                        'order_type': order_type,
                        'order_side': order_side,
                        'filled_qty': filled_qty,
                        'quantity': filled_qty,
                        'order_qty': order_qty,
                        'avg_price': avg_price,
                        'fill_price': avg_price,
                        'exchange_segment': exchange_segment,
                        'exchange_name': exchange_name,
                        'trading_symbol': trading_symbol,
                        # --- FIX: Add the rest of the broker order data for consistency ---
                        **broker_order
                    })
                elif status in ['REJECTED', 'CANCELLED', 'CANCELED']:
                    reason = broker_order.get('OrderRejectionReason', 'Unknown')
                    logger.error(f"❌ [{idx}/{len(order_ids)}] {status} [{exchange_name}] {order_type} OrderID={order_id} - {reason}")
                    verified_failed.append({
                        'order_id': order_id,
                        'status': status,
                        'order_type': order_type,
                        'reason': reason,
                        'exchange_segment': exchange_segment
                    })
                else:
                    # PENDING - track for retry
                    logger.warning(f"⏳ [{idx}/{len(order_ids)}] PENDING [{exchange_name}] {order_type} OrderID={order_id} Status={status}")
                    pending_orders.append(broker_order)

            # ✅ RETRY PENDING ORDERS (Single retry with 1s delay)
            if pending_orders:
                # This internal retry is less critical now that the builder handles retries.
                # Re-adding a short sleep to give the broker time to process PENDINGNEW orders.
                logger.info(f"⏳ Re-checking status for {len(pending_orders)} pending orders...")
                await asyncio.sleep(1.0) # Give broker a moment to update status from PENDINGNEW
                try:
                    # --- FIX: Run synchronous get_order_book in an executor to avoid blocking ---
                    if self.client_id:
                        order_book_retry_func = functools.partial(self.xt_i.get_order_book, clientID=self.client_id)
                    else:
                        order_book_retry_func = self.xt_i.get_order_book
                    order_book_retry = await loop.run_in_executor(self.executor, order_book_retry_func)
                    # --- END FIX ---

                    # Parse retry response
                    if isinstance(order_book_retry, dict):
                        if order_book_retry.get('type') == 'success':
                            result = order_book_retry.get('result', {})
                            if isinstance(result, dict):
                                order_list_retry = result.get('orderList', []) or result.get('OrderList', [])
                            elif isinstance(result, list):
                                order_list_retry = result
                            else:
                                order_list_retry = []
                        else:
                            order_list_retry = []
                    elif isinstance(order_book_retry, list):
                        order_list_retry = order_book_retry
                    else:
                        order_list_retry = []

                    order_map_retry = {
                        str(o.get('AppOrderID') or o.get('OrderID')): o
                        for o in order_list_retry
                        if o.get('AppOrderID') or o.get('OrderID')
                    }

                    still_pending_after_retry = []
                    for pending_broker_order in pending_orders:
                        order_id = pending_broker_order.get('AppOrderID')
                        order_id_str = str(order_id)

                        if order_id_str in order_map_retry:
                            broker_order = order_map_retry[order_id_str]
                            # ✅ FIX: Convert to uppercase
                            status = str(broker_order.get('OrderStatus', 'UNKNOWN')).upper()

                            # ✅ FIX: Check uppercase status
                            if status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']:
                                # ✅✅ FIX: Convert to float and int (THIS WAS MISSING!)
                                avg_price = float(broker_order.get('OrderAverageTradedPrice', 0) or 0)
                                filled_qty = int(broker_order.get('CumulativeQuantity', 0) or 0)
                                order_qty = int(broker_order.get('OrderQuantity', 0) or 0)

                                logger.info(f"✅ RETRY SUCCESS [{pending_broker_order.get('TradingSymbol')}] OrderID={order_id} @ ₹{avg_price:.2f}")
                                # --- BUG FIX: Append standardized dict, not raw broker_order ---
                                trading_symbol = broker_order.get('TradingSymbol', '')
                                order_side = broker_order.get('OrderSide', 'UNKNOWN')
                                exchange_segment = broker_order.get('ExchangeSegment')
                                if 'CE' in trading_symbol:
                                    option_type = 'CE'
                                    order_type = f'CE {order_side}'
                                elif 'PE' in trading_symbol:
                                    option_type = 'PE'
                                    order_type = f'PE {order_side}'
                                else:
                                    option_type = 'UNKNOWN'
                                    order_type = order_side
                                verified_success.append({
                                    'order_id': order_id, 'status': status, 'option_type': option_type,
                                    'action': order_side, 'order_type': order_type, 'order_side': order_side,
                                    'filled_qty': filled_qty, 'quantity': filled_qty, 'order_qty': order_qty,
                                    'avg_price': avg_price, 'fill_price': avg_price,
                                    'exchange_segment': exchange_segment,
                                    'exchange_name': {2: "NSE", 12: "BSE"}.get(exchange_segment, "UNK"),
                                    'trading_symbol': trading_symbol,
                                    # Add other fields from the raw order for consistency
                                    **broker_order
                                })
                                # --- END BUG FIX ---
                            # ✅ FIX: Handle terminal states in retry to prevent loops
                            elif status in ['REJECTED', 'CANCELLED', 'CANCELED']:
                                reason = broker_order.get('OrderRejectionReason', 'Terminal status on retry')
                                logger.error(f"❌ RETRY FAILED [{pending_broker_order.get('TradingSymbol')}] OrderID={order_id} Status={status} - {reason} (SYNC)")
                                verified_failed.append({
                                    'order_id': order_id,
                                    'status': status,
                                    'reason': reason
                                })
                            else:
                                # --- REFACTORED: Handle pending statuses with specific strategies ---
                                CANCEL_AND_RETRY_STATUSES = ['OPEN', 'NEW', 'REPLACED']  # PENDINGNEW moved to WAIT
                                MODIFY_STATUSES = ['PARTIALLYFILLED']
                                WAIT_STATUSES = ['PENDINGCANCEL', 'PENDINGREPLACE', 'PENDINGNEW']

                                if status in CANCEL_AND_RETRY_STATUSES:
                                    logger.warning(f"⏳ Order {order_id} has status {status}. Attempting to cancel for re-execution.")
                                    original_uid = broker_order.get('OrderUniqueIdentifier')
                                    if not original_uid:
                                        logger.error(f"❌ Cannot cancel order {order_id}, OrderUniqueIdentifier is missing.")
                                        verified_failed.append({'order_id': order_id, 'status': 'CANCEL_FAILED', 'reason': 'Missing OrderUniqueIdentifier'})
                                    else:
                                        cancel_func = functools.partial(
                                            self.xt_i.cancel_order,
                                            appOrderID=order_id,
                                            orderUniqueIdentifier=original_uid,
                                            clientID=self.client_id
                                        )
                                        cancel_response = await loop.run_in_executor(self.executor, cancel_func)
                                        if cancel_response and cancel_response.get('type') == 'success':
                                            logger.info(f"✅ Order {order_id} cancelled successfully (status was {status}). The calling strategy should handle re-execution.")
                                            # Mark as failed with CANCELLED status so the builder/roller can retry it.
                                            verified_failed.append({'order_id': order_id, 'status': 'REEXECUTE_NEEDED', 'reason': f'Cancelled due to {status} status. Needs re-execution.'}) # Custom status for re-execution
                                        else:
                                            error_msg = cancel_response.get('description', 'Cancellation failed')
                                            # If cancellation failed because the order is already gone (filled/rejected),
                                            # that's not a terminal failure for this verification cycle. We just let it
                                            # be re-checked in the next verification attempt.
                                            if "not found in OpenOrder List" in error_msg:
                                                logger.warning(f"⚠️  Could not cancel order {order_id} (status {status}) as it was not found. It may have already been processed. Will re-verify.")
                                                # By not adding to verified_failed, it remains in the unverified pool for the next attempt.
                                            else:
                                                # For other cancellation errors, it's a real failure.
                                                logger.error(f"❌ Failed to cancel order {order_id} (status {status}): {error_msg}")
                                                verified_failed.append({'order_id': order_id, 'status': 'CANCEL_FAILED', 'reason': error_msg})

                                elif status in MODIFY_STATUSES:
                                    logger.warning(f"⏳ Order {order_id} has status {status}. Attempting to chase/modify.")
                                    
                                    # Parse attempt number from batch name to control buffer escalation
                                    import re
                                    attempt_number = 0
                                    match = re.search(r'_ATTEMPT(\d+)', batch_name)
                                    if match:
                                        attempt_number = int(match.group(1)) - 1
                                    
                                    mod_result = await self.modify_and_chase_order(broker_order, attempt_number=attempt_number)

                                    if not mod_result.get('success'):
                                        verified_failed.append({
                                            'order_id': order_id,
                                            'status': mod_result.get('status') or 'MODIFY_FAILED',
                                            'reason': mod_result.get('error')
                                        })
                                elif status in WAIT_STATUSES:
                                    logger.info(f"⏳ Order {order_id} has status {status}. Waiting for broker confirmation. Will re-verify.")
                                    verified_failed.append({'order_id': order_id, 'status': status, 'reason': f'Awaiting broker confirmation for status: {status}'})
                                else:
                                    logger.warning(f"⏳ Order {order_id} has an unhandled pending status: {status}. It will be re-verified in the next cycle.")
                                    verified_failed.append({'order_id': order_id, 'status': status, 'reason': f'Unhandled pending status: {status}'})
                        else:
                            # If not found in the retry book, it's a failure for this verification cycle.
                            logger.error(f"❌ RETRY FAILED OrderID={order_id} not found in order book.")
                            verified_failed.append({'order_id': order_id, 'status': 'NOT_FOUND_ON_RETRY', 'reason': 'Order not in order book on retry'})

                except Exception as retry_error:
                    logger.error(f"❌ Retry fetch failed: {retry_error}")
                    # If the retry itself fails, mark all pending as failed for this cycle.
                    for pending_broker_order in pending_orders:
                        verified_failed.append({'order_id': pending_broker_order.get('AppOrderID'), 'status': 'RETRY_FAILED', 'reason': str(retry_error)})

            logger.info(f"✅ Bulk verification complete: {len(verified_success)} success, {len(verified_failed)} failed")

        except Exception as e:
            logger.error(f"❌ Bulk verification error: {e}")
            import traceback
            logger.error(traceback.format_exc())

        return {
            'verified_success': verified_success,
            'verified_failed': verified_failed
        }

# Global executor instance
global_executor: Optional[OrderExecutor] = None


def _initialize_worker_executor():
    """Auto-initialize executor for worker processes using shared token."""
    try:
        # Use absolute path to ensure worker finds the same DB as main process
        db_path = os.path.abspath("shared_tokens.db")
        if not os.path.exists(db_path):
            logger.error(f"❌ Worker Init: Shared token DB not found at {db_path}")
            return False

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM tokens WHERE key = 'xts_interactive_token'")
        result = cursor.fetchone()
        conn.close()
        
        if result:
            token_data = json.loads(result[0])
            token = token_data.get('token')
            user_id = token_data.get('userID')
            is_investor = token_data.get('isInvestorClient')
            
            if token and user_id:
                logger.info(f"🔄 Auto-initializing OrderExecutor for worker process (User: {user_id})")
                
                # Initialize XTS
                xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
                # Manually set session
                xt._set_common_variables(token, user_id, is_investor)
                xt.isInvestorClient = False # Force Pro
                
                # Initialize Executor
                set_order_executor(xt, max_concurrent=20, client_id=user_id)
                return True
            else:
                logger.error("❌ Worker Init: Token data incomplete in DB")
        else:
            logger.error(f"❌ Worker Init: Key 'xts_interactive_token' not found in {db_path}")
    except Exception as e:
        logger.error(f"❌ Failed to auto-initialize worker executor: {e}", exc_info=True)
    return False

def set_order_executor(xt_interactive, max_concurrent: int = 20, client_id: str = None):
    """Set global order executor instance"""
    global global_executor
    global_executor = OrderExecutor(xt_interactive, max_concurrent, client_id)
    logger.info(f"✅ Global OrderExecutor set with max_concurrent={max_concurrent}")


def get_order_executor() -> Optional[OrderExecutor]:
    """Get global order executor instance"""
    global global_executor
    if global_executor is None:
        _initialize_worker_executor()
    return global_executor
