"""
market_data/tasks.py — Background tasks for the Market Data Microservice

Contains:
  - Option chain cache updater
  - REST polling fallback
  - Greeks calculator
  - XTS socket status monitor
  - Snapshot builder + loop (create_trade_snapshots_loop)
  - Verification helpers
  - WebSocket broadcast helpers
  - Cleanup / misc loops
"""
import asyncio
import math
import httpx
from typing import Set, List, Dict, Optional
from datetime import datetime
from dataclasses import dataclass

from utils.logger import logger
from models.state import state
from utils.greeks import calculate_all_greeks, calculate_greeks_from_iv
from utils.helpers import calculate_dte, get_ist_now

from trading.chain_provider import (
    get_option_chain as build_get_option_chain,
    SYMBOL_CONFIG,
    get_bulk_ltp,
    get_ltp
)

_websocket_clients: Set = set()


# ============================================================================
# DATACLASSES
# ============================================================================

@dataclass
class PositionGreeks:
    token: int
    strike: int
    option_type: str
    quantity: int       # signed: +ve BUY, -ve SELL
    ltp: float
    delta: float
    gamma: float
    theta: float
    vega: float
    iv: float           # always stored as % (e.g. 25.36)

    @property
    def net_delta(self) -> float:
        return self.delta * self.quantity

    @property
    def net_gamma(self) -> float:
        return self.gamma * self.quantity

    @property
    def net_theta(self) -> float:
        return self.theta * self.quantity

    @property
    def net_vega(self) -> float:
        return self.vega * self.quantity


@dataclass
class TradeGreeks:
    trade_uid: str
    positions: List[PositionGreeks]

    @property
    def total_delta(self) -> float:
        return sum(p.net_delta for p in self.positions)

    @property
    def total_gamma(self) -> float:
        return sum(p.net_gamma for p in self.positions)

    @property
    def total_theta(self) -> float:
        return sum(p.net_theta for p in self.positions)

    @property
    def total_vega(self) -> float:
        return sum(p.net_vega for p in self.positions)

    @property
    def avg_iv(self) -> float:
        if not self.positions:
            return 0.0
        return sum(p.iv for p in self.positions) / len(self.positions)


# ============================================================================
# WEBSOCKET HELPERS
# ============================================================================

def set_websocket_clients(clients: Set):
    global _websocket_clients
    _websocket_clients = clients


async def broadcast_message(message: dict):
    if not _websocket_clients:
        return
    disconnected = set()
    for client in list(_websocket_clients):
        try:
            await asyncio.wait_for(client.send_json(message), timeout=2.0)
        except Exception as e:
            logger.warning(f"WS broadcast failed: {type(e).__name__} - {e}")
            disconnected.add(client)
    for client in disconnected:
        _websocket_clients.discard(client)


async def broadcast_log(level: str, message: str):
    await broadcast_message({
        'type': 'log_message',
        'data': {
            'level': level.upper(),
            'message': message,
            'timestamp': get_ist_now().isoformat()
        }
    })


async def websocket_keepalive_loop():
    logger.info("💓 WebSocket keep-alive loop started")
    try:
        while True:
            await asyncio.sleep(30)
            await broadcast_message({"type": "ping", "timestamp": get_ist_now().timestamp()})
    except asyncio.CancelledError:
        logger.info("💓 WebSocket keep-alive loop stopped")


# ============================================================================
# MARKET DATA BACKGROUND LOOPS
# ============================================================================

async def update_option_chain_cache_loop(interval_seconds: float = 10.0):
    """
    Periodically rebuilds option chains for all watched symbols and stores them
    in state.option_chains (shared memory proxy or local dict).
    """
    logger.info(f"🔗 Option chain cache loop started (interval={interval_seconds}s)")
    try:
        while True:
            all_successful = True
            try:
                symbols_to_update = list(SYMBOL_CONFIG.keys())
                for symbol in symbols_to_update:
                    try:
                        loop = asyncio.get_event_loop()
                        chain = await loop.run_in_executor(None, build_get_option_chain, symbol)
                        if chain:
                            state.option_chains[symbol] = chain
                            logger.debug(f"   🔄 Chain refreshed for {symbol}")
                        else:
                            all_successful = False
                    except Exception as e:
                        logger.warning(f"   ⚠️ Chain refresh failed for {symbol}: {e}")
                        all_successful = False
            except Exception as e:
                logger.error(f"❌ Option chain cache loop error: {e}", exc_info=True)
                all_successful = False

            sleep_time = interval_seconds if all_successful else 1.0
            await asyncio.sleep(sleep_time)
    except asyncio.CancelledError:
        logger.info("🔗 Option chain cache loop stopped")


async def rest_polling_loop(interval_seconds: float = 1.0):
    """
    Fallback price polling via REST when the XTS socket is disconnected.
    Only fires when state.data_source == 'REST_POLL'.

    ✅ FIX: After updating PriceSHM, also queues each tick into
    state.market_data_queue so process_and_broadcast_market_data_queue
    picks them up and broadcasts price_update + chain_header_update to
    the ZMQ PUB socket → UI clients.
    """
    logger.info("🔄 REST polling fallback loop started")
    loop = asyncio.get_event_loop()
    try:
        while True:
            try:
                if getattr(state, 'data_source', 'WEBSOCKET') == 'REST_POLL':
                    subscribed_tokens = list(getattr(state, 'subscribed_tokens', set()))
                    if subscribed_tokens:
                        # ✅ get_bulk_ltp is synchronous — run in executor to avoid blocking
                        prices = await loop.run_in_executor(
                            None, get_bulk_ltp, subscribed_tokens
                        )
                        if prices:
                            for token, ltp in prices.items():
                                token_int = int(token)
                                ltp_float = float(ltp)

                                # ✅ FIX: Use state.price_shm (correct SHM in marketdata_service)
                                # NOT state.shared_data (that's the main app's SHM)
                                if hasattr(state, 'price_shm') and state.price_shm:
                                    state.price_shm.update(token_int, ltp_float)

                                # ✅ FIX: Queue tick so broadcast manager publishes it via ZMQ PUB
                                if hasattr(state, 'market_data_queue') and state.market_data_queue:
                                    try:
                                        state.market_data_queue.put_nowait({
                                            'ExchangeInstrumentID': token_int,
                                            'ltp': ltp_float
                                        })
                                    except asyncio.QueueFull:
                                        pass  # Queue backed up — REST poll will catch it next cycle

                            logger.debug(f"🔄 REST poll: queued {len(prices)} ticks")
            except Exception as e:
                logger.warning(f"⚠️ REST polling error: {e}")
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        logger.info("🔄 REST polling fallback loop stopped")


async def calculate_greeks_loop(interval_seconds: float = 1.0):
    """
    Periodically recalculates Greeks for all strikes in the cached chains
    and writes them back into state.option_chains so snapshots and the
    UI always have fresh Greek values.
    """
    logger.info(f"🧮 Greeks calculator loop started (interval={interval_seconds}s)")
    try:
        while True:
            try:
                for symbol, chain in list(state.option_chains.items()):
                    if not isinstance(chain, dict):
                        continue

                    # Update fut_ltp with the latest price from the state cache
                    fut_token = chain.get('fut_token')
                    if fut_token:
                        live_fut_price = state.get_price(int(fut_token))
                        if live_fut_price and live_fut_price > 0:
                            chain['fut_ltp'] = live_fut_price

                    fut_ltp = chain.get('fut_ltp', 0)
                    dte     = chain.get('dte', 0)
                    if fut_ltp <= 0 or dte < 0:
                        continue

                    # Calculate Synthetic Future Price
                    synthetic_spot = fut_ltp
                    try:
                        atm_strike = chain.get('atm')
                        gap = chain.get('gap', 50)
                        if atm_strike and gap > 0:
                            atm_row = next(
                                (r for r in chain.get('chain', []) if r.get('strike') == atm_strike),
                                None
                            )
                            if atm_row:
                                ce_tok = int(atm_row.get('ce_token') or 0)
                                pe_tok = int(atm_row.get('pe_token') or 0)
                                if ce_tok > 0 and pe_tok > 0:
                                    ce_p = state.get_price(ce_tok) or atm_row.get('ce_ltp', 0)
                                    pe_p = state.get_price(pe_tok) or atm_row.get('pe_ltp', 0)
                                    if ce_p > 0 and pe_p > 0:
                                        calculated_synthetic = atm_strike + ce_p - pe_p
                                        if abs(calculated_synthetic - atm_strike) <= (gap * 2):
                                            synthetic_spot = calculated_synthetic
                                        else:
                                            logger.debug(
                                                f"[{symbol}] Synthetic spot {calculated_synthetic:.2f} "
                                                f"too far from ATM {atm_strike}. Using cash {fut_ltp:.2f}."
                                            )
                        chain['synthetic_spot'] = synthetic_spot
                    except Exception as syn_e:
                        logger.warning(f"Error calculating synthetic spot for {symbol}: {syn_e}")
                        chain['synthetic_spot'] = fut_ltp

                    rows = chain.get('chain', [])
                    for row in rows:
                        strike = row.get('strike', 0)
                        for side, otype, tok_key, iv_key in [
                            ('ce', 'call', 'ce_token', 'ce_iv'),
                            ('pe', 'put', 'pe_token', 'pe_iv'),
                        ]:
                            tok = row.get(f'{side}_token')
                            if not tok:
                                continue
                            ltp = (
                                state.get_price(int(tok))
                                or row.get(f'{side}_ltp', 0)
                            )
                            if ltp and ltp > 0:
                                greeks = calculate_all_greeks(
                                    otype, strike, synthetic_spot, dte, ltp, 0.0
                                )
                                row[f'{side}_delta'] = greeks.get('delta', 0.0)
                                row[f'{side}_gamma'] = greeks.get('gamma', 0.0)
                                row[f'{side}_theta'] = greeks.get('theta', 0.0)
                                row[f'{side}_vega']  = greeks.get('vega', 0.0)
                                raw_iv = greeks.get('iv', 0)
                                if raw_iv > 0:
                                    row[iv_key] = _normalise_iv(raw_iv)
                            row[f'{side}_ltp'] = ltp or row.get(f'{side}_ltp', 0)
            except Exception as e:
                logger.error(f"❌ Greeks loop error: {e}", exc_info=True)
            await asyncio.sleep(interval_seconds)
    except asyncio.CancelledError:
        logger.info("🧮 Greeks calculator loop stopped")


async def monitor_xts_socket_status():
    """Broadcasts XTS socket connected/disconnected events to the main app."""
    logger.info("🚦 XTS Socket Status Monitor started")
    last_status = None
    last_data_source = None
    while True:
        try:
            await asyncio.sleep(2)
            current_status = state.socket_connected
            current_data_source = getattr(state, 'data_source', 'UNKNOWN')
            if current_status != last_status or current_data_source != last_data_source:
                await broadcast_message({
                    'type': 'xts_socket_status',
                    'data': {
                        'connected':  current_status,
                        'dataSource': current_data_source,
                    }
                })
                last_status = current_status
                last_data_source = current_data_source
        except asyncio.CancelledError:
            logger.info("🚦 XTS Socket Status Monitor stopped")
            break
        except Exception as e:
            logger.error(f"Socket monitor error: {e}")


# ============================================================================
# VERIFICATION
# ============================================================================

async def verify_orders_task(order_ids: List[str], batch_name: str = "BATCH") -> Dict:
    try:
        if not order_ids:
            return {'verified_success': [], 'verified_failed': []}
        from trading.order_executor import get_order_executor
        executor = get_order_executor()
        if not executor:
            logger.error(f"❌ OrderExecutor not available for {batch_name}")
            return {'verified_success': [], 'verified_failed': []}

        await broadcast_log('INFO', f"[{batch_name}] Verifying {len(order_ids)} orders...")
        result = await executor.verify_orders_bulk(order_ids)

        verified_success = result.get('verified_success', [])
        verified_failed  = result.get('verified_failed', [])

        if not verified_success and not verified_failed and order_ids:
            verified_failed = [
                {'order_id': oid, 'status': 'VERIFICATION_PENDING',
                 'reason': 'Empty order book response'}
                for oid in order_ids
            ]

        log_msg = (
            f"[{batch_name}] Verification Complete: "
            f"{len(verified_success)} OK, {len(verified_failed)} Failed."
        )
        logger.info("=" * 100)
        logger.info(f"✅ [{batch_name}] VERIFICATION COMPLETE — "
                    f"{len(verified_success)}/{len(order_ids)} verified")
        logger.info("=" * 100)
        await broadcast_log('SUCCESS' if not verified_failed else 'WARNING', log_msg)

        if verified_success or verified_failed:
            if not hasattr(state, 'verification_results'):
                state.verification_results = {}
            state.verification_results[batch_name] = {
                'batch_name': batch_name,
                'timestamp':  get_ist_now().isoformat(),
                'verified':   verified_success,
                'failed':     verified_failed,
                'total':      len(order_ids)
            }
            await broadcast_message({
                'type': 'verification_complete',
                'data': {
                    'batch_name':     batch_name,
                    'verified_count': len(verified_success),
                    'failed_count':   len(verified_failed),
                    'total':          len(order_ids)
                },
                'timestamp': get_ist_now().isoformat()
            })
        return result

    except Exception as e:
        logger.error(f"❌ Verification task failed for {batch_name}: {e}")
        import traceback; logger.error(traceback.format_exc())
        return {'verified_success': [], 'verified_failed': []}


def start_verification_task(
    order_ids: List[str], batch_name: str = "BATCH"
) -> Optional[asyncio.Task]:
    try:
        loop = asyncio.get_event_loop()
        task = loop.create_task(verify_orders_task(order_ids, batch_name))
        if not hasattr(state, 'verification_tasks'):
            state.verification_tasks = {}
        state.verification_tasks[batch_name] = task

        def on_complete(future):
            try:
                result = future.result()
                v = len(result.get('verified_success', []))
                f = len(result.get('verified_failed', []))
                logger.info(f"✅ Verification done for {batch_name}: {v} ok, {f} failed.")
                if hasattr(state, 'verification_tasks') and batch_name in state.verification_tasks:
                    del state.verification_tasks[batch_name]
            except Exception as e:
                logger.error(f"❌ Verification callback error for {batch_name}: {e}")

        task.add_done_callback(on_complete)
        return task
    except Exception as e:
        logger.error(f"❌ Failed to start verification task: {e}")
        return None


def get_verification_status(batch_name: str) -> Optional[Dict]:
    if hasattr(state, 'verification_results'):
        return state.verification_results.get(batch_name)
    return None


def is_verification_running(batch_name: str) -> bool:
    if hasattr(state, 'verification_tasks'):
        task = state.verification_tasks.get(batch_name)
        return task is not None and not task.done()
    return False


async def wait_for_verification(
    batch_name: str, timeout: float = 30.0
) -> Optional[Dict]:
    try:
        if not hasattr(state, 'verification_tasks'):
            return None
        task = state.verification_tasks.get(batch_name)
        if not task:
            return get_verification_status(batch_name)
        return await asyncio.wait_for(task, timeout=timeout)
    except asyncio.TimeoutError:
        logger.warning(f"⚠️ Verification timeout for {batch_name}")
        return None
    except Exception as e:
        logger.error(f"❌ Error waiting for verification: {e}")
        return None


# ============================================================================
# MISC BACKGROUND LOOPS
# ============================================================================

def get_live_pnl_data() -> dict:
    try:
        if not state.db:
            return {}
        straddles = state.db.get_active_straddles()
        if not straddles:
            return {'total_pnl': 0.0, 'realized_pnl': 0.0,
                    'unrealized_pnl': 0.0, 'active_trades': 0}
        from trading.pnl_calculator import calculate_aggregate_pnl
        return calculate_aggregate_pnl(straddles, state.prices)
    except Exception as e:
        logger.error(f"❌ Live PnL calculation error: {e}")
        return {'total_pnl': 0.0, 'realized_pnl': 0.0,
                'unrealized_pnl': 0.0, 'active_trades': 0}


async def update_order_book_loop():
    logger.info("📋 Order book sync monitor started")
    try:
        while True:
            try:
                await asyncio.sleep(30)
                async with httpx.AsyncClient() as client:
                    await client.get("http://localhost:8002/orderbook", timeout=1.0)
            except asyncio.CancelledError:
                break
            except Exception:
                pass
    except asyncio.CancelledError:
        pass


async def cleanup_old_data():
    logger.info("🧹 Cleanup task started (runs every 6h)")
    try:
        while True:
            try:
                await asyncio.sleep(21600)
                if state.db:
                    state.db.cleanup_old_orders(days=30)
                    logger.info("✅ Database cleanup complete")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"❌ Cleanup error: {e}")
                await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pass


# ============================================================================
# IV NORMALISATION
# ============================================================================

def _normalise_iv(iv_value: float) -> float:
    """
    calculate_all_greeks() returns IV as decimal (e.g. 0.2536).
    Converts to % (25.36) exactly once.
    Values already >= 2.0 are assumed to be in % form already.
    """
    if iv_value <= 0:
        return 0.0
    return round(iv_value * 100.0, 4) if iv_value < 2.0 else round(iv_value, 4)


# ============================================================================
# CORE SNAPSHOT BUILDER
# ============================================================================

async def _create_snapshot_for_trade(
    trade: dict,
    spot_details_map: dict,
    trade_data_override: dict = None,
    log_level: str = 'DEBUG'
):
    """Build a full snapshot for one trade and store in state.trade_snapshots."""
    trade_uid = trade.get('trade_uid')
    if not trade_uid:
        return

    try:
        loop = asyncio.get_event_loop()

        # ── 1. Resolve full trade data ────────────────────────────────────────
        db_trade_data = await loop.run_in_executor(
            None, state.db.get_straddle_by_id, trade_uid
        )

        if trade_data_override:
            full_trade_data = trade_data_override
            if hasattr(state, 'trade_data_cache'):
                state.trade_data_cache[trade_uid] = {
                    'data':      trade_data_override,
                    'timestamp': datetime.now().timestamp()
                }
        else:
            full_trade_data = db_trade_data
            if hasattr(state, 'trade_data_cache'):
                cached_entry = state.trade_data_cache.get(trade_uid)
                if cached_entry and full_trade_data:
                    cached_trade = cached_entry.get('data')
                    if cached_trade:
                        cached_pnl = cached_trade.get('realized_pnl', 0.0)
                        db_pnl     = full_trade_data.get('realized_pnl', 0.0)
                        if cached_pnl != 0 and db_pnl == 0:
                            logger.warning(
                                f"⚠️ Restoring realized_pnl={cached_pnl} from cache for {trade_uid}"
                            )
                            full_trade_data['realized_pnl'] = cached_pnl
                            if cached_trade.get('psqf_percentage'):
                                full_trade_data['psqf_percentage'] = cached_trade['psqf_percentage']
                            loop.run_in_executor(
                                None, state.db.insert_straddle, full_trade_data.copy()
                            )

        if not full_trade_data:
            full_trade_data = trade
        if 'straddle_id' not in full_trade_data:
            full_trade_data['straddle_id'] = trade_uid

        # ── 2. Resolve symbol / segment / chain ───────────────────────────────
        symbol_upper = full_trade_data.get('symbol', 'NIFTY').upper()
        base_symbol  = next(
            (k for k in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True)
             if k in symbol_upper), None
        )
        option_chain          = spot_details_map.get(symbol_upper)
        derived_segment       = SYMBOL_CONFIG.get(base_symbol, {}).get('segment') if base_symbol else None
        correct_exchange_segment = derived_segment or full_trade_data.get('exchange_segment', 2)

        # ── 3. Resolve lot_size ───────────────────────────────────────────────
        lot_size = full_trade_data.get('lot_size')
        if not lot_size or lot_size <= 0:
            if option_chain and option_chain.get('lot_size'):
                lot_size = option_chain['lot_size']
                loop.run_in_executor(
                    None,
                    lambda: (
                        state.db.update_straddle_field(trade_uid, 'lot_size', lot_size)
                        if hasattr(state.db, 'update_straddle_field') else None
                    )
                )
                logger.info(f"💾 Derived & persisted lot_size={lot_size} for {trade_uid}")
            else:
                lot_size = 65
                logger.error(f"Cannot determine lot_size for {trade_uid}. Using fallback {lot_size}.")

        # ── 4. Guard: chain must be ready ─────────────────────────────────────
        if not option_chain or not option_chain.get('fut_ltp'):
            logger.warning(f"📸 Snapshot aborted for {trade_uid}: chain not ready.")
            return

        # ── 5. Live synthetic spot ────────────────────────────────────────────
        cash_spot       = option_chain.get('fut_ltp', 0.0)
        live_spot_price = option_chain.get('synthetic_spot', cash_spot)

        # ── 6. Position & PnL reconstruction ─────────────────────────────────
        total_pnl             = 0.0
        total_realized_pnl    = 0.0
        total_unrealized_pnl  = 0.0
        live_positions:         List[dict]       = []
        positions_for_greeks:   List[dict]       = []
        trade_orders:           List[dict]       = []
        pnl_and_position_details: Dict[int, dict] = {}
        trade_greeks: Optional[TradeGreeks]      = None
        price_map:    Dict[int, float]           = {}

        try:
            all_db_orders = await loop.run_in_executor(
                None, state.db.get_orders_by_trade_id, trade_uid
            )
            trade_orders = [
                o for o in all_db_orders
                if str(
                    o.get('order_status', '') or o.get('OrderStatus', '')
                ).upper() in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']
            ]

            filtered = []
            for order in trade_orders:
                ouid = (
                    order.get('OrderUniqueIdentifier')
                    or order.get('order_unique_id')
                )
                if ouid and trade_uid in ouid:
                    filtered.append(order)
                elif not ouid:
                    logger.warning(
                        f"Skipping order {order.get('AppOrderID')} — no OrderUniqueIdentifier"
                    )
            trade_orders = filtered

            # Merge temp cache
            if hasattr(state, 'temp_order_cache') and state.temp_order_cache:
                cached_fills = state.temp_order_cache.get(trade_uid, [])
                if cached_fills:
                    db_ids = {
                        str(
                            o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')
                        )
                        for o in trade_orders
                    }
                    added = 0
                    for fill in cached_fills:
                        fid = str(
                            fill.get('AppOrderID') or fill.get('app_order_id') or fill.get('apporderid')
                        )
                        if fid and fid not in db_ids:
                            trade_orders.append(fill)
                            added += 1
                    if added:
                        logger.info(f"Merged {added} orders from temp cache for {trade_uid}")
                    db_all_ids = {
                        str(
                            o.get('AppOrderID') or o.get('app_order_id') or o.get('apporderid')
                        )
                        for o in all_db_orders
                    }
                    state.temp_order_cache[trade_uid] = [
                        f for f in cached_fills
                        if str(
                            f.get('AppOrderID') or f.get('app_order_id') or f.get('apporderid')
                        ) not in db_all_ids
                    ]

            logger.debug(f"📋 {len(trade_orders)} filled orders for {trade_uid}")

            if trade_orders:
                all_tokens: Set[int] = set()
                agg: Dict[int, dict] = {}

                for order in trade_orders:
                    tv = (
                        order.get('exchange_instrument_id')
                        or order.get('ExchangeInstrumentID')
                    )
                    if not tv:
                        continue
                    token = int(tv)
                    qty   = int(
                        order.get('cumulative_quantity')
                        or order.get('CumulativeQuantity', 0)
                    )
                    price = float(
                        order.get('order_avg_price')
                        or order.get('OrderAverageTradedPrice', 0)
                    )
                    side  = str(
                        order.get('order_side')
                        or order.get('OrderSide', '')
                    ).upper()
                    all_tokens.add(token)
                    if token not in agg:
                        agg[token] = {
                            'buy_qty': 0, 'buy_value': 0.0,
                            'sell_qty': 0, 'sell_value': 0.0
                        }
                    if side == 'BUY':
                        agg[token]['buy_qty']   += qty
                        agg[token]['buy_value'] += qty * price
                    elif side == 'SELL':
                        agg[token]['sell_qty']   += qty
                        agg[token]['sell_value'] += qty * price

                if all_tokens:
                    price_map = await loop.run_in_executor(
                        None, get_bulk_ltp, list(all_tokens), correct_exchange_segment
                    )
                    missing = [t for t in all_tokens if price_map.get(t, 0) <= 0]
                    if missing:
                        logger.warning(f"LTP missing for tokens {missing}")

                # PnL pool
                total_pnl_pool = 0.0
                for token in all_tokens:
                    a       = agg[token]
                    net_qty = a['sell_qty'] - a['buy_qty']
                    ltp     = price_map.get(token, 0.0)
                    mtm     = net_qty * ltp if ltp > 0 else 0.0
                    total_pnl_pool += (a['sell_value'] - a['buy_value']) - mtm

                total_realized_pnl   = full_trade_data.get('realized_pnl', 0.0)
                total_unrealized_pnl = total_pnl_pool - total_realized_pnl

                # Build live_positions
                for token in all_tokens:
                    a            = agg[token]
                    net_open_qty = a['sell_qty'] - a['buy_qty']
                    ltp          = price_map.get(token, 0.0)
                    entry_price  = 0.0
                    leg_mtm      = 0.0

                    if net_open_qty != 0:
                        if net_open_qty > 0:
                            entry_price = (
                                a['sell_value'] / a['sell_qty']
                                if a['sell_qty'] > 0 else 0.0
                            )
                            if ltp > 0:
                                leg_mtm = (entry_price - ltp) * net_open_qty
                        else:
                            entry_price = (
                                a['buy_value'] / a['buy_qty']
                                if a['buy_qty'] > 0 else 0.0
                            )
                            if ltp > 0:
                                leg_mtm = (ltp - entry_price) * abs(net_open_qty)

                    strike, option_type = None, None
                    if option_chain and option_chain.get('chain'):
                        for row in option_chain['chain']:
                            if row.get('ce_token') == token:
                                strike, option_type = row['strike'], 'CE'
                                break
                            if row.get('pe_token') == token:
                                strike, option_type = row['strike'], 'PE'
                                break

                    pnl_and_position_details[token] = {
                        'net_open_qty': net_open_qty,
                        'entry_price':  entry_price,
                        'ltp':          ltp,
                        'leg_mtm_pnl':  leg_mtm,
                        'strike':       strike,
                        'option_type':  option_type,
                        'token_pnl': (
                            (a['sell_value'] - a['buy_value'])
                            - (net_open_qty * ltp)
                        )
                    }

                    if net_open_qty != 0 and strike is not None:
                        action = 'SELL' if net_open_qty > 0 else 'BUY'
                        positions_for_greeks.append({
                            'token': token, 'strike': strike,
                            'option_type': option_type,
                            'quantity': abs(net_open_qty),
                            'action':  'BUY' if net_open_qty < 0 else 'SELL',
                            'symbol':  symbol_upper
                        })
                        live_positions.append({
                            'token':       token,
                            'strike':      strike,
                            'option_type': option_type,
                            'quantity':    abs(net_open_qty),
                            'action':      action,
                            'entry_price': entry_price,
                            'ltp':         ltp,
                            'pnl':         leg_mtm,
                            'iv': 0.0, 'delta': 0.0,
                            'gamma': 0.0, 'theta': 0.0, 'vega': 0.0
                        })

                # Greeks
                chain_rows = option_chain.get('chain', [])
                iv_lookup: Dict[int, float] = {}
                for row in chain_rows:
                    if row.get('ce_token'):
                        iv_lookup[int(row['ce_token'])] = float(row.get('ce_iv') or 0)
                    if row.get('pe_token'):
                        iv_lookup[int(row['pe_token'])] = float(row.get('pe_iv') or 0)

                dte     = option_chain.get('dte', 0)
                pg_list: List[PositionGreeks] = []

                for pos in positions_for_greeks:
                    token         = int(pos['token'])
                    cached_iv_pct = iv_lookup.get(token, 0.0)
                    live_ltp      = (
                        price_map.get(token, 0.0)
                        or state.get_price(token)
                        or 0.0
                    )
                    greeks: dict = {}

                    if live_ltp > 0:
                        greeks = calculate_all_greeks(
                            pos['option_type'].lower(),
                            pos['strike'], live_spot_price, dte, live_ltp, 0.0
                        )

                    raw_iv = greeks.get('iv', 0)
                    if raw_iv > 0:
                        greeks['iv'] = _normalise_iv(raw_iv)
                    elif cached_iv_pct > 0:
                        greeks = calculate_greeks_from_iv(
                            pos['option_type'].lower(), pos['strike'],
                            live_spot_price, dte, cached_iv_pct / 100.0, 0.0
                        )
                        greeks['iv'] = cached_iv_pct
                    else:
                        greeks = {
                            'delta': 0.0, 'gamma': 0.0,
                            'theta': 0.0, 'vega': 0.0, 'iv': 0.0
                        }

                    signed_qty = (
                        pos['quantity'] if pos['action'] == 'BUY'
                        else -pos['quantity']
                    )
                    pg = PositionGreeks(
                        token=token, strike=pos['strike'],
                        option_type=pos['option_type'],
                        quantity=signed_qty, ltp=live_ltp,
                        delta=greeks.get('delta', 0.0),
                        gamma=greeks.get('gamma', 0.0),
                        theta=greeks.get('theta', 0.0),
                        vega=greeks.get('vega', 0.0),
                        iv=greeks.get('iv', 0.0)
                    )
                    pg_list.append(pg)

                    for lp in live_positions:
                        if lp['token'] == token:
                            lp['iv']    = greeks.get('iv', 0.0)
                            lp['delta'] = pg.net_delta
                            lp['gamma'] = pg.net_gamma
                            lp['theta'] = pg.net_theta
                            lp['vega']  = pg.net_vega

                trade_greeks = TradeGreeks(trade_uid=trade_uid, positions=pg_list)

            else:
                logger.warning(f"No filled orders for {trade_uid}. Will use fallback.")

        except Exception as e:
            logger.error(
                f"Position/PnL reconstruction error for {trade_uid}: {e}",
                exc_info=True
            )

        total_pnl = total_realized_pnl + total_unrealized_pnl

        # ── 7. Fallback path (no orders in DB) ────────────────────────────────
        if not trade_orders:
            logger.warning(f"Using fallback (no orders) for {trade_uid}")
            ce_token = (
                int(full_trade_data["ce_token"])
                if full_trade_data.get("ce_token") else None
            )
            pe_token = (
                int(full_trade_data["pe_token"])
                if full_trade_data.get("pe_token") else None
            )
            chain_rows    = option_chain.get('chain', [])
            greeks_lookup: Dict = {}
            for row in chain_rows:
                for side_key, otype in [('ce_token', 'CE'), ('pe_token', 'PE')]:
                    tok = row.get(side_key)
                    if tok:
                        sp = side_key[:2]
                        greeks_lookup[tok] = {
                            'iv':    float(row.get(f'{sp}_iv')    or 0),
                            'delta': float(row.get(f'{sp}_delta') or 0),
                            'gamma': float(row.get(f'{sp}_gamma') or 0),
                            'theta': float(row.get(f'{sp}_theta') or 0),
                            'vega':  float(row.get(f'{sp}_vega')  or 0),
                        }

            for key, otype, qty_field, entry_field in [
                (ce_token, 'CE', 'ce_quantity', 'ce_entry_price'),
                (pe_token, 'PE', 'pe_quantity', 'pe_entry_price'),
            ]:
                if not key:
                    continue
                qty    = full_trade_data.get(qty_field, 0)
                entry  = full_trade_data.get(entry_field, 0)
                ltp_fb = state.get_price(key) or 0.0
                pnl_fb = (
                    (entry - ltp_fb) * qty
                    if entry > 0 and ltp_fb > 0 else 0.0
                )
                total_pnl            += pnl_fb
                total_unrealized_pnl += pnl_fb

                g_data     = greeks_lookup.get(key, {})
                signed_qty = -qty
                pg = PositionGreeks(
                    token=key, strike=full_trade_data['strike'],
                    option_type=otype, quantity=signed_qty, ltp=ltp_fb,
                    delta=g_data.get('delta', 0.0),
                    gamma=g_data.get('gamma', 0.0),
                    theta=g_data.get('theta', 0.0),
                    vega=g_data.get('vega', 0.0),
                    iv=g_data.get('iv', 0.0)
                )
                positions_for_greeks.append({
                    'token': key, 'strike': full_trade_data['strike'],
                    'option_type': otype, 'quantity': qty,
                    'action': 'SELL', 'symbol': symbol_upper
                })
                live_positions.append({
                    'token': key, 'strike': full_trade_data['strike'],
                    'option_type': otype, 'quantity': qty, 'action': 'SELL',
                    'entry_price': entry, 'ltp': ltp_fb, 'pnl': pnl_fb,
                    'iv':    g_data.get('iv', 0.0),
                    'delta': pg.net_delta,
                    'gamma': pg.net_gamma,
                    'theta': pg.net_theta,
                    'vega':  pg.net_vega,
                })
                if trade_greeks is None:
                    trade_greeks = TradeGreeks(trade_uid=trade_uid, positions=[pg])
                else:
                    trade_greeks.positions.append(pg)

        elif trade_greeks is None:
            chain_rows = option_chain.get('chain', [])
            iv_lookup  = {}
            for row in chain_rows:
                if row.get('ce_token'):
                    iv_lookup[int(row['ce_token'])] = float(row.get('ce_iv') or 0)
                if row.get('pe_token'):
                    iv_lookup[int(row['pe_token'])] = float(row.get('pe_iv') or 0)
            dte     = option_chain.get('dte', 0)
            pg_list = []
            for pos in positions_for_greeks:
                token         = int(pos['token'])
                cached_iv_pct = iv_lookup.get(token, 0.0)
                live_ltp      = (
                    price_map.get(token, 0.0)
                    or state.get_price(token) or 0.0
                )
                greeks = {}
                if live_ltp > 0:
                    greeks = calculate_all_greeks(
                        pos['option_type'].lower(),
                        pos['strike'], live_spot_price, dte, live_ltp, 0.0
                    )
                raw_iv = greeks.get('iv', 0)
                if raw_iv > 0:
                    greeks['iv'] = _normalise_iv(raw_iv)
                elif cached_iv_pct > 0:
                    greeks = calculate_greeks_from_iv(
                        pos['option_type'].lower(), pos['strike'],
                        live_spot_price, dte, cached_iv_pct / 100.0, 0.0
                    )
                    greeks['iv'] = cached_iv_pct
                else:
                    greeks = {
                        'delta': 0.0, 'gamma': 0.0,
                        'theta': 0.0, 'vega': 0.0, 'iv': 0.0
                    }
                signed_qty = (
                    pos['quantity'] if pos['action'] == 'BUY'
                    else -pos['quantity']
                )
                pg = PositionGreeks(
                    token=token, strike=pos['strike'],
                    option_type=pos['option_type'],
                    quantity=signed_qty, ltp=live_ltp,
                    delta=greeks.get('delta', 0.0),
                    gamma=greeks.get('gamma', 0.0),
                    theta=greeks.get('theta', 0.0),
                    vega=greeks.get('vega', 0.0),
                    iv=greeks.get('iv', 0.0)
                )
                pg_list.append(pg)
                for lp in live_positions:
                    if lp['token'] == token:
                        lp['iv']    = greeks.get('iv', 0.0)
                        lp['delta'] = pg.net_delta
                        lp['gamma'] = pg.net_gamma
                        lp['theta'] = pg.net_theta
                        lp['vega']  = pg.net_vega
            trade_greeks = TradeGreeks(trade_uid=trade_uid, positions=pg_list)

        if trade_greeks is None:
            trade_greeks = TradeGreeks(trade_uid=trade_uid, positions=[])

        net_delta = trade_greeks.total_delta
        net_gamma = trade_greeks.total_gamma

        # ── 8. Live ATM IV ─────────────────────────────────────────────────────
        avg_iv = live_atm_iv = 0.0
        current_atm_strike  = 0
        try:
            gap = option_chain.get('gap') or 50
            current_atm_strike = int(round(live_spot_price / gap) * gap)
            atm_row = next(
                (r for r in option_chain.get('chain', [])
                 if r.get('strike') == current_atm_strike), None
            )
            if atm_row:
                ce_iv_c = float(atm_row.get('ce_iv') or 0)
                pe_iv_c = float(atm_row.get('pe_iv') or 0)
                valid = [x for x in [ce_iv_c, pe_iv_c] if x > 0]
                if valid:
                    live_atm_iv = sum(valid) / len(valid)
                if live_atm_iv == 0:
                    dte_atm  = option_chain.get('dte', 0)
                    calc_ivs = []
                    for tok_key, otype in [
                        (int(atm_row.get('ce_token') or 0), 'call'),
                        (int(atm_row.get('pe_token') or 0), 'put'),
                    ]:
                        if tok_key:
                            ltp_a = (
                                state.get_price(tok_key)
                                or float(atm_row.get(f'{otype[:2]}_ltp') or 0)
                            )
                            if ltp_a > 0:
                                g = calculate_all_greeks(
                                    otype, current_atm_strike,
                                    live_spot_price, dte_atm, ltp_a, 0.0
                                )
                                iv = g.get('iv', 0)
                                if iv > 0:
                                    calc_ivs.append(_normalise_iv(iv))
                    if calc_ivs:
                        live_atm_iv = sum(calc_ivs) / len(calc_ivs)
        except Exception as e:
            logger.error(f"ATM IV error for {trade_uid}: {e}")

        avg_iv = live_atm_iv if live_atm_iv > 0 else trade_greeks.avg_iv

        # ── 9. Per-leg PnL / Greeks ────────────────────────────────────────────
        ce_token_int = (
            int(full_trade_data['ce_token'])
            if full_trade_data.get('ce_token') else None
        )
        pe_token_int = (
            int(full_trade_data['pe_token'])
            if full_trade_data.get('pe_token') else None
        )
        ce_pnl = pnl_and_position_details.get(ce_token_int, {}).get('token_pnl', 0.0)
        pe_pnl = pnl_and_position_details.get(pe_token_int, {}).get('token_pnl', 0.0)
        ce_iv = ce_delta = pe_iv = pe_delta = 0.0
        for pg in trade_greeks.positions:
            if pg.token == ce_token_int:
                ce_iv, ce_delta = pg.iv, pg.net_delta
            elif pg.token == pe_token_int:
                pe_iv, pe_delta = pg.iv, pg.net_delta

        # ── 10. Hedge / Roll dynamic params ───────────────────────────────────
        pts_out        = (
            abs(net_delta) / abs(net_gamma) if abs(net_gamma) > 1e-6 else 0.0
        )
        config_data    = full_trade_data.get('config') or {}
        points_allowed = float("inf")
        roll_trigger_price = 0.0

        try:
            hedge_div    = config_data.get("hedge_div", 57)
            straddle_div = config_data.get("straddle_div", 4)
            try:
                roll_straddle_div = float(config_data.get('roll_straddle_div', 0.2))
                if roll_straddle_div == 2.0: roll_straddle_div = 0.2
            except (ValueError, TypeError):
                roll_straddle_div = 0.2
            atm_strike = option_chain.get('atm')
            atm_row    = next(
                (r for r in option_chain['chain'] if r['strike'] == atm_strike), None
            )
            if atm_row:
                ce_ltp_atm = (
                    state.get_price(atm_row.get('ce_token'))
                    or atm_row.get('ce_ltp', 0.0)
                )
                pe_ltp_atm = (
                    state.get_price(atm_row.get('pe_token'))
                    or atm_row.get('pe_ltp', 0.0)
                )
                atm_straddle_live = ce_ltp_atm + pe_ltp_atm
                avg_iv_dec        = avg_iv / 100.0
                straddle_based = (
                    (atm_straddle_live / straddle_div)
                    if straddle_div > 0 and atm_straddle_live > 0
                    else float("inf")
                )
                spot_iv_based = (
                    (live_spot_price * avg_iv_dec) / hedge_div
                    if hedge_div > 0 and avg_iv_dec > 0
                    else float("inf")
                )
                points_allowed     = min(straddle_based, spot_iv_based)
                roll_trigger_price = (
                    atm_straddle_live / roll_straddle_div
                    if roll_straddle_div > 0 else 0.0
                )
        except Exception as e:
            logger.warning(f"Dynamic params error for {trade_uid}: {e}")

        # ── 11. PnL per straddle unit ──────────────────────────────────────────
        net_ce = sum(
            p['quantity'] if p['action'] == 'SELL' else -p['quantity']
            for p in live_positions if p['option_type'] == 'CE'
        )
        net_pe = sum(
            p['quantity'] if p['action'] == 'SELL' else -p['quantity']
            for p in live_positions if p['option_type'] == 'PE'
        )
        num_straddle_units = math.ceil((abs(net_ce) + abs(net_pe)) / 2.0)

        if num_straddle_units < 1:
            db_ce = full_trade_data.get('ce_quantity', 0)
            db_pe = full_trade_data.get('pe_quantity', 0)
            num_straddle_units = math.ceil((db_ce + db_pe) / 2.0)

        pnl_per_straddle = (
            total_unrealized_pnl / num_straddle_units
            if num_straddle_units > 0 else 0.0
        )

        # ── 12. SL threshold ───────────────────────────────────────────────────
        sl_bps      = config_data.get('sl_bps', 14)
        sl_points   = (
            (live_spot_price * sl_bps) / 10000
            if live_spot_price > 0 else 0.0
        )
        sl_threshold = -1 * sl_points * num_straddle_units

        days_to_expiry = -1
        try:
            expiry_val = full_trade_data.get('expiry')
            if expiry_val and expiry_val != 'N/A':
                days_to_expiry = calculate_dte(expiry_val)
        except Exception:
            pass

        # ── 13. Assemble snapshot ─────────────────────────────────────────────
        snapshot_data = {
            'timestamp':          get_ist_now().isoformat(),
            'total_pnl':          total_pnl,
            'realized_pnl':       total_realized_pnl,
            'unrealized_pnl':     total_unrealized_pnl,
            'pnl_per_straddle':   pnl_per_straddle,
            'net_delta':          net_delta,
            'net_gamma':          net_gamma,
            'net_theta':          trade_greeks.total_theta,
            'net_vega':           trade_greeks.total_vega,
            'avg_iv':             avg_iv,
            'ce_pnl':             ce_pnl,
            'pe_pnl':             pe_pnl,
            'ce_iv':              ce_iv,
            'pe_iv':              pe_iv,
            'ce_delta':           ce_delta,
            'pe_delta':           pe_delta,
            'pts_out':            pts_out,
            'points_allowed':     points_allowed,
            'roll_trigger_price': roll_trigger_price,
            'sl_threshold':       sl_threshold,
            'sl_points':          sl_points,
            'days_to_expiry':     days_to_expiry,
            'lot_size':           lot_size,
            'spot_price':         cash_spot,
            'synthetic_spot':     live_spot_price,
            'total_contracts': (
                full_trade_data.get('ce_quantity', 0)
                + full_trade_data.get('pe_quantity', 0)
            ),
            'live_positions':     live_positions,
        }

        if not hasattr(state, 'trade_snapshots'):
            state.trade_snapshots = {}
        state.trade_snapshots[trade_uid] = snapshot_data

        # ── 14. Log ───────────────────────────────────────────────────────────
        pos_log = ""
        for p in sorted(live_positions, key=lambda x: x['option_type'], reverse=True):
            pos_log += (
                f"\n   {p['action']} {p['quantity']} {symbol_upper} "
                f"{p['strike']} {p['option_type']}"
                f" | Entry:{p['entry_price']:.2f} LTP:{p['ltp']:.2f}"
                f" PnL:₹{p['pnl']:.2f}"
                f" IV:{p.get('iv', 0):.2f}% Δ:{p.get('delta', 0):.2f}"
            )
        log_msg = (
            f"📸 [{trade_uid}] Spot:{cash_spot:.2f} Syn.Fut:{live_spot_price:.2f} | "
            f"PnL:₹{total_pnl:.2f} "
            f"(R:₹{total_realized_pnl:.2f} U:₹{total_unrealized_pnl:.2f}) | "
            f"PnL/Straddle:₹{pnl_per_straddle:.2f} | "
            f"Δ:{net_delta:.2f} Γ:{net_gamma:.4f} "
            f"Θ:{trade_greeks.total_theta:.2f} V:{trade_greeks.total_vega:.2f} | "
            f"IV:{avg_iv:.2f}% | "
            f"PtsOut:{pts_out:.2f}/{points_allowed:.2f} | "
            f"Roll:₹{roll_trigger_price:.2f} | SL:₹{sl_threshold:.2f} | "
            f"DTE:{days_to_expiry}"
            f"{pos_log}"
        )
        if log_level.upper() == 'INFO':
            logger.info(log_msg)
        else:
            logger.debug(log_msg)

    except Exception as e:
        logger.exception(f"❌ Snapshot failed for {trade_uid}: {e}")


# ============================================================================
# ON-DEMAND SNAPSHOT HELPERS
# ============================================================================

async def create_snapshot_for_trade(
    trade_uid: str,
    trade_data: dict = None,
    log_level: str = 'DEBUG'
):
    """Creates and stores a snapshot for a single trade (on-demand or from loop)."""
    if not hasattr(state, 'trade_snapshots'):
        state.trade_snapshots = {}

    loop  = asyncio.get_event_loop()
    trade = trade_data or await loop.run_in_executor(
        None, state.db.get_straddle_by_id, trade_uid
    )
    if not trade:
        logger.warning(f"Cannot snapshot non-existent trade {trade_uid}")
        return

    symbol = trade.get('symbol', 'NIFTY').upper()
    chain  = (
        state.get_option_chain(symbol)
        if hasattr(state, 'get_option_chain') else None
    )
    is_valid = (
        isinstance(chain, dict)
        and 'fut_ltp' in chain
        and isinstance(chain.get('chain'), list)
        and len(chain['chain']) > 0
    )
    if not is_valid:
        chain = await loop.run_in_executor(None, build_get_option_chain, symbol)

    spot_details_map = {symbol: chain} if chain else {}
    await _create_snapshot_for_trade(
        trade, spot_details_map,
        trade_data_override=trade_data,
        log_level=log_level
    )


async def trigger_snapshot_and_broadcast(
    trade_uid: str,
    trade_data: dict = None,
    log_level: str = 'INFO'
):
    """On-demand: create snapshot then immediately push full update to UI."""
    logger.info(f"⚡ Immediate snapshot requested for {trade_uid}")
    await create_snapshot_for_trade(trade_uid, trade_data, log_level=log_level)
    await _assemble_and_broadcast_snapshot(trade_uid)


async def _assemble_and_broadcast_snapshot(trade_uid: str):
    """Build full straddle_full_update payload and broadcast."""
    try:
        from trading.trade_manager import get_trade_manager
        from trading.event_bus import get_event_bus, EventPriority
    except ImportError:
        get_trade_manager = lambda _: None
        get_event_bus     = lambda: None
        EventPriority     = None

    snapshot = (
        state.trade_snapshots.get(trade_uid)
        if hasattr(state, 'trade_snapshots') else None
    )
    if not snapshot:
        return

    loop  = asyncio.get_event_loop()
    trade = await loop.run_in_executor(
        None, state.db.get_straddle_by_id, trade_uid
    )
    if not trade:
        return

    manager        = get_trade_manager(trade_uid) if callable(get_trade_manager) else None
    event_bus_inst = get_event_bus() if callable(get_event_bus) else None

    pa = snapshot.get('points_allowed')
    payload = {
        'trade_uid':          trade_uid,
        'symbol':             trade.get('symbol'),
        'strike':             trade.get('strike'),
        'status':             trade.get('status'),
        'ce_quantity':        trade.get('ce_quantity', 0),
        'pe_quantity':        trade.get('pe_quantity', 0),
        'live_pnl':           snapshot.get('total_pnl'),
        'realized_pnl':       snapshot.get('realized_pnl', 0.0),
        'unrealized_pnl':     snapshot.get('unrealized_pnl', 0.0),
        'live_net_delta':     snapshot.get('net_delta'),
        'net_gamma':          snapshot.get('net_gamma'),
        'net_theta':          snapshot.get('net_theta'),
        'net_vega':           snapshot.get('net_vega'),
        'avg_iv':             snapshot.get('avg_iv'),
        'pts_out':            snapshot.get('pts_out'),
        'points_allowed':     None if pa == float('inf') else pa,
        'roll_trigger_price': snapshot.get('roll_trigger_price'),
        'sl_threshold':       snapshot.get('sl_threshold'),
        'synthetic_spot':     snapshot.get('synthetic_spot'),
        'spot_price':         snapshot.get('spot_price'),
        'entry_spot':         trade.get('entry_spot'),
        'pnl_per_straddle':   snapshot.get('pnl_per_straddle'),
        'live_positions':     snapshot.get('live_positions', []),
    }

    if manager:
        live_config = trade.get('config', {})
        payload['monitors'] = {
            'sl': {
                'running':    manager.sl_monitor.running,
                'sl_bps':     manager.sl_monitor.sl_bps,
                'sl_points':  manager.sl_monitor.sl_points,
                'interval':   manager.sl_monitor.sl_monitor_interval,
                'start_time': live_config.get('sl_start_time') or 'Trade Start',
            },
            'hedge': {
                'running':      manager.hedge_monitor.running,
                'hedge_div':    manager.hedge_monitor.hedge_div,
                'straddle_div': manager.hedge_monitor.straddle_div,
                'interval':     manager.hedge_monitor.hedge_monitor_interval,
                'start_time':   live_config.get('hedge_start_time') or 'Trade Start',
            },
            'roll': {
                'running':           manager.roll_monitor.running,
                'roll_straddle_div': manager.roll_monitor.roll_straddle_div,
                'interval':          manager.roll_monitor.roll_monitor_interval,
                'start_time':        live_config.get('roll_start_time') or 'Trade Start',
            },
            'square_off': {
                'running':   manager.square_off_monitor.running,
                'exit_time': manager.square_off_monitor.exit_time_str or 'Not Set',
            },
        }

    if event_bus_inst and EventPriority:
        events = event_bus_inst.get_trade_events(trade_uid)
        payload['events'] = [
            {
                'timestamp': e.timestamp.strftime('%H:%M:%S'),
                'type':      e.event_type,
                'priority':  EventPriority(e.priority).name
            }
            for e in reversed(events[-5:])
        ]

    asyncio.create_task(
        broadcast_message({'type': 'straddle_full_update', 'data': payload})
    )


# ============================================================================
# MAIN SNAPSHOT LOOP
# ============================================================================

async def create_trade_snapshots_loop(interval_seconds: float = 0.5):
    """
    Every interval_seconds:
      1. Drain snapshot_q from each live worker process
      2. For active trades WITHOUT a worker process → create snapshot directly
      3. Push lightweight row-data to Snapshot Service (port 8003) or WS fallback
      4. Broadcast full straddle_update (detail panel) for each trade
    """
    import config as _config
    logger.info(f"📸 Snapshotter loop started (interval={interval_seconds}s)")

    if not hasattr(state, 'trade_snapshots'):
        state.trade_snapshots = {}

    snapshot_service_url = (
        f"http://localhost:"
        f"{getattr(_config, 'SNAPSHOT_SERVICE_PORT', 8003)}"
        f"/api/push-snapshots"
    )

    while True:
        try:
            # ── Step 1: Drain worker process queues ───────────────────────────
            trade_processes = getattr(state, 'trade_processes', {})
            for trade_uid in list(trade_processes.keys()):
                pinfo = trade_processes.get(trade_uid)
                if not pinfo or not pinfo['process'].is_alive():
                    logger.warning(f"Worker process for {trade_uid} is dead. Removing.")
                    trade_processes.pop(trade_uid, None)
                    continue
                q      = pinfo.get('snapshot_q')
                if not q:
                    continue
                latest = None
                try:
                    while not q.empty():
                        latest = q.get_nowait()
                except Exception:
                    pass
                if latest is not None:
                    if not hasattr(state, 'trade_snapshots'):
                        state.trade_snapshots = {}
                    state.trade_snapshots[trade_uid] = latest

            # ── Step 2: Direct snapshot for trades with no worker process ─────
            if state.db:
                all_active  = state.db.get_active_straddles() or []
                worker_uids = set(trade_processes.keys())
                for trade in all_active:
                    tid = trade.get('trade_uid')
                    if tid and tid not in worker_uids:
                        try:
                            await create_snapshot_for_trade(tid, log_level='DEBUG')
                        except Exception as e:
                            logger.error(f"Direct snapshot error for {tid}: {e}")

            # ── Step 3 + 4: Build updates and broadcast ───────────────────────
            snapshots = getattr(state, 'trade_snapshots', {})
            if not snapshots:
                await asyncio.sleep(interval_seconds)
                continue

            lightweight_updates = []
            for trade_uid, snap in list(snapshots.items()):
                pa = snap.get('points_allowed')
                lightweight_updates.append({
                    'trade_uid':          trade_uid,
                    'live_pnl':           snap.get('total_pnl'),
                    'unrealized_pnl':     snap.get('unrealized_pnl'),
                    'realized_pnl':       snap.get('realized_pnl', 0.0),
                    'live_net_delta':     snap.get('net_delta'),
                    'net_gamma':          snap.get('net_gamma'),
                    'net_theta':          snap.get('net_theta'),
                    'net_vega':           snap.get('net_vega'),
                    'avg_iv':             snap.get('avg_iv'),
                    'pts_out':            snap.get('pts_out'),
                    'points_allowed':     None if pa == float('inf') else pa,
                    'roll_trigger_price': snap.get('roll_trigger_price'),
                    'sl_threshold':       snap.get('sl_threshold'),
                    'synthetic_spot':     snap.get('synthetic_spot'),
                    'spot_price':         snap.get('spot_price'),
                    'pnl_per_straddle':   snap.get('pnl_per_straddle'),
                    'position_ltps': {
                        str(p['token']): p['ltp']
                        for p in snap.get('live_positions', [])
                    },
                    'position_pnls': {
                        str(p['token']): p['pnl']
                        for p in snap.get('live_positions', [])
                    },
                    'position_ivs': {
                        str(p['token']): p.get('iv', 0.0)
                        for p in snap.get('live_positions', [])
                    },
                    'live_positions': snap.get('live_positions', []),
                })

            pushed = False
            if lightweight_updates:
                try:
                    async with httpx.AsyncClient(timeout=0.2) as client:
                        await client.post(
                            snapshot_service_url,
                            json={'updates': lightweight_updates}
                        )
                    pushed = True
                except Exception:
                    pass

                if not pushed:
                    asyncio.create_task(broadcast_message({
                        'type': 'chain_header_update',
                        'symbol': symbol,
                        'spot':    round(futltp, 2),
                        'syn_fut': round(chain.get('syntheticspot', futltp), 2),
                        'atm':     int(round(chain.get('syntheticspot', futltp) / chain.get('gap', 50)) * chain.get('gap', 50)),
                    }))

                for upd in lightweight_updates:
                    tid  = upd['trade_uid']
                    snap = snapshots.get(tid, {})
                    pa   = snap.get('points_allowed')
                    asyncio.create_task(broadcast_message({
                        'type': 'straddle_update',
                        'data': {
                            'trade_uid':          tid,
                            'live_pnl':           snap.get('total_pnl'),
                            'unrealized_pnl':     snap.get('unrealized_pnl'),
                            'realized_pnl':       snap.get('realized_pnl', 0.0),
                            'live_net_delta':     snap.get('net_delta'),
                            'net_gamma':          snap.get('net_gamma'),
                            'net_theta':          snap.get('net_theta'),
                            'net_vega':           snap.get('net_vega'),
                            'avg_iv':             snap.get('avg_iv'),
                            'pts_out':            snap.get('pts_out'),
                            'points_allowed':     None if pa == float('inf') else pa,
                            'roll_trigger_price': snap.get('roll_trigger_price'),
                            'sl_threshold':       snap.get('sl_threshold'),
                            'synthetic_spot':     snap.get('synthetic_spot'),
                            'spot_price':         snap.get('spot_price'),
                            'pnl_per_straddle':   snap.get('pnl_per_straddle'),
                            'live_positions':     snap.get('live_positions', []),
                        }
                    }))

            await asyncio.sleep(interval_seconds)

        except asyncio.CancelledError:
            logger.info("📸 Snapshotter loop shutting down.")
            break
        except Exception as e:
            logger.exception(f"❌ Snapshotter loop error: {e}")
            await asyncio.sleep(10)
