"""
Trade Process Worker
This module contains the entry point and logic for a dedicated process
that manages the entire lifecycle of a single trade.
"""
import asyncio
import multiprocessing
import time
import logging
import os
from typing import Dict, List

from utils.logger import logger
from models.state import state
import config
from database.db_manager import Database
from market_data.data_client import (
    initialize_market_data_client,
    close_market_data_client,
    sync_prices_from_service_loop,
)
from trading.trade_manager import TradeManager, register_event_handlers


def setup_process_logging(trade_uid: str):
    """Sets up a file handler for the specific trade process."""
    try:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        log_file = os.path.join(log_dir, f"trade_{trade_uid}.log")
        handler = logging.FileHandler(log_file, encoding='utf-8')
        formatter = logging.Formatter(
            '%(asctime)s [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except Exception as e:
        logger.error(f"Failed to setup process logging for {trade_uid}: {e}")


async def trade_process_worker(
    trade_uid: str,
    initial_trade_data: Dict,
    command_q: multiprocessing.Queue,
    initial_option_chains: Dict,
    shared_trade_data_cache: Dict,
    initial_orders: List[Dict] = None,
):
    """The main async worker for a single trade process."""
    setup_process_logging(trade_uid)
    logger.info(f"🚀 Process for trade {trade_uid} started.")

    loop = asyncio.get_event_loop()

    # ── Step 1: Database ──────────────────────────────────────────────────
    state.db = Database()
    state.option_chains = initial_option_chains
    state.trade_data_cache = shared_trade_data_cache if shared_trade_data_cache is not None else {}

    # ── Step 2: Local event bus ───────────────────────────────────────────
    # Must be initialized BEFORE TradeManager so monitor @property event_bus
    # lookups and register_event_handlers() both find a valid instance.
    from trading.event_bus import EventBus, set_event_bus
    local_event_bus = EventBus()
    set_event_bus(local_event_bus)
    event_bus_task = asyncio.create_task(local_event_bus.process_events())
    logger.info(f"✅ Local event bus initialized for {trade_uid}")

    # ── Step 3: Register all action handlers onto the local event bus ─────
    register_event_handlers()

    # ── Step 4: Seed temp order cache ─────────────────────────────────────
    state.temp_order_cache = {}
    if initial_orders:
        state.temp_order_cache[trade_uid] = initial_orders
        logger.info(f"Worker cache seeded with {len(initial_orders)} orders for {trade_uid}.")

    # ── Step 5: Market data + price sync ──────────────────────────────────
    await initialize_market_data_client()
    price_sync_task = asyncio.create_task(sync_prices_from_service_loop())

    # ── Step 6: TradeManager + monitors (using initial data) ───────────────
    manager = TradeManager(trade_uid, initial_trade_data)
    await manager.start_monitoring()

    monitor_check_interval = 1.0
    last_check_time        = 0.0

    try:
        while True:
            # ── Command queue ─────────────────────────────────────────────
            if not command_q.empty():
                command_data = command_q.get()
                command      = command_data.get('command')
                logger.info(f"📨 Command received: {command} for {trade_uid}")

                if command == 'HEDGE':
                    from trading.hedger import execute_synthetic_hedge
                    # The snapshot is kept up-to-date by the run_all_checks loop
                    snapshot = state.trade_snapshots.get(trade_uid)
                    if snapshot:
                        net_delta = snapshot.get('net_delta', 0.0)
                        logger.info(f"Manual HEDGE triggered for {trade_uid} with net_delta: {net_delta}")
                        # Execute a full delta neutralization hedge
                        await execute_synthetic_hedge(trade_uid, net_delta, target_delta_reduction=-net_delta)
                    else:
                        logger.error(f"Cannot execute manual hedge for {trade_uid}: no snapshot available.")

                elif command == 'ROLL':
                    from trading.roller import roll_position
                    logger.info(f"Manual ROLL triggered for {trade_uid}")
                    # The roll_position function handles fetching the latest data and executing the roll.
                    # It expects the status to have been set to 'ROLLING' by the main process.
                    await roll_position(trade_uid)

                elif command == 'SQUARE_OFF':
                    from trading.square_off import square_off_by_trade_uid
                    reason = command_data.get('reason')
                    await square_off_by_trade_uid(trade_uid, reason=reason)
                    break
                elif command == 'STOP':
                    logger.info(f"🛑 STOP command for {trade_uid}. Shutting down.")
                    break
                elif command == 'UPDATE_CONFIG':
                    new_config = command_data.get('data') or command_data.get('config')
                    if new_config:
                        await manager.update_configuration(new_config)
                
                elif command == 'PARTIAL_SQUARE_OFF':
                    from trading.square_off import partial_square_off
                    percentage = command_data.get('percentage')
                    if percentage:
                        logger.info(f"Manual PARTIAL SQUARE OFF ({percentage}%) triggered for {trade_uid}")
                        await partial_square_off(trade_uid, percentage)
                    else:
                        logger.error(f"Cannot execute partial square off for {trade_uid}: missing 'percentage'.")

            # ── Monitor checks (every 1s, each monitor self-gates) ────────
            now = time.time()
            if now - last_check_time >= monitor_check_interval:
                await manager.run_all_checks()
                last_check_time = now

            # ── Auto-exit if trade closed in DB ───────────────────────────
            trade_data   = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            trade_status = trade_data.get('status') if trade_data else None
            if trade_status and trade_status.startswith('CLOSED'):
                logger.info(f"Trade {trade_uid} closed ({trade_status}). Shutting down.")
                break

            await asyncio.sleep(0.1)

    except Exception as e:
        logger.error(f"❌ Unhandled exception in trade process for {trade_uid}: {e}", exc_info=True)
    finally:
        logger.info(f"🛑 Shutting down trade process for {trade_uid}.")
        price_sync_task.cancel()
        event_bus_task.cancel()
        await manager.stop_monitoring()
        await close_market_data_client()
        if state.db:
            state.db.close()
        logger.info(f"✅ Process for {trade_uid} finished.")


def trade_process_worker_entry(
    trade_uid: str,
    initial_trade_data: Dict,
    command_q: multiprocessing.Queue,
    initial_option_chains: Dict,
    shared_trade_data_cache: Dict,
    initial_orders: List[Dict] = None,
):
    """Synchronous entry point for multiprocessing.Process."""
    asyncio.run(
        trade_process_worker(
            trade_uid,
            initial_trade_data,
            command_q,
            initial_option_chains,
            shared_trade_data_cache,
            initial_orders,
        )
    )
