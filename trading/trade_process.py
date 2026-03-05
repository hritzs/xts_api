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
from market_data.data_client import initialize_market_data_client, close_market_data_client, sync_prices_from_service_loop
from background.tasks import create_snapshot_for_trade
from trading.trade_manager import TradeManager

def setup_process_logging(trade_uid: str):
    """Sets up a file handler for the specific trade process."""
    try:
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        
        log_file = os.path.join(log_dir, f"trade_{trade_uid}.log")
        handler = logging.FileHandler(log_file, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    except Exception as e:
        # Fallback to console/default logger if file setup fails
        logger.error(f"Failed to setup process logging for {trade_uid}: {e}")

async def trade_process_worker(trade_uid: str, trade_config: Dict, command_q: multiprocessing.Queue, snapshot_q: multiprocessing.Queue, initial_option_chains: Dict, initial_orders: List[Dict] = None):
    """The main worker function for a single trade process."""
    # 1. Setup logging for this process, directing output to a trade-specific file
    setup_process_logging(trade_uid)
    logger.info(f"🚀 Process for trade {trade_uid} started.")

    # 2. Initialize process-specific state
    loop = asyncio.get_event_loop()
    state.db = Database()
    state.option_chains = initial_option_chains
    
    # --- NEW: Initialize temp order cache with orders passed from builder ---
    # This ensures that snapshot creation has access to orders even if DB replication is lagging.
    state.temp_order_cache = {}
    if initial_orders:
        state.temp_order_cache[trade_uid] = initial_orders
        logger.info(f"Initialized worker process cache with {len(initial_orders)} orders for {trade_uid}.")
    
    # Initialize the market data client for this process to fetch prices
    await initialize_market_data_client()
    
    # Start a background task to keep prices in sync for this process
    price_sync_task = asyncio.create_task(sync_prices_from_service_loop())

    # 3. Create and start the TradeManager for this specific trade
    manager = TradeManager(trade_uid, trade_config)
    await manager.start_monitoring()

    # 4. Main loop
    snapshot_interval = 0.5  # seconds
    last_snapshot_time = 0

    try:
        while True:
            # Check for commands from the main process
            if not command_q.empty():
                command_data = command_q.get()
                command = command_data.get('command')
                logger.info(f"Received command: {command} for {trade_uid}")

                if command == 'SQUARE_OFF':
                    from trading.square_off import square_off_by_trade_uid
                    await square_off_by_trade_uid(trade_uid)
                    break # After square-off, the trade is done. Break the loop.
                elif command == 'STOP':
                    logger.info(f"Received STOP command for {trade_uid}. Shutting down.")
                    break
                elif command == 'UPDATE_CONFIG':
                    new_config = command_data.get('config')
                    if new_config:
                        logger.info(f"Updating config for {trade_uid} and restarting monitors.")
                        await manager.update_config_and_restart(new_config)

            # Create and send snapshot periodically
            now = time.time()
            if now - last_snapshot_time > snapshot_interval:
                await create_snapshot_for_trade(trade_uid, log_level='DEBUG')
                snapshot = state.trade_snapshots.get(trade_uid)
                if snapshot:
                    try:
                        snapshot_q.put_nowait(snapshot)
                    except multiprocessing.queues.Full:
                        logger.warning(f"Snapshot queue for {trade_uid} is full. Skipping snapshot.")
                last_snapshot_time = now

            # Check if the trade is closed
            trade_data = await loop.run_in_executor(None, state.db.get_straddle_by_id, trade_uid)
            trade_status = trade_data.get('status') if trade_data else None
            
            if trade_status and trade_status.startswith('CLOSED'):
                logger.info(f"Trade {trade_uid} is closed (status: {trade_status}). Shutting down process.")
                break

            await asyncio.sleep(0.1)  # Main loop sleep

    except Exception as e:
        logger.error(f"❌ Unhandled exception in trade process for {trade_uid}: {e}", exc_info=True)
    finally:
        logger.info(f"🛑 Shutting down trade process for {trade_uid}.")
        price_sync_task.cancel()
        await manager.stop_monitoring()
        await close_market_data_client()
        if state.db:
            state.db.close()
        logger.info(f"✅ Process for {trade_uid} finished.")


def trade_process_worker_entry(trade_uid: str, trade_config: Dict, command_q: multiprocessing.Queue, snapshot_q: multiprocessing.Queue, initial_option_chains: Dict, initial_orders: List[Dict] = None):
    """Synchronous entry point for the multiprocessing.Process."""
    asyncio.run(trade_process_worker(trade_uid, trade_config, command_q, snapshot_q, initial_option_chains, initial_orders))