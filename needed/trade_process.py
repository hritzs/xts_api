"""
Worker process for managing a single trade.
Each trade runs in its own isolated process for stability and parallelism.
"""
import asyncio
import multiprocessing
from typing import Dict
from utils.logger import logger
from database.db_manager import Database
from trading.trade_manager import create_trade_manager, get_trade_manager
from background.tasks import _create_snapshot_for_trade, trigger_snapshot_and_broadcast
from trading.square_off import square_off_by_trade_uid, partial_square_off
from trading.builder import manual_sync_trade_orders
from utils.shared_data import SharedDataManager

class TradeProcessState:
    """A simple state object for a single trade process."""
    def __init__(self, option_chains_proxy):
        self.db = None
        self.trade_manager = None
        self.shared_data = SharedDataManager(create=False)
        self.option_chains = option_chains_proxy
        self.trade_snapshots = {}
        self.cancellation_flags = {}
        self.temp_order_cache = {}

    def get_price(self, token: int) -> float:
        """Gets price from the shared data manager."""
        return self.shared_data.get_price(token)

    def get_option_chain(self, symbol: str) -> Dict:
        """Gets option chain from the shared data manager."""
        return self.option_chains.get(symbol)

    def update_option_chain(self, symbol: str, chain: Dict):
        """Updates option chain in the shared data manager."""
        if self.shared_data:
            self.option_chains[symbol] = chain

async def trade_process_worker_async(trade_uid: str, config: Dict, command_q: multiprocessing.Queue, snapshot_q: multiprocessing.Queue, option_chains_proxy):
    """The async main loop for a single trade process."""
    logger.info(f"[{trade_uid}] Worker process started. PID: {multiprocessing.current_process().pid}")
    
    # 1. Initialize state for this process
    state_obj = TradeProcessState(option_chains_proxy)
    state_obj.db = Database()
    
    # 2. Monkey-patch the global state object in modules used by this worker.
    # This is a pragmatic way to adapt existing code without massive refactoring.
    import models.state
    import background.tasks
    import trading.square_off
    import trading.builder
    import trading.hedger
    import trading.pnl_calculator
    import trading.trade_manager
    # Import all monitor modules that will be used by the TradeManager
    from trading.monitors import sl_monitor, hedge_monitor, roll_monitor, square_off_monitor, straddle_price_monitor
    
    modules_to_patch = [
        models.state, 
        background.tasks, 
        trading.square_off, 
        trading.builder, 
        trading.hedger, 
        trading.pnl_calculator, 
        trading.trade_manager,
        # Add all monitor modules to the patch list
        sl_monitor, hedge_monitor, roll_monitor, square_off_monitor, straddle_price_monitor
    ]

    for mod in modules_to_patch:
        mod.state = state_obj

    # 3. Create and start the TradeManager
    state_obj.trade_manager = create_trade_manager(trade_uid, config)
    await state_obj.trade_manager.restore_and_start_monitoring()
    logger.info(f"[{trade_uid}] TradeManager created and monitors started.")

    # 4. Main loop
    while True:
        try:
            # Check for commands from the main process (non-blocking)
            if not command_q.empty():
                command_data = command_q.get()
                command = command_data.get('command')
                logger.info(f"[{trade_uid}] Received command: {command}")
                
                if command == 'SQUARE_OFF':
                    result = await square_off_by_trade_uid(trade_uid)
                    # If square-off was successful, the trade is closed, and this worker's job is done.
                    if result and result.get('success'):
                        logger.info(f"[{trade_uid}] Square-off successful. Shutting down worker process.")
                        await state_obj.trade_manager.stop_monitoring()
                        break # Exit the loop to terminate the process.
                elif command == 'PARTIAL_SQUARE_OFF':
                    await partial_square_off(trade_uid, command_data.get('percentage'))
                elif command == 'SYNC':
                    await manual_sync_trade_orders(trade_uid)
                elif command == 'STOP':
                    logger.info(f"[{trade_uid}] Stop command received. Shutting down.")
                    await state_obj.trade_manager.stop_monitoring()
                    break
                elif command == 'UPDATE_CONFIG':
                    new_config = command_data.get('data')
                    logger.info(f"[{trade_uid}] Received UPDATE_CONFIG command with data: {new_config}")
                    if state_obj.trade_manager:
                        await state_obj.trade_manager.update_configuration(new_config)
                    else:
                        logger.error(f"[{trade_uid}] TradeManager not found, cannot update config.")

            # The monitors are running in this process's event loop.
            # The snapshot loop in the main process will pull data from the queue.
            latest_snapshot = state_obj.trade_snapshots.get(trade_uid)
            if latest_snapshot:
                snapshot_q.put(latest_snapshot)

            await asyncio.sleep(1)

        except (asyncio.CancelledError, KeyboardInterrupt):
            logger.info(f"[{trade_uid}] Worker loop cancelled.")
            break
        except Exception as e:
            logger.error(f"[{trade_uid}] Error in worker loop: {e}", exc_info=True)
            await asyncio.sleep(5)

    # Cleanup
    state_obj.db.close()
    logger.info(f"[{trade_uid}] Worker process finished.")

def trade_process_worker_entry(trade_uid: str, config: Dict, command_q: multiprocessing.Queue, snapshot_q: multiprocessing.Queue, option_chains_proxy):
    """Entry point for the multiprocessing.Process."""
    asyncio.run(trade_process_worker_async(trade_uid, config, command_q, snapshot_q, option_chains_proxy))