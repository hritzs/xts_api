"""
Trade Manager - Manages the lifecycle and monitoring of a single trade.
"""
import asyncio
from typing import Dict, Optional

from utils.logger import logger
from models.state import state
from trading.monitors.sl_monitor import SLMonitor
from trading.monitors.hedge_monitor import HedgeMonitor
from trading.monitors.roll_monitor import RollMonitor
from trading.monitors.square_off_monitor import SquareOffMonitor
from trading.monitors.straddle_price_monitor import StraddlePriceMonitor

# Global registry for trade managers
_trade_managers: Dict[str, 'TradeManager'] = {}

class TradeManager:
    """Manages all monitoring aspects for a single trade."""

    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid = trade_uid
        self.config = config
        self.db = state.db
        self._initialize_monitors()
        logger.info(f"✅ TradeManager created: {self.trade_uid}")

    def _initialize_monitors(self):
        """Initializes all monitor instances for the trade."""
        self.sl_monitor = SLMonitor(self.trade_uid, self.config)
        self.hedge_monitor = HedgeMonitor(self.trade_uid, self.config)
        self.roll_monitor = RollMonitor(self.trade_uid, self.config)
        self.square_off_monitor = SquareOffMonitor(self.trade_uid, self.config)
        self.straddle_price_monitor = StraddlePriceMonitor(self.trade_uid, self.config)
        logger.info(f"✅ All monitors initialized for {self.trade_uid}")

    async def start_monitoring(self):
        """Starts all monitors for the trade."""
        logger.info(f"📊 Starting all monitors for new trade: {self.trade_uid}")
        await self.sl_monitor.start()
        await self.hedge_monitor.start()
        await self.roll_monitor.start()
        await self.square_off_monitor.start()
        await self.straddle_price_monitor.start()

    async def stop_monitoring(self):
        """Stops all monitors for the trade."""
        logger.info(f"🛑 Stopping all monitors for trade: {self.trade_uid}")
        await self.sl_monitor.stop()
        await self.hedge_monitor.stop()
        await self.roll_monitor.stop()
        await self.square_off_monitor.stop()
        await self.straddle_price_monitor.stop()

    async def restore_and_start_monitoring(self):
        """Restores state from DB and starts monitors."""
        logger.info(f"🔄 Restoring and starting monitoring for {self.trade_uid}...")
        # The config is already loaded from the DB when the process starts.
        # We just need to start the monitors.
        await self.start_monitoring()

    async def update_configuration(self, new_config: dict):
        """Updates the trade's config and restarts all monitors."""
        logger.info(f"[{self.trade_uid}] Updating configuration with: {new_config}")
        try:
            # 1. Stop all current monitors
            await self.stop_monitoring()
            logger.info(f"[{self.trade_uid}] All monitors stopped for config update.")

            # 2. Fetch the latest trade data and merge the new config
            loop = asyncio.get_event_loop()
            trade_data = await loop.run_in_executor(None, self.db.get_straddle_by_id, self.trade_uid)
            if not trade_data:
                logger.error(f"[{self.trade_uid}] Could not find trade data to update config. Aborting update.")
                return

            # Merge new config into existing config
            if 'config' not in trade_data or not isinstance(trade_data.get('config'), dict):
                trade_data['config'] = {}
            trade_data['config'].update(new_config)

            # 3. Save the updated trade data to the DB
            await loop.run_in_executor(None, self.db.insert_straddle, trade_data)
            logger.info(f"[{self.trade_uid}] Saved updated config to DB.")

            # 4. Re-initialize and start monitors with the new config
            self.config = trade_data['config']  # Update manager's internal config
            self._initialize_monitors()  # This method re-creates monitor instances
            await self.start_monitoring()  # This method starts them

            logger.info(f"✅ [{self.trade_uid}] Monitors restarted with new configuration.")

            # 5. Trigger a snapshot to update the UI with the new monitor statuses
            from background.tasks import trigger_snapshot_and_broadcast
            # Pass the updated trade_data to ensure the snapshot uses the absolute latest config
            await trigger_snapshot_and_broadcast(self.trade_uid, trade_data=trade_data)

        except Exception as e:
            logger.error(f"[{self.trade_uid}] Failed to update configuration: {e}", exc_info=True)


def create_trade_manager(trade_uid: str, config: Dict) -> 'TradeManager':
    """Factory function to create and register a TradeManager."""
    if trade_uid in _trade_managers:
        logger.warning(f"TradeManager for {trade_uid} already exists. Returning existing instance.")
        return _trade_managers[trade_uid]
    
    manager = TradeManager(trade_uid, config)
    _trade_managers[trade_uid] = manager
    return manager

def get_trade_manager(trade_uid: str) -> Optional['TradeManager']:
    """Retrieves a registered TradeManager instance."""
    return _trade_managers.get(trade_uid)

def remove_trade_manager(trade_uid: str):
    """
    Stops the associated process and removes it from the global registry.
    NOTE: This is a forceful termination. It's better for the worker
    process to self-terminate upon completion of its trade.
    """
    logger.info(f"Request to remove trade manager and process for {trade_uid}...")
    
    # This function is called from the main process and accesses the main process's state.
    if trade_uid in state.trade_processes:
        process_info = state.trade_processes.pop(trade_uid, None) # Atomically get and remove
        if not process_info:
            return

        process = process_info.get('process')
        if process and process.is_alive():
            logger.warning(f"Forcefully terminating process for trade {trade_uid} (PID: {process.pid}).")
            process.terminate()
            process.join(timeout=5)
        logger.info(f"✅ Process for {trade_uid} removed from registry.")
    else:
        logger.warning(f"⚠️  Could not find process for trade {trade_uid} to remove.")

def register_event_handlers():
    """Placeholder for registering event handlers if needed."""
    pass