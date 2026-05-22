"""
Trade Manager - Manages the lifecycle and monitoring of a single trade.
"""
import asyncio
import httpx
from typing import Dict, Optional

from utils.logger import logger
from models.state import state
import config
from trading.monitors.sl_monitor import SLMonitor
from trading.monitors.hedge_monitor import HedgeMonitor
from trading.monitors.roll_monitor import RollMonitor
from trading.monitors.square_off_monitor import SquareOffMonitor
from trading.monitors.straddle_price_monitor import StraddlePriceMonitor


# Global registry for trade managers (per-process)
_trade_managers: Dict[str, 'TradeManager'] = {}


class TradeManager:
    """Manages all monitoring aspects for a single trade."""

    def __init__(self, trade_uid: str, trade_data: Dict):
        self.trade_uid = trade_uid
        self.trade_data = trade_data
        self.config    = trade_data.get('config', {})
        self.db        = state.db
        self._initialize_monitors()
        logger.info(f"✅ TradeManager created: {self.trade_uid}")

    def _initialize_monitors(self):
        # ── IMPORTANT: SL and Roll must receive self.config (the nested config dict),
        #    NOT self.trade_data. sl_start_time / roll_start_time live inside config.
        self.sl_monitor             = SLMonitor(self.trade_uid, self.config)           # ← FIXED
        self.hedge_monitor          = HedgeMonitor(self.trade_uid, self.config)        # unchanged
        self.roll_monitor           = RollMonitor(self.trade_uid, self.config)         # ← FIXED
        self.square_off_monitor     = SquareOffMonitor(self.trade_uid, self.config)    # ← FIXED
        self.straddle_price_monitor = StraddlePriceMonitor(self.trade_uid, self.config)
        logger.info(f"✅ All monitors initialized for {self.trade_uid}")

    async def start_monitoring(self):
        logger.info(f"📊 Starting all monitors for trade: {self.trade_uid}")
        await self.sl_monitor.start()
        await self.hedge_monitor.start()
        await self.roll_monitor.start()
        await self.square_off_monitor.start()
        await self.straddle_price_monitor.start()

    async def stop_monitoring(self):
        logger.info(f"🛑 Stopping all monitors for trade: {self.trade_uid}")
        await self.sl_monitor.stop()
        await self.hedge_monitor.stop()
        await self.roll_monitor.stop()
        await self.square_off_monitor.stop()
        await self.straddle_price_monitor.stop()

    async def restore_and_start_monitoring(self):
        logger.info(f"🔄 Restoring and starting monitoring for {self.trade_uid}...")
        await self.start_monitoring()

    async def run_all_checks(self):
        """
        Fetches latest snapshot from snapshot_service, updates local state,
        then runs all active monitor checks. Called every 1s by trade_process_worker.
        Each monitor is self-gated by start_time + interval so actual logic
        only executes at the configured frequency.
        """
        snapshot = await _get_snapshot_from_service(self.trade_uid)
        if not snapshot:
            logger.warning(f"[{self.trade_uid}] No snapshot from service. Skipping checks.")
            return

        if not hasattr(state, "trade_snapshots") or state.trade_snapshots is None:
            state.trade_snapshots = {}
        state.trade_snapshots[self.trade_uid] = snapshot

        if self.sl_monitor and self.sl_monitor.running:
            await self.sl_monitor.check()
        if self.hedge_monitor and self.hedge_monitor.running:
            await self.hedge_monitor.check()
        if self.roll_monitor and self.roll_monitor.running:
            await self.roll_monitor.check()
        if self.square_off_monitor and self.square_off_monitor.running:
            await self.square_off_monitor.check()
        if self.straddle_price_monitor and self.straddle_price_monitor.running:
            await self.straddle_price_monitor.check()

    async def update_configuration(self, new_config: dict):
        """Updates the trade's config and restarts all monitors."""
        logger.info(f"[{self.trade_uid}] Updating configuration: {new_config}")
        try:
            await self.stop_monitoring()

            loop = asyncio.get_event_loop()
            trade_data = await loop.run_in_executor(None, self.db.get_straddle_by_id, self.trade_uid)
            if not trade_data:
                logger.error(f"[{self.trade_uid}] Trade not found. Aborting config update.")
                return

            if 'config' not in trade_data or not isinstance(trade_data.get('config'), dict):
                trade_data['config'] = {}
            trade_data['config'].update(new_config)

            await loop.run_in_executor(None, self.db.insert_straddle, trade_data)
            logger.info(f"[{self.trade_uid}] Updated config saved to DB.")

            self.config = trade_data['config']
            self._initialize_monitors()
            await self.start_monitoring()
            logger.info(f"✅ [{self.trade_uid}] Monitors restarted with new config.")

        except Exception as e:
            logger.error(f"[{self.trade_uid}] Config update failed: {e}", exc_info=True)


# ── Private helper ────────────────────────────────────────────────────────────

async def _get_snapshot_from_service(trade_uid: str) -> Optional[dict]:
    """Fetch latest snapshot for a trade from snapshot_service REST API."""
    url = f"http://localhost:{getattr(config, 'SNAPSHOT_SERVICE_PORT', 8003)}/api/snapshots/{trade_uid}"
    try:
        async with httpx.AsyncClient(timeout=1.0) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                return resp.json()
    except Exception as e:
        logger.debug(f"[{trade_uid}] Snapshot fetch failed: {e}")
    return None


# ── Event handlers (run inside worker process) ────────────────────────────────

async def _handle_hedge(event):
    """Handle hedge_needed — delegates to hedger.py"""
    try:
        from trading.hedger import execute_synthetic_hedge
        data = event.data or {}
        net_delta              = data.get('net_delta', 0.0)
        target_delta_reduction = data.get('target_delta_reduction')
        atm_strike             = data.get('atm_strike')
        logger.warning(f"🛡️ Hedge executing for {event.trade_uid} | Δ={net_delta:.2f}")
        await execute_synthetic_hedge(
            trade_uid              = event.trade_uid,
            net_delta              = net_delta,
            target_delta_reduction = target_delta_reduction,
            hedge_type             = "DELTA",
            atm_strike_override    = atm_strike,
        )
    except Exception as e:
        logger.error(f"❌ Hedge handler error [{event.trade_uid}]: {e}", exc_info=True)


async def _handle_sl(event):
    """Handle sl_triggered — delegates to square_off.py"""
    try:
        from trading.square_off import square_off_by_trade_uid
        logger.warning(f"🚨 SL executing square-off for {event.trade_uid}")
        await square_off_by_trade_uid(event.trade_uid, reason="SL")
    except Exception as e:
        logger.error(f"❌ SL handler error [{event.trade_uid}]: {e}", exc_info=True)


async def _handle_square_off(event):
    """Handle square_off_needed (manual) or time_to_square_off (auto)"""
    try:
        from trading.square_off import square_off_by_trade_uid
        reason = "Manual" if event.data.get('manual_trigger') else "Time-based"
        logger.warning(f"⏹️ {reason} square-off executing for {event.trade_uid}")
        await square_off_by_trade_uid(event.trade_uid, reason=reason)
    except Exception as e:
        logger.error(f"❌ Square-off handler error [{event.trade_uid}]: {e}", exc_info=True)


async def _handle_roll(event):
    """Handle roll_needed — delegates to roller.py"""
    try:
        from trading.roller import roll_position
        logger.warning(f"🔄 Roll executing for {event.trade_uid}")
        await roll_position(event.trade_uid, event.data)
    except Exception as e:
        logger.error(f"❌ Roll handler error [{event.trade_uid}]: {e}", exc_info=True)


async def _handle_partial_square_off(event):
    """Handle partial_square_off_needed — delegates to square_off.py"""
    try:
        from trading.square_off import partial_square_off
        logger.warning(f"⚡ Partial square-off executing for {event.trade_uid}")
        percentage = event.data.get('percentage')
        if percentage is None:
            logger.error(f"❌ Partial square-off handler error [{event.trade_uid}]: 'percentage' missing from event data.")
            return
        await partial_square_off(event.trade_uid, percentage)
    except Exception as e:
        logger.error(f"❌ Partial square-off handler error [{event.trade_uid}]: {e}", exc_info=True)


# ── Registry helpers ──────────────────────────────────────────────────────────

def register_event_handlers():
    """
    Register all event handlers on the current process's global event bus.
    Called once per worker process after set_event_bus() has been called.
    """
    from trading.event_bus import get_event_bus
    eb = get_event_bus()
    if eb is None:
        logger.warning("⚠️ register_event_handlers: event_bus is None. Skipping.")
        return

    eb.register_handler("hedge_needed",              _handle_hedge)
    eb.register_handler("sl_triggered",              _handle_sl)
    eb.register_handler("square_off_needed",         _handle_square_off)
    eb.register_handler("time_to_square_off",        _handle_square_off)
    eb.register_handler("partial_square_off_needed", _handle_partial_square_off)
    eb.register_handler("roll_needed",               _handle_roll)

    logger.info("✅ Event handlers registered: hedge | sl | square_off (manual/auto) | partial_sqoff | roll")


def create_trade_manager(trade_uid: str, config: Dict) -> 'TradeManager':
    if trade_uid in _trade_managers:
        logger.warning(f"TradeManager for {trade_uid} already exists. Returning existing.")
        return _trade_managers[trade_uid]
    manager = TradeManager(trade_uid, config)
    _trade_managers[trade_uid] = manager
    return manager


def get_trade_manager(trade_uid: str) -> Optional['TradeManager']:
    return _trade_managers.get(trade_uid)


def remove_trade_manager(trade_uid: str):
    logger.info(f"Removing trade manager and process for {trade_uid}...")
    if trade_uid in state.trade_processes:
        process_info = state.trade_processes.pop(trade_uid, None)
        if not process_info:
            return
        process = process_info.get('process')
        if process and process.is_alive():
            logger.warning(f"Force-terminating process for {trade_uid} (PID: {process.pid}).")
            process.terminate()
            process.join(timeout=5)
        logger.info(f"✅ Process for {trade_uid} removed.")
    else:
        logger.warning(f"⚠️ No process found for {trade_uid}.")
