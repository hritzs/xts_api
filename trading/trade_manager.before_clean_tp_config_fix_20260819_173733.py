"""
Trade Manager - Manages the lifecycle and monitoring of a single trade.
"""
import asyncio
import httpx
from typing import Dict, Optional, Literal
from dataclasses import dataclass
from datetime import datetime, timedelta
from utils.helpers import get_ist_now
from utils.logger import logger
from models.state import state
import config
from trading.monitors.sl_monitor import SLMonitor
from trading.monitors.hedge_monitor import HedgeMonitor
from trading.monitors.roll_monitor import RollMonitor
from trading.monitors.square_off_monitor import SquareOffMonitor
from trading.monitors.entry_straddle_monitor import EntryStraddleMonitor
from trading.monitors.straddle_price_monitor import StraddlePriceMonitor
from trading.monitors.tp_monitor import TPMonitor


# Global registry for trade managers (per-process)
_trade_managers: Dict[str, 'TradeManager'] = {}

@dataclass
class ConditionTask:
    """A generic task for the condition engine to check periodically."""
    uid: str
    task_type: Literal[
        "ENTRY_STRADDLE",
        "EXIT_STRADDLE",
        "LUT_SCORE",
        "TIME_EXIT",
        "STOP_LOSS",
        "TAKE_PROFIT",
        "HEDGE",
        "ROLL",
    ]
    enabled: bool
    trade_uid: str
    next_check: datetime
    interval_seconds: int
    config: dict


class TradeManager:
    """Manages all monitoring aspects for a single trade."""

    def __init__(self, trade_uid: str, trade_data: Dict):
        self.trade_uid = trade_uid
        self.trade_data = trade_data or {}
        self.config = self.trade_data.get('config', {}) or {}
        self.db = state.db
        self._initialize_monitors()
        logger.info(f"✅ TradeManager created: {self.trade_uid}")

    def _initialize_monitors(self):
        # IMPORTANT:
        # SL / Roll / SquareOff / StraddlePrice all need the nested config dict,
        # not the whole trade_data record.
        self.sl_monitor = SLMonitor(self.trade_uid, self.config)
        self.hedge_monitor = HedgeMonitor(self.trade_uid, self.config)
        self.roll_monitor = RollMonitor(self.trade_uid, self.config)
        self.square_off_monitor = SquareOffMonitor(self.trade_uid, self.config)
        self.straddle_price_monitor = StraddlePriceMonitor(self.trade_uid, self.config)
        self.entry_straddle_monitor = EntryStraddleMonitor(self.trade_uid, self.config)
        self.tp_monitor = TPMonitor(self.trade_uid, self.config)
        logger.info(f"✅ All monitors initialized for {self.trade_uid}")

    async def start_monitoring(self):
        """
        Initializes and starts background monitors based on trade status.
        PENDING_ENTRY trades run ONLY EntryStraddleMonitor.
        """
        self.is_running = True
        trade_data = self.db.get_straddle_by_id(self.trade_uid)

        # ── DYNAMIC POST-EXECUTION CONFIG RE-READ ──
        fresh_trade_data = self.db.get_straddle_by_id(self.trade_uid)
        if fresh_trade_data:
            if 'target_entry_price' in fresh_trade_data and fresh_trade_data['target_entry_price'] is not None:
                if getattr(self, 'entry_straddle_monitor', None):
                    self.entry_straddle_monitor.target = float(fresh_trade_data['target_entry_price'])
            if 'target_exit_price' in fresh_trade_data and fresh_trade_data['target_exit_price'] is not None:
                if getattr(self, 'tp_monitor', None):
                    self.tp_monitor.target = float(fresh_trade_data['target_exit_price'])

        status = (trade_data or {}).get("status", "ACTIVE")
        
        import asyncio
        from utils.logger import logger

        if status == "PENDING_ENTRY":
            logger.info(f"[{self.trade_uid}] PENDING_ENTRY state: Starting ONLY EntryStraddleMonitor.")
            if getattr(self, 'entry_straddle_monitor', None) and not getattr(self.entry_straddle_monitor, 'running', False):
                asyncio.create_task(self.entry_straddle_monitor.start())
            return  # Prevents falling through and starting active monitors prematurely
            
        elif status in {"ACTIVE", "PARTIAL", "SQUARING-OFF", "PARTIAL-SQF", "HEDGING", "ROLLING", "BUILDING"}:
            logger.info(f"[{self.trade_uid}] Trade is {status} at startup. Booting all runtime monitors...")
            monitors = [
                getattr(self, 'sl_monitor', None), getattr(self, 'hedge_monitor', None), 
                getattr(self, 'roll_monitor', None), getattr(self, 'square_off_monitor', None), 
                getattr(self, 'straddle_price_monitor', None), getattr(self, 'tp_monitor', None)
            ]
            for m in monitors:
                if m and hasattr(m, 'start') and not getattr(m, 'running', False):
                    asyncio.create_task(m.start())
            return

    async def stop_monitoring(self):
        logger.info(f"🛑 Stopping all monitors for trade: {self.trade_uid}")

        for monitor_name, monitor in [
            ("SL", self.sl_monitor),
            ("HEDGE", self.hedge_monitor),
            ("ROLL", self.roll_monitor),
            ("SQUARE_OFF", self.square_off_monitor),
            ("STRADDLE_PRICE", self.straddle_price_monitor),
            ("ENTRY_STRADDLE", self.entry_straddle_monitor),
            ("TP", self.tp_monitor),
        ]:
            try:
                await monitor.stop()
            except Exception as e:
                logger.error(f"[{self.trade_uid}] Failed to stop {monitor_name} monitor: {e}", exc_info=True)

    async def restore_and_start_monitoring(self):
        logger.info(f"🔄 Restoring and starting monitoring for {self.trade_uid}...")
        await self.start_monitoring()

    async def run_condition_tasks(self):
        """
        Main recurring check loop called continuously by the trade process worker.
        """
        if not self.is_running:
            return

        try:
            # 1. Fetch latest trade state from DB
            trade_data = self.db.get_straddle_by_id(self.trade_uid)
            if not trade_data:
                return

            # -- DYNAMIC POST-EXECUTION CONFIG RE-READ --
            fresh_trade_data = self.db.get_straddle_by_id(self.trade_uid)
            if fresh_trade_data:
                if 'target_entry_price' in fresh_trade_data and fresh_trade_data['target_entry_price'] is not None:
                    if getattr(self, 'entry_straddle_monitor', None):
                        self.entry_straddle_monitor.target = float(fresh_trade_data['target_entry_price'])
                if 'target_exit_price' in fresh_trade_data and fresh_trade_data['target_exit_price'] is not None:
                    if getattr(self, 'tp_monitor', None):
                        self.tp_monitor.target = float(fresh_trade_data['target_exit_price'])

            status = trade_data.get("status")

            # 2. PENDING_ENTRY
            if status == "PENDING_ENTRY":
                mon = getattr(self, 'entry_straddle_monitor', None)
                if mon:
                    if hasattr(mon, 'start') and not getattr(mon, 'running', False):
                        from utils.logger import logger
                        logger.info(f"[{self.trade_uid}] ?? Starting entry_straddle_monitor...")
                        import asyncio
                        asyncio.create_task(mon.start())
                    await mon.check()

            # 3. ACTIVE / EXECUTED
            elif status in {
                "ACTIVE",
                "PARTIAL",
                "SQUARING-OFF",
                "PARTIAL-SQF",
                "HEDGING",
                "ROLLING",
                "BUILDING",
            }:
                import time
                import asyncio
                import httpx
                
                current_time = time.time()
                last_fetch = getattr(self, '_last_snapshot_fetch_time', 0)
                
                if current_time - last_fetch >= 1.0:
                    if not getattr(self, '_http_client', None):
                        self._http_client = httpx.AsyncClient()
                    
                    try:
                        url = f"http://localhost:8003/api/snapshots/{self.trade_uid}"
                        resp = await self._http_client.get(url, timeout=2.0)
                        if resp.status_code == 200:
                            data = resp.json()
                            self.latest_snapshot = data.get("data", data)
                            try:
                                from models.state import state
                                if not hasattr(state, 'trade_snapshots'):
                                    state.trade_snapshots = {}
                                state.trade_snapshots[self.trade_uid] = self.latest_snapshot
                            except Exception:
                                pass
                    except Exception:
                        pass
                    finally:
                        self._last_snapshot_fetch_time = current_time
                
                if not getattr(self, 'latest_snapshot', None):
                    return

                active_monitors = [
                    'sl_monitor',
                    'hedge_monitor',
                    'roll_monitor',
                    'square_off_monitor',
                    'straddle_price_monitor',
                    'tp_monitor',
                ]
                for mon_name in active_monitors:
                    mon = getattr(self, mon_name, None)
                    if mon and hasattr(mon, 'start') and not getattr(mon, 'running', False):
                        from utils.logger import logger
                        logger.info(f"[{self.trade_uid}] ?? Starting {mon_name}...")
                        asyncio.create_task(mon.start())

                if getattr(self, 'sl_monitor', None): await self.sl_monitor.check()
                if getattr(self, 'hedge_monitor', None): await self.hedge_monitor.check()
                if getattr(self, 'roll_monitor', None): await self.roll_monitor.check()
                if getattr(self, 'square_off_monitor', None): await self.square_off_monitor.check()
                if getattr(self, 'straddle_price_monitor', None): await self.straddle_price_monitor.check()
                if getattr(self, 'tp_monitor', None): await self.tp_monitor.check()

        except Exception as e:
            from utils.logger import logger
            logger.error(f"[{self.trade_uid}] Error in run_condition_tasks: {e}", exc_info=True)

    async def _check_entry_straddle(self, task: ConditionTask):
        await self.entry_straddle_monitor.check()
        pass

    async def _check_exit_straddle(self, task: ConditionTask):
        logger.info(f"Checking EXIT_STRADDLE for {task.trade_uid}...")
        # Logic to check current straddle vs. target and call square_off will go here.
        pass

    async def _check_lut_score(self, task: ConditionTask):
        logger.info(f"Checking LUT_SCORE for {task.trade_uid}...")
        # Logic to evaluate LUT and call _execute_build will go here.
        pass

    async def _check_time_exit(self, task: ConditionTask):
        logger.info(f"Checking TIME_EXIT for {task.trade_uid}...")
        # Logic to check current time vs. exit time and call square_off will go here.
        pass

    async def _check_stop_loss(self, task: ConditionTask):
        logger.info(f"Checking STOP_LOSS for {task.trade_uid}...")
        # Logic to check PnL vs. SL threshold and call square_off will go here.
        pass

    async def _check_take_profit(self, task: ConditionTask):
        logger.info(f"Checking TAKE_PROFIT for {task.trade_uid}...")
        # Logic to check PnL vs. TP threshold and call square_off will go here.
        pass

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

            self.trade_data = trade_data
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
        net_delta = data.get('net_delta', 0.0)
        target_delta_reduction = data.get('target_delta_reduction')
        atm_strike = data.get('atm_strike')
        logger.warning(f"🛡️ Hedge executing for {event.trade_uid} | Δ={net_delta:.2f}")
        await execute_synthetic_hedge(
            trade_uid=event.trade_uid,
            net_delta=net_delta,
            target_delta_reduction=target_delta_reduction,
            hedge_type="DELTA",
            atm_strike_override=atm_strike,
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
        data = event.data or {}
        reason = "Manual" if data.get('manual_trigger') else "Time-based"
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
        data = event.data or {}
        percentage = data.get('percentage')
        if percentage is None:
            logger.error(f"❌ Partial square-off handler error [{event.trade_uid}]: 'percentage' missing from event data.")
            return
        await partial_square_off(event.trade_uid, percentage)
    except Exception as e:
        logger.error(f"❌ Partial square-off handler error [{event.trade_uid}]: {e}", exc_info=True)


async def _handle_tp(event):
    """Handle TP_HIT event — delegates to square_off.py"""
    try:
        from trading.square_off import square_off_by_trade_uid
        reason = event.data.get("reason", "TP_HIT")
        logger.warning(f"💰 TAKE PROFIT HIT! Executing square-off for {event.trade_uid} (Reason: {reason})")
        await square_off_by_trade_uid(event.trade_uid, reason=reason)
    except Exception as e:
        logger.error(f"❌ TP handler error [{event.trade_uid}]: {e}", exc_info=True)


# ── Registry helpers ──────────────────────────────────────────────────────────



async def _handle_exit_at_straddle(event):
    """Handle EXIT_AT_STRADDLE event."""
    try:
        from trading.square_off import square_off_by_trade_uid

        reason = event.data.get("reason", "EXIT_AT_STRADDLE")

        logger.warning(
            f"?? EXIT_AT_STRADDLE executing square-off for "
            f"{event.trade_uid} (Reason: {reason})"
        )

        await square_off_by_trade_uid(
            event.trade_uid,
            reason=reason
        )

    except Exception as e:
        logger.error(
            f"? EXIT_AT_STRADDLE handler error "
            f"[{event.trade_uid}]: {e}",
            exc_info=True
        )


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

    eb.register_handler("hedge_needed", _handle_hedge)
    eb.register_handler("sl_triggered", _handle_sl)
    eb.register_handler("square_off_needed", _handle_square_off)
    eb.register_handler("time_to_square_off", _handle_square_off)
    eb.register_handler("partial_square_off_needed", _handle_partial_square_off)
    eb.register_handler("TP_HIT", _handle_tp)
    eb.register_handler("EXIT_AT_STRADDLE", _handle_exit_at_straddle)
    eb.register_handler("roll_needed", _handle_roll)

    logger.info("✅ Event handlers registered: hedge | sl | square_off (manual/auto) | partial_sqoff | roll | tp")


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

    if trade_uid in _trade_managers:
        _trade_managers.pop(trade_uid, None)

    process_info = state.trade_processes.pop(trade_uid, None) if trade_uid in state.trade_processes else None
    process = getattr(state, 'local_process_refs', {}).pop(trade_uid, None)
    command_q = getattr(state, 'local_command_queues', {}).pop(trade_uid, None)

    if command_q:
        try:
            command_q.put({'command': 'STOP'})
        except Exception as e:
            logger.warning(f"Failed to send STOP to {trade_uid}: {e}")

    if process:
        try:
            process.join(timeout=2)
        except Exception as e:
            logger.warning(f"Error while waiting for process {trade_uid} to stop gracefully: {e}")

    if process and process.is_alive():
        logger.warning(f"Force-terminating process for {trade_uid} (PID: {process.pid}).")
        try:
            process.terminate()
            process.join(timeout=5)
        except Exception as e:
            logger.warning(f"Error while force-terminating process for {trade_uid}: {e}")

    if process_info or process or command_q:
        logger.info(f"✅ Process for {trade_uid} removed.")
    else:
        logger.warning(f"⚠️ No process found for {trade_uid}.")
