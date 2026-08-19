"""
Monitors for managing trade lifecycle events.
"""

import asyncio
import time
from datetime import datetime, time as dt_time, timedelta
from typing import Dict, Any, Optional

from utils.logger import logger
from models.state import state
from utils.helpers import get_ist_now
from trading.event_bus import EventBus, EventPriority


class BaseMonitor:
    def __init__(self, trade_uid: str, interval: float, event_bus: EventBus, name: str):
        self.trade_uid = trade_uid
        self.interval = interval
        self.event_bus = event_bus
        self.name = name
        self.running = False
        self.task: Optional[asyncio.Task] = None
        self.last_run_time: Optional[datetime] = None

    async def start(self):
        if self.running:
            return
        self.running = True
        self.task = asyncio.create_task(self._monitor_loop())
        logger.info(f"[{self.trade_uid}] {self.name} started.")

    async def stop(self):
        if not self.running:
            return
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        logger.info(f"[{self.trade_uid}] {self.name} stopped.")

    async def _monitor_loop(self):
        while self.running:
            try:
                await self.check_condition()
                self.last_run_time = get_ist_now()
            except Exception as e:
                logger.error(f"[{self.trade_uid}] Error in {self.name}: {e}", exc_info=True)
            await asyncio.sleep(self.interval)

    async def check_condition(self):
        raise NotImplementedError

    def update_config(self, new_config: Dict):
        # Generic config update, specific monitors can override
        pass


class SLMonitor(BaseMonitor):
    def __init__(self, trade_uid: str, sl_bps: float, sl_start_time: dt_time, interval: float, event_bus: EventBus):
        super().__init__(trade_uid, interval, event_bus, "SL Monitor")
        self.sl_bps = sl_bps
        self.sl_start_time = sl_start_time
        self.sl_points = 0.0 # This will be set by trade_manager after build

    def update_config(self, new_config: Dict):
        self.sl_bps = new_config.get("sl_bps", self.sl_bps)
        self.sl_start_time = dt_time.fromisoformat(new_config["sl_start_time"]) if isinstance(new_config.get("sl_start_time"), str) else new_config.get("sl_start_time", self.sl_start_time)
        # SL points might also be updated if the base value changes
        if "sl_points" in new_config:
            self.sl_points = new_config["sl_points"]
        logger.info(f"[{self.trade_uid}] SL Monitor config updated: sl_bps={self.sl_bps}, sl_start_time={self.sl_start_time}")

    async def check_condition(self):
        now = get_ist_now()
        if now.time() < self.sl_start_time:
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            logger.warning(f"[{self.trade_uid}] SL Monitor: No snapshot available.")
            return

        total_pnl = snapshot.get("total_pnl", 0.0)
        current_gross_lots = (snapshot.get("ce_quantity", 0) + snapshot.get("pe_quantity", 0)) / (2.0 * snapshot.get("lot_size", 1))
        
        # Use the sl_points from the snapshot if available, otherwise calculate from config
        sl_value_per_lot = snapshot.get("sl_points", self.sl_points)
        
        if sl_value_per_lot <= 0:
            # Fallback calculation if sl_points is not set or invalid
            entry_spot = snapshot.get("entry_spot", 0.0)
            if entry_spot > 0:
                sl_value_per_lot = self.sl_bps * entry_spot / 10000

        if sl_value_per_lot <= 0:
            logger.warning(f"[{self.trade_uid}] SL Monitor: Invalid SL points ({sl_value_per_lot}). Skipping check.")
            return

        total_sl_threshold = -1 * sl_value_per_lot * current_gross_lots * snapshot.get("lot_size", 1)

        if total_pnl <= total_sl_threshold:
            logger.warning(
                f"[{self.trade_uid}] SL HIT! PnL: {total_pnl:.2f} <= Threshold: {total_sl_threshold:.2f}. "
                "Emitting square_off_needed event."
            )
            await self.event_bus.emit(
                event_type="square_off_needed",
                trade_uid=self.trade_uid,
                priority=EventPriority.SQUARE_OFF,
                data={"reason": "SL_HIT", "pnl": total_pnl, "threshold": total_sl_threshold}
            )


class HedgeMonitor(BaseMonitor):
    def __init__(self, trade_uid: str, hedge_div: float, straddle_div: float, hedge_start_time: dt_time, interval: float, event_bus: EventBus):
        super().__init__(trade_uid, interval, event_bus, "Hedge Monitor")
        self.hedge_div = hedge_div
        self.straddle_div = straddle_div
        self.hedge_start_time = hedge_start_time

    def update_config(self, new_config: Dict):
        self.hedge_div = new_config.get("hedge_div", self.hedge_div)
        self.straddle_div = new_config.get("straddle_div", self.straddle_div)
        self.hedge_start_time = dt_time.fromisoformat(new_config["hedge_start_time"]) if isinstance(new_config.get("hedge_start_time"), str) else new_config.get("hedge_start_time", self.hedge_start_time)
        logger.info(f"[{self.trade_uid}] Hedge Monitor config updated: hedge_div={self.hedge_div}, straddle_div={self.straddle_div}, hedge_start_time={self.hedge_start_time}")

    async def check_condition(self):
        now = get_ist_now()
        if now.time() < self.hedge_start_time:
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            logger.warning(f"[{self.trade_uid}] Hedge Monitor: No snapshot available.")
            return

        net_delta = snapshot.get("net_delta", 0.0)
        straddle_value = snapshot.get("straddle_value", 0.0)

        if straddle_value > 0 and abs(net_delta) / straddle_value > self.hedge_div / 100:
            logger.info(
                f"[{self.trade_uid}] HEDGE NEEDED! Net Delta: {net_delta:.2f}, Straddle Value: {straddle_value:.2f}, "
                f"Ratio: {abs(net_delta) / straddle_value:.2f} > Threshold: {self.hedge_div / 100:.2f}. "
                "Emitting hedge_needed event."
            )
            await self.event_bus.emit(
                event_type="hedge_needed",
                trade_uid=self.trade_uid,
                priority=EventPriority.HEDGE,
                data={"net_delta": net_delta, "straddle_value": straddle_value}
            )


class StraddlePriceDropMonitor(BaseMonitor): # New Monitor Class
    def __init__(self, trade_uid: str, trigger_price: float, pct_sqf: float, event_bus: EventBus, interval: float = 5.0):
        super().__init__(trade_uid, interval, event_bus, "Straddle Price Drop Monitor")
        self.trigger_price = trigger_price
        self.pct_sqf = pct_sqf
        self.triggered = False # To prevent multiple triggers for the same event

    def update_config(self, new_config: Dict):
        self.trigger_price = new_config.get("straddle_price_drop_trigger", self.trigger_price)
        self.pct_sqf = new_config.get("straddle_price_drop_pct_sqf", self.pct_sqf)
        logger.info(f"[{self.trade_uid}] Straddle Price Drop Monitor config updated: trigger_price={self.trigger_price}, pct_sqf={self.pct_sqf}")

    async def check_condition(self):
        if self.triggered: # Already triggered for this condition, wait for reset
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            logger.warning(f"[{self.trade_uid}] Straddle Price Drop Monitor: No snapshot available.")
            return

        # Get current ATM straddle price
        atm_strike = snapshot.get("strike") # Assuming snapshot strike is ATM or representative
        if not atm_strike:
            logger.warning(f"[{self.trade_uid}] Straddle Price Drop Monitor: Could not determine ATM strike from snapshot.")
            return

        # Find the ATM row in the current chain data to get live LTPs
        chain_data = state.get_published_option_chain(snapshot.get("symbol"))
        if not chain_data:
            logger.warning(f"[{self.trade_uid}] Straddle Price Drop Monitor: No published option chain for {snapshot.get('symbol')}.")
            return

        atm_row = next((row for row in chain_data.get("chain", []) if row.get("strike") == atm_strike), None)
        if not atm_row:
            logger.warning(f"[{self.trade_uid}] Straddle Price Drop Monitor: ATM row not found in live chain for strike {atm_strike}.")
            return

        ce_ltp = state.get_price(atm_row.get("ce_token")) or atm_row.get("ce_ltp", 0.0)
        pe_ltp = state.get_price(atm_row.get("pe_token")) or atm_row.get("pe_ltp", 0.0)
        live_atm_straddle_price = ce_ltp + pe_ltp

        logger.debug("=" * 100)
        logger.info("[TRADE CONFIG]")
        logger.info(f"entry_at_straddle = {self.trade_config.get('entry_at_straddle')}")
        logger.info(f"exit_at_straddle  = {self.trade_config.get('exit_at_straddle')}")
        logger.debug("=" * 100)

        exit_target = self.trade_config.get("exit_at_straddle")

        if exit_target not in (None, "", 0):

            try:
                exit_target = float(exit_target)
            except Exception:
                logger.warning(f"Invalid exit_at_straddle: {exit_target}")
                exit_target = None

        if exit_target is not None:

            logger.debug("=" * 100)
            logger.info("[EXIT STRADDLE CHECK]")
            logger.info(f"Current={live_atm_straddle_price}")
            logger.info(f"Target ={exit_target}")
            logger.debug("=" * 100)

            if live_atm_straddle_price <= exit_target:

                logger.info("[EXIT AT STRADDLE TRIGGERED]")

                await self.event_bus.emit(
                    event_type="square_off_needed",
                    trade_uid=self.trade_uid,
                    priority=EventPriority.SQUARE_OFF,
                    data={
                        "reason": "EXIT_AT_STRADDLE"
                    }
                )

                return


        if self.trigger_price > 0 and live_atm_straddle_price <= self.trigger_price:
            logger.warning(
                f"[{self.trade_uid}] STRADDLE PRICE DROP TRIGGER HIT! "
                f"Live ATM Straddle Price: {live_atm_straddle_price:.2f} <= Trigger: {self.trigger_price:.2f}. "
                f"Initiating partial square-off of {self.pct_sqf}%."
            )
            self.triggered = True # Set flag to prevent re-triggering immediately
            await self.event_bus.emit(
                event_type="partial_square_off_needed",
                trade_uid=self.trade_uid,
                priority=EventPriority.SQUARE_OFF,
                data={"percentage": self.pct_sqf, "reason": "Straddle Price Drop Trigger"}
            )
        elif self.triggered and live_atm_straddle_price > self.trigger_price:
            # Reset triggered flag if price recovers above trigger
            self.triggered = False
            logger.info(f"[{self.trade_uid}] Straddle Price recovered above trigger. Resetting trigger flag.")


class RollMonitor(BaseMonitor):
    def __init__(self, trade_uid: str, roll_straddle_div: float, roll_start_time: dt_time, interval: float, event_bus: EventBus):
        super().__init__(trade_uid, interval, event_bus, "Roll Monitor")
        self.roll_straddle_div = roll_straddle_div
        self.roll_start_time = roll_start_time

    def update_config(self, new_config: Dict):
        self.roll_straddle_div = new_config.get("roll_straddle_div", self.roll_straddle_div)
        self.roll_start_time = dt_time.fromisoformat(new_config["roll_start_time"]) if isinstance(new_config.get("roll_start_time"), str) else new_config.get("roll_start_time", self.roll_start_time)
        logger.info(f"[{self.trade_uid}] Roll Monitor config updated: roll_straddle_div={self.roll_straddle_div}, roll_start_time={self.roll_start_time}")

    async def check_condition(self):
        now = get_ist_now()
        if now.time() < self.roll_start_time:
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            logger.warning(f"[{self.trade_uid}] Roll Monitor: No snapshot available.")
            return

        net_delta = snapshot.get("net_delta", 0.0)
        straddle_value = snapshot.get("straddle_value", 0.0)

        if straddle_value > 0 and abs(net_delta) / straddle_value > self.roll_straddle_div:
            logger.info(
                f"[{self.trade_uid}] ROLL NEEDED! Net Delta: {net_delta:.2f}, Straddle Value: {straddle_value:.2f}, "
                f"Ratio: {abs(net_delta) / straddle_value:.2f} > Threshold: {self.roll_straddle_div:.2f}. "
                "Emitting roll_needed event."
            )
            await self.event_bus.emit(
                event_type="roll_needed",
                trade_uid=self.trade_uid,
                priority=EventPriority.ROLL,
                data={"net_delta": net_delta, "straddle_value": straddle_value}
            )


class SquareOffMonitor(BaseMonitor):
    def __init__(self, trade_uid: str, exit_time: dt_time, interval: float, event_bus: EventBus):
        super().__init__(trade_uid, interval, event_bus, "Square-Off Monitor")
        self.exit_time = exit_time
        self.exit_time_str = exit_time.strftime("%H:%M:%S")

    def update_config(self, new_config: Dict):
        self.exit_time = dt_time.fromisoformat(new_config["exit_time"]) if isinstance(new_config.get("exit_time"), str) else new_config.get("exit_time", self.exit_time)
        self.exit_time_str = self.exit_time.strftime("%H:%M:%S")
        logger.info(f"[{self.trade_uid}] Square-Off Monitor config updated: exit_time={self.exit_time_str}")

    async def check_condition(self):
        now = get_ist_now()
        if now.time() >= self.exit_time:
            logger.info(
                f"[{self.trade_uid}] SQUARE-OFF TIME REACHED! Current Time: {now.time()}, Exit Time: {self.exit_time}. "
                "Emitting square_off_needed event."
            )
            await self.event_bus.emit(
                event_type="square_off_needed",
                trade_uid=self.trade_uid,
                priority=EventPriority.SQUARE_OFF,
                data={"reason": "EXIT_TIME"}
            )


class TPMonitor(BaseMonitor):
    """Monitors for Take-Profit based on basis points (BPS)."""

    def __init__(self, trade_uid: str, config: Dict):
        self.tp_bps = _safe_float(config.get("tp_bps"))
        # TP monitor can start immediately if configured, so no separate start time
        start_time = dt_time(0, 0, 0)
        interval = _safe_float(config.get("sl_monitor_interval", 60.0))
        super().__init__(trade_uid, interval, "TP Monitor", start_time)
        self.tp_points = 0.0

    def update_config(self, new_config: Dict):
        self.tp_bps = _safe_float(new_config.get("tp_bps", self.tp_bps))
        logger.info(f"[{self.trade_uid}] TP Monitor config updated: tp_bps={self.tp_bps}")

    async def check(self):
        if not self.running or self.tp_bps <= 0:
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            return

        entry_spot = _safe_float(snapshot.get("entry_spot"))
        if entry_spot <= 0:
            return

        self.tp_points = (self.tp_bps / 10000.0) * entry_spot
        pnl_per_straddle = _safe_float(snapshot.get("pnl_per_straddle"))

        if pnl_per_straddle >= self.tp_points:
            logger.warning(
                f"[{self.trade_uid}] TAKE PROFIT HIT! PnL/Straddle: {pnl_per_straddle:.2f} >= "
                f"TP Target: {self.tp_points:.2f} ({self.tp_bps} bps). Emitting square_off_needed event."
            )
            await self.event_bus.emit("square_off_needed", self.trade_uid, EventPriority.SQUARE_OFF, {"reason": "TP_HIT"})


class TradeMonitor:
    """
    Manages multiple monitors for a single trade.
    """
    def __init__(self, trade_uid: str, trade_config: Dict, event_bus: EventBus):
        self.trade_uid = trade_uid
        self.trade_config = trade_config
        self.event_bus = event_bus
        self.monitors: Dict[str, BaseMonitor] = {}

        # Initialize monitors based on config
        sl_monitor_interval = trade_config.get("sl_monitor_interval", 60.0)
        hedge_monitor_interval = trade_config.get("hedge_monitor_interval", 60.0)
        roll_monitor_interval = trade_config.get("roll_monitor_interval", 60.0)
        square_off_monitor_interval = trade_config.get("square_off_monitor_interval", 60.0)

        # SL Monitor
        sl_bps = trade_config.get("sl_bps")
        sl_start_time_str = trade_config.get("sl_start_time")
        if sl_bps is not None and sl_start_time_str:
            sl_start_time = dt_time.fromisoformat(sl_start_time_str)
            self.monitors["sl"] = SLMonitor(trade_uid, sl_bps, sl_start_time, sl_monitor_interval, event_bus)
        else:
            logger.warning(f"[{trade_uid}] SL Monitor disabled due to missing/invalid config.")

        # Hedge Monitor
        hedge_div = trade_config.get("hedge_div")
        straddle_div = trade_config.get("straddle_div")
        hedge_start_time_str = trade_config.get("hedge_start_time")
        if hedge_div is not None and straddle_div is not None and hedge_start_time_str:
            hedge_start_time = dt_time.fromisoformat(hedge_start_time_str)
            self.monitors["hedge"] = HedgeMonitor(trade_uid, hedge_div, straddle_div, hedge_start_time, hedge_monitor_interval, event_bus)
        else:
            logger.warning(f"[{trade_uid}] Hedge Monitor disabled due to missing/invalid config.")

        # Straddle Price Drop Monitor (New)
        straddle_price_drop_trigger = trade_config.get("straddle_price_drop_trigger")
        straddle_price_drop_pct_sqf = trade_config.get("straddle_price_drop_pct_sqf")
        if straddle_price_drop_trigger is not None and straddle_price_drop_pct_sqf is not None and straddle_price_drop_trigger > 0 and straddle_price_drop_pct_sqf > 0:
            self.monitors["straddle_price_drop"] = StraddlePriceDropMonitor(trade_uid, straddle_price_drop_trigger, straddle_price_drop_pct_sqf, event_bus)
        else:
            logger.info(f"[{trade_uid}] Straddle Price Drop Monitor disabled (trigger={straddle_price_drop_trigger}, pct_sqf={straddle_price_drop_pct_sqf}).")

        # Roll Monitor
        roll_straddle_div = trade_config.get("roll_straddle_div")
        roll_start_time_str = trade_config.get("roll_start_time")
        if roll_straddle_div is not None and roll_start_time_str:
            roll_start_time = dt_time.fromisoformat(roll_start_time_str)
            self.monitors["roll"] = RollMonitor(trade_uid, roll_straddle_div, roll_start_time, roll_monitor_interval, event_bus)
        else:
            logger.warning(f"[{trade_uid}] Roll Monitor disabled due to missing/invalid config.")

        # Square-Off Monitor
        exit_time_str = trade_config.get("exit_time")
        if exit_time_str:
            exit_time = dt_time.fromisoformat(exit_time_str)
            self.monitors["square_off"] = SquareOffMonitor(trade_uid, exit_time, square_off_monitor_interval, event_bus)
        else:
            logger.warning(f"[{trade_uid}] Square-Off Monitor disabled due to missing/invalid config.")

    async def start_all(self):
        for monitor in self.monitors.values():
            await monitor.start()

    async def stop_all(self):
        for monitor in self.monitors.values():
            await monitor.stop()

    def update_config(self, new_config: Dict):
        self.trade_config.update(new_config)
        for monitor_name, monitor_instance in self.monitors.items():
            monitor_instance.update_config(self.trade_config)
            # If a monitor was previously disabled due to missing config, and now config is present, start it.
            if not monitor_instance.running and monitor_name == "sl" and self.trade_config.get("sl_bps") is not None and self.trade_config.get("sl_start_time"):
                asyncio.create_task(monitor_instance.start())
            elif not monitor_instance.running and monitor_name == "hedge" and self.trade_config.get("hedge_div") is not None and self.trade_config.get("straddle_div") is not None and self.trade_config.get("hedge_start_time"):
                asyncio.create_task(monitor_instance.start())
            elif not monitor_instance.running and monitor_name == "roll" and self.trade_config.get("roll_straddle_div") is not None and self.trade_config.get("roll_start_time"):
                asyncio.create_task(monitor_instance.start())
            elif not monitor_instance.running and monitor_name == "square_off" and self.trade_config.get("exit_time"):
                asyncio.create_task(monitor_instance.start())
            elif not monitor_instance.running and monitor_name == "straddle_price_drop" and self.trade_config.get("straddle_price_drop_trigger") is not None and self.trade_config.get("straddle_price_drop_pct_sqf") is not None and self.trade_config.get("straddle_price_drop_trigger") > 0 and self.trade_config.get("straddle_price_drop_pct_sqf") > 0:
                asyncio.create_task(monitor_instance.start())