"""
Hedge Monitor - Start Time + Interval gates, lazy event_bus
"""
import time
from datetime import datetime
from typing import Dict
from utils.logger import logger
from models.state import state
from trading.event_bus import get_event_bus, EventPriority
from utils.helpers import get_ist_now


class HedgeMonitor:
    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid            = trade_uid
        self.config               = config
        self.running              = False
        self.interval             = float(config.get('hedge_monitor_interval', 60.0))
        self.hedge_div            = float(config.get('hedge_div', 19.0))
        self.straddle_div         = float(config.get('straddle_div', 3.0))
        self.hedge_frac           = float(config.get('hedge_frac', 1.0))
        self.hedge_start_time_str = config.get('hedge_start_time')  # "HH:MM:SS"
        self._last_check_time     = 0.0

        logger.info(f"✅ HedgeMonitor initialized: {self.trade_uid}")
        logger.info(f"   Interval: {self.interval}s | Start: {self.hedge_start_time_str}")

    @property
    def event_bus(self):
        return get_event_bus()

    async def start(self):
        if self.running:
            return
        self.running = True

        try:
            now = get_ist_now()
            parts = list(map(int, self.hedge_start_time_str.split(':')))
            configured_start = now.replace(
                hour=parts[0],
                minute=parts[1],
                second=parts[2] if len(parts) > 2 else 0,
                microsecond=0
            )
            elapsed = (now - configured_start).total_seconds()
            
            if elapsed < 0:
                self._last_check_time = time.monotonic() - self.interval
            else:
                self._last_check_time = time.monotonic() - (elapsed % self.interval)
        except Exception:
            self._last_check_time = time.monotonic()
            elapsed = 0.0

        try:
            delay = self.interval - (elapsed % self.interval) if elapsed >= 0 else -elapsed
        except Exception:
            delay = self.interval

        logger.info(
            f"✅ HedgeMonitor enabled for {self.trade_uid} | "
            f"First check in {delay:.0f}s"
        )

    async def stop(self):
        if not self.running:
            return
        self.running = False
        logger.info(f"🛑 HedgeMonitor disabled for {self.trade_uid}.")

    async def check(self):
        if not self.running:
            return

        # ── Gate 1: Start time ────────────────────────────────────────────────
        if self.hedge_start_time_str:
            now_time = get_ist_now().time()
            try:
                from datetime import time as dt_time
                parts = list(map(int, self.hedge_start_time_str.split(':')))
                start = dt_time(parts[0], parts[1], parts[2] if len(parts) > 2 else 0)
                if now_time < start:
                    return
            except Exception:
                pass

        # ── Gate 2: Interval ──────────────────────────────────────────────────
        now_mono = time.monotonic()
        if now_mono - self._last_check_time < self.interval:
            return
        self._last_check_time = now_mono

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            logger.warning(f"Hedge check for {self.trade_uid}: Snapshot not available.")
            return

        pts_out        = snapshot.get('pts_out', 0.0)
        points_allowed = snapshot.get('points_allowed', float('inf'))
        net_delta      = snapshot.get('net_delta', 0.0)
        atm_strike     = snapshot.get('atm_strike', 0)

        logger.info(f"🛡️  Hedge Check for {self.trade_uid}: Pts Out: {pts_out:.2f}, Allowed: {points_allowed:.2f}")

        if pts_out > points_allowed:
            logger.warning(
                f"HEDGE TRIGGERED for {self.trade_uid}: "
                f"Pts Out ({pts_out:.2f}) > Allowed ({points_allowed:.2f})"
            )
            hedge_params = {
                "net_delta":              net_delta,
                "target_delta_reduction": -net_delta * self.hedge_frac,
                "trigger_time":           get_ist_now(),
                "atm_strike":             atm_strike,
            }
            if not hasattr(state, 'hedge_params'):
                state.hedge_params = {}
            state.hedge_params[self.trade_uid] = hedge_params

            eb = self.event_bus
            if eb is None:
                logger.error(f"❌ HedgeMonitor: event_bus is None for {self.trade_uid}")
                return
            await eb.emit("hedge_needed", self.trade_uid, EventPriority.HEDGE, hedge_params)
