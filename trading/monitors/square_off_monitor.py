"""
Square-off Monitor - Start Time + Interval gates, lazy event_bus
"""
import time
from datetime import datetime
from typing import Optional
from utils.logger import logger
from trading.event_bus import get_event_bus, EventPriority
from utils.helpers import get_ist_now


class SquareOffMonitor:
    def __init__(self, trade_uid: str, config: dict):
        self.trade_uid     = trade_uid
        self.config        = config
        self.exit_time_str = config.get('exit_time')
        self.exit_time: Optional[datetime] = None
        self.running       = False
        self._triggered    = False

        if self.exit_time_str:
            try:
                today = get_ist_now().date()
                parts = self.exit_time_str.split(':')
                if len(parts) == 3:
                    time_part = datetime.strptime(self.exit_time_str, '%H:%M:%S').time()
                else:
                    time_part = datetime.strptime(self.exit_time_str, '%H:%M').time()
                self.exit_time = datetime.combine(today, time_part).replace(tzinfo=get_ist_now().tzinfo)
                logger.info(f"✅ SquareOffMonitor initialized for {self.trade_uid} at {self.exit_time_str}")
            except (ValueError, TypeError):
                logger.warning(f"⚠️ Invalid exit_time format: '{self.exit_time_str}'. Disabling time-based square-off.")
                self.exit_time = None
        else:
            logger.info(f"⏹️ SquareOffMonitor for {self.trade_uid} is disabled (no exit time).")

    @property
    def event_bus(self):
        return get_event_bus()

    async def start(self):
        if self.exit_time:
            self.running = True
            logger.info(f"⏹️ SquareOffMonitor enabled: {self.trade_uid} | Exit at {self.exit_time_str}")

    async def stop(self):
        self.running = False
        logger.info(f"🛑 SquareOffMonitor disabled for {self.trade_uid}.")

    async def check(self):
        if not self.running or not self.exit_time or self._triggered:
            return

        now = get_ist_now()
        if now >= self.exit_time:
            logger.warning(f"⏹️ TIME TO SQUARE OFF: {self.trade_uid} (Exit time {self.exit_time_str} reached)")
            self._triggered = True
            eb = self.event_bus
            if eb is None:
                logger.error(f"❌ SquareOffMonitor: event_bus is None for {self.trade_uid}")
                return
            await eb.emit(
                event_type="time_to_square_off",
                trade_uid=self.trade_uid,
                priority=EventPriority.SQUARE_OFF
            )
            await self.stop()
