"""
Square-off Monitor - Corrected
Checks if it's time to square-off the position based on a configured exit time.
Handles both HH:MM and HH:MM:SS time formats.
"""
import asyncio
from typing import Dict, Optional
from utils.logger import logger
from trading.event_bus import get_event_bus, EventPriority
from models.state import state
from utils.helpers import get_ist_now
from datetime import datetime


class SquareOffMonitor:
    """
    ⏹️ SQUARE-OFF MONITOR

    Features:
    - Monitors the current time against a configured exit time.
    - Triggers a square-off event when the exit time is reached.
    - Correctly parses both 'HH:MM' and 'HH:MM:SS' time formats.
    """

    def __init__(self, trade_uid: str, config: dict):
        self.trade_uid = trade_uid
        self.config = config
        self.exit_time_str = config.get('exit_time')
        self.exit_time: Optional[datetime] = None
        self.running = False

        if self.exit_time_str:
            try:
                today = get_ist_now().date()
                # --- FIX: Handle both HH:MM and HH:MM:SS ---
                if len(self.exit_time_str.split(':')) == 3:
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

    async def start(self):
        """Enables the monitor if an exit time is configured."""
        if self.exit_time:
            self.running = True
            logger.info(f"⏹️ SquareOffMonitor enabled: {self.trade_uid}")

    async def stop(self):
        """Disables the monitor."""
        self.running = False
        logger.info(f"🛑 SquareOffMonitor disabled for {self.trade_uid}")

    async def check(self):
        """Performs a single check. Called by the TradeManager orchestrator."""
        if not self.running or not self.exit_time:
            return

        now = get_ist_now()
        if now >= self.exit_time:
            logger.warning(f"⏹️ TIME TO SQUARE OFF: {self.trade_uid} (Exit time {self.exit_time_str} reached)")
            event_bus = get_event_bus()
            await event_bus.emit(event_type="time_to_square_off", trade_uid=self.trade_uid, priority=EventPriority.SQUARE_OFF)
            await self.stop()  # Stop after triggering once