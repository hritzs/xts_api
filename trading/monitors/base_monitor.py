"""
Base Monitor Class
"""
import asyncio
from datetime import datetime
from typing import Dict, Optional

from utils.logger import logger
from utils.helpers import get_ist_now


class BaseMonitor:
    def __init__(self, trade_uid: str, interval: float, name: str, start_time):
        self.trade_uid = trade_uid
        self.interval = interval
        self.name = name
        self.start_time = start_time
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
        raise NotImplementedError

    async def check(self):
        raise NotImplementedError