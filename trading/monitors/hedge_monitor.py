"""
Hedge Monitor - Checks for delta hedging needs based on snapshot data.
"""
import asyncio
from typing import Dict
from utils.logger import logger
from models.state import state
from trading.event_bus import get_event_bus, EventPriority
from utils.helpers import get_ist_now

class HedgeMonitor:
    """
    Monitors the need for delta hedging based on pre-calculated snapshot data.
    """
    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid = trade_uid
        self.config = config
        self.running = False
        
        # Parameters from config
        self.hedge_monitor_interval = float(config.get('hedge_monitor_interval', 60.0))
        self.hedge_div = float(config.get('hedge_div', 19.0))
        self.straddle_div = float(config.get('straddle_div', 3.0))
        self.hedge_frac = float(config.get('hedge_frac', 1.0))
        
        self.event_bus = get_event_bus()

    async def start(self):
        """Starts the hedge monitor task."""
        if not self.running:
            self.running = True
            # The orchestrator in trade_manager.py now handles the timed checks.
            # This monitor no longer needs its own loop.
            logger.info(f"✅ HedgeMonitor enabled for {self.trade_uid}. Checks will be orchestrated.")

    async def stop(self):
        """Stops the hedge monitor task."""
        if self.running:
            self.running = False
            logger.info(f"🛑 HedgeMonitor disabled for {self.trade_uid}.")

    async def check(self):
        """
        Performs a single check for hedging needs using the latest trade snapshot.
        This method is called by the TradeManager's orchestrator.
        """
        if not self.running:
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            logger.warning(f"Hedge check for {self.trade_uid}: Snapshot not available.")
            return

        # Extract pre-calculated values from the snapshot
        pts_out = snapshot.get('pts_out', 0.0)
        points_allowed = snapshot.get('points_allowed', float('inf'))
        net_delta = snapshot.get('net_delta', 0.0)
        atm_strike = snapshot.get('atm_strike', 0) # Get ATM strike from snapshot

        logger.info(f"🛡️  Hedge Check for {self.trade_uid}: Pts Out: {pts_out:.2f}, Allowed: {points_allowed:.2f}")

        if pts_out > points_allowed:
            logger.warning(f"HEDGE TRIGGERED for {self.trade_uid}: Pts Out ({pts_out:.2f}) > Allowed ({points_allowed:.2f})")
            
            target_delta_reduction = -net_delta * self.hedge_frac
            
            hedge_params = {
                "net_delta": net_delta,
                "target_delta_reduction": target_delta_reduction,
                "trigger_time": get_ist_now(),
                "atm_strike": atm_strike
            }
            
            if not hasattr(state, 'hedge_params'): state.hedge_params = {}
            state.hedge_params[self.trade_uid] = hedge_params

            await self.event_bus.emit("hedge_needed", self.trade_uid, EventPriority.HEDGE, hedge_params)