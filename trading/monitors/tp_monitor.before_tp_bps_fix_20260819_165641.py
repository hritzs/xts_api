"""
Take Profit (TP) Monitor
Triggers a partial square-off when PnL/Straddle exceeds a defined point threshold.
"""
import asyncio
from typing import Dict
from utils.logger import logger
from trading.event_bus import get_event_bus, EventPriority
from models.state import state
from utils.helpers import get_ist_now

class TPMonitor:
    """
    🎯 TAKE PROFIT MONITOR
    (RULE-BASED)
    
    Triggers when:
        pnl_per_straddle > tp_threshold_points
    (where tp_threshold_points is calculated in the snapshot based on SL points and a multiplier)
    """

    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid = trade_uid
        self.config = config
        self.tp_monitor_interval = config.get("tp_monitor_interval", 60) # Can be its own interval
        self.tp_sl_multiplier = float(config.get("tp_sl_multiplier", 2.0)) # Keep for display
        self.tp_sqf_percentage = float(config.get("tp_sqf_percentage", 25.0))
        self.running = False
        self.triggered = False # Add a flag to ensure it only triggers once

        logger.info(f"✅ TPMonitor initialized: {trade_uid}")
        logger.info(f"   Interval: {self.tp_monitor_interval}s")
        logger.info(f"   TP/SL Multiplier: {self.tp_sl_multiplier}x")
        logger.info(f"   SQF % on TP: {self.tp_sqf_percentage}")

    async def start(self):
        if self.running:
            return
        self.running = True
        logger.info(f"🎯 TPMonitor enabled: {self.trade_uid}")

    async def stop(self):
        if not self.running:
            return
        self.running = False
        logger.info(f"🛑 TPMonitor disabled: {self.trade_uid}")

    async def check(self):
        import datetime
        now = datetime.datetime.now()
        
        # 1. Only execute at the end of the minute (e.g., 58s or 59s)
        if now.second < 58:
            return
            
        # 2. Ensure it strictly runs only ONCE per minute
        current_minute = now.minute
        if getattr(self, '_last_check_minute', -1) == current_minute:
            return
        self._last_check_minute = current_minute

        try:
            if self.triggered: # Don't check again if it has already fired
                await self.stop() # Stop the monitor after it has triggered
                return

            snapshot_time = get_ist_now()
            logger.info(f"🎯 TP Check at {snapshot_time.strftime('%H:%M:%S')} for {self.trade_uid}")

            snapshot = state.trade_snapshots.get(self.trade_uid)
            if not snapshot:
                logger.warning(f"TPMonitor: No snapshot for {self.trade_uid}. Skipping.")
                return

            pnl_per_straddle = snapshot.get('pnl_per_straddle', 0.0)
            # Get the pre-calculated threshold from the snapshot
            tp_threshold_points = snapshot.get('tp_threshold_points', float('inf'))

            if pnl_per_straddle > tp_threshold_points:
                logger.warning(f"✅ TAKE PROFIT TRIGGERED: {self.trade_uid} | PnL/Straddle: ₹{pnl_per_straddle:.2f} > TP Threshold: ₹{tp_threshold_points:.2f}")
                self.triggered = True
                await get_event_bus().emit(
                    # --- FIX: Use the correct event type that the handler is listening for ---
                    event_type="partial_square_off_needed", 
                    trade_uid=self.trade_uid, 
                    priority=EventPriority.SQUARE_OFF, 
                    data={'percentage': self.tp_sqf_percentage}
                )
                await self.stop()
            else:
                logger.info(f"🎯 TP Check OK for {self.trade_uid}: PnL/Straddle ₹{pnl_per_straddle:.2f} <= TP Threshold ₹{tp_threshold_points:.2f}")

        except Exception as e:
            logger.error(f"❌ TPMonitor check error for {self.trade_uid}: {e}", exc_info=True)