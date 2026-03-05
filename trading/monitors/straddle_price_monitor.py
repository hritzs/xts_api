"""
Straddle Price Monitor
Triggers a square-off if the combined premium drops by a configured amount from its high-water mark.
"""
import asyncio
from typing import Dict
from utils.logger import logger
from models.state import state
from trading.event_bus import get_event_bus, EventPriority


class StraddlePriceMonitor:
    """
    🎯 STRADDLE PRICE MONITOR

    Monitors the combined straddle premium and triggers a square-off if it drops
    by a configured amount from its highest point (high-water mark).
    """

    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid = trade_uid
        self.config = config
        self.running = False
        self.high_water_mark = 0.0
        # Read from config on each check to allow for dynamic changes
        self.monitor_interval = int(config.get('straddle_price_monitor_interval', 5))  # Check frequently
        logger.info(f"✅ StraddlePriceMonitor initialized: {trade_uid}")

    async def start(self):
        """Enables the monitor to be checked by the orchestrator."""
        trade = state.db.get_straddle_by_id(self.trade_uid)
        if not trade:
            return

        # Initialize high_water_mark with entry premium
        self.high_water_mark = trade.get('ce_entry_price', 0) + trade.get('pe_entry_price', 0)
        self.running = True
        logger.info(f"🎯 StraddlePriceMonitor enabled: {self.trade_uid} | Initial HWM: {self.high_water_mark:.2f}")

    async def stop(self):
        """Disables the monitor."""
        self.running = False
        logger.info(f"🛑 StraddlePriceMonitor disabled: {self.trade_uid}")

    async def check(self):
        """Performs a single check. Called by the TradeManager orchestrator."""
        if not self.running:
            return

        trade = state.db.get_straddle_by_id(self.trade_uid)
        if not trade or trade.get('status') != 'ACTIVE':
            await self.stop()
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            return

        live_config = trade.get('config', {})
        price_drop_trigger = float(live_config.get('straddle_price_drop_trigger', 0.0))

        if price_drop_trigger <= 0:
            return

        # This monitor uses the main CE/PE legs at the original strike
        original_strike = trade.get('strike')
        ce_leg = next((p for p in snapshot.get('live_positions', []) if p.get('strike') == original_strike and p.get('option_type') == 'CE'), None)
        pe_leg = next((p for p in snapshot.get('live_positions', []) if p.get('strike') == original_strike and p.get('option_type') == 'PE'), None)

        if not (ce_leg and pe_leg): return

        current_straddle_price = ce_leg.get('ltp', 0) + pe_leg.get('ltp', 0)
        self.high_water_mark = max(self.high_water_mark, current_straddle_price)
        trigger_price = self.high_water_mark - price_drop_trigger

        if current_straddle_price < trigger_price:
            reason = f"Straddle price drop: {current_straddle_price:.2f} < {trigger_price:.2f} (HWM: {self.high_water_mark:.2f})"
            logger.warning(f"🚨 STRADDLE PRICE TRIGGER: {self.trade_uid} | {reason}")
            event_bus = get_event_bus()
            await event_bus.emit("sl_triggered", self.trade_uid, EventPriority.SL, {'reason': reason})
            await self.stop()