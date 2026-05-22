"""
Straddle Price Monitor - Interval gate + lazy event_bus
No start_time — runs from position entry.
"""
import time
from typing import Dict
from utils.logger import logger
from models.state import state
from trading.event_bus import get_event_bus, EventPriority


class StraddlePriceMonitor:
    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid        = trade_uid
        self.config           = config
        self.running          = False
        self.high_water_mark  = 0.0
        self.interval         = float(config.get('straddle_price_monitor_interval', 3))
        self._last_check_time = 0.0

        logger.info(f"✅ StraddlePriceMonitor initialized: {trade_uid} | Interval: {self.interval}s")

    @property
    def event_bus(self):
        return get_event_bus()

    async def start(self):
        trade = state.db.get_straddle_by_id(self.trade_uid)
        if not trade:
            return
        self.high_water_mark  = trade.get('ce_entry_price', 0) + trade.get('pe_entry_price', 0)
        self.running          = True
        self._last_check_time = 0.0
        logger.info(f"🎯 StraddlePriceMonitor enabled: {self.trade_uid} | Initial HWM: {self.high_water_mark:.2f}")

    async def stop(self):
        self.running = False
        logger.info(f"🛑 StraddlePriceMonitor disabled: {self.trade_uid}")

    async def check(self):
        if not self.running:
            return

        # ── Gate: Interval ────────────────────────────────────────────────
        now_mono = time.monotonic()
        if now_mono - self._last_check_time < self.interval:
            return
        self._last_check_time = now_mono

        trade = state.db.get_straddle_by_id(self.trade_uid)
        if not trade or trade.get('status') != 'ACTIVE':
            await self.stop()
            return

        snapshot = state.trade_snapshots.get(self.trade_uid)
        if not snapshot:
            return

        live_config         = trade.get('config', {})
        price_drop_trigger  = float(live_config.get('straddle_price_drop_trigger', 0.0))
        if price_drop_trigger <= 0:
            return

        original_strike = trade.get('strike')
        ce_leg = next((p for p in snapshot.get('live_positions', [])
                       if p.get('strike') == original_strike and p.get('option_type') == 'CE'), None)
        pe_leg = next((p for p in snapshot.get('live_positions', [])
                       if p.get('strike') == original_strike and p.get('option_type') == 'PE'), None)

        if not (ce_leg and pe_leg):
            return

        current_price        = ce_leg.get('ltp', 0) + pe_leg.get('ltp', 0)
        self.high_water_mark = max(self.high_water_mark, current_price)
        trigger_price        = self.high_water_mark - price_drop_trigger

        if current_price < trigger_price:
            reason = (
                f"Straddle price drop: {current_price:.2f} < "
                f"{trigger_price:.2f} (HWM: {self.high_water_mark:.2f})"
            )
            logger.warning(f"🚨 STRADDLE PRICE TRIGGER: {self.trade_uid} | {reason}")
            eb = self.event_bus
            if eb is None:
                logger.error(f"❌ StraddlePriceMonitor: event_bus is None for {self.trade_uid}")
                return
            await eb.emit(
                event_type="sl_triggered",
                trade_uid=self.trade_uid,
                priority=EventPriority.SL,
                data={'reason': reason}
            )
            await self.stop()
