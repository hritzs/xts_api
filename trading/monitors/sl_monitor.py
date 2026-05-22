"""
Stop-Loss Monitor - Start Time + Interval gates, lazy event_bus
"""
import time
from datetime import datetime
import asyncio
from utils.logger import logger
from models.state import state
from trading.event_bus import get_event_bus, EventPriority
from utils.helpers import get_ist_now


class SLMonitor:
    def __init__(self, trade_uid: str, config: dict):
        self.trade_uid         = trade_uid
        self.config            = config
        self.running           = False
        self.sl_bps            = float(config.get('sl_bps', 14))
        self.interval          = float(config.get('sl_monitor_interval', 60))
        self.sl_start_time_str = config.get('sl_start_time')  # "HH:MM:SS"
        self.sl_points         = 0.0
        self._last_check_time  = 0.0

        logger.info(f"✅ SLMonitor initialized: {self.trade_uid}")
        logger.info(f"   SL BPS: {self.sl_bps} | Interval: {self.interval}s | Start: {self.sl_start_time_str}")

    @property
    def event_bus(self):
        return get_event_bus()

    async def start(self):
        if self.running:
            return
        self.running = True

        try:
            now = get_ist_now()
            parts = list(map(int, self.sl_start_time_str.split(':')))
            configured_start = now.replace(
                hour=parts[0],
                minute=parts[1],
                second=parts[2] if len(parts) > 2 else 0,
                microsecond=0
            )
            elapsed = (now - configured_start).total_seconds()
            
            if elapsed < 0:
                # Before start time: offset so it triggers EXACTLY at start time
                self._last_check_time = time.monotonic() - self.interval
            else:
                # After start time: align checks to the interval rhythm
                self._last_check_time = time.monotonic() - (elapsed % self.interval)
        except Exception:
            self._last_check_time = time.monotonic()
            elapsed = 0.0

        try:
            delay = self.interval - (elapsed % self.interval) if elapsed >= 0 else -elapsed
        except Exception:
            delay = self.interval

        logger.info(
            f"🛡️ SLMonitor enabled: {self.trade_uid} | "
            f"First check in {delay:.0f}s"
        )

    async def stop(self):
        if not self.running:
            return
        self.running = False
        logger.info(f"🛑 SLMonitor disabled: {self.trade_uid}")

    async def check(self):
        if not self.running:
            return

        # ── Gate 1: Start time ────────────────────────────────────────────────
        if self.sl_start_time_str:
            now_time = get_ist_now().time()
            try:
                from datetime import time as dt_time
                parts = list(map(int, self.sl_start_time_str.split(':')))
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

        try:
            logger.info(f"🛡️ SL Check at {get_ist_now().strftime('%H:%M:%S')} for {self.trade_uid}")

            snapshot = state.trade_snapshots.get(self.trade_uid)
            if not snapshot:
                logger.warning(f"SLMonitor: No snapshot available for {self.trade_uid}. Skipping check.")
                return

            if snapshot.get('live_positions'):
                logger.info(f"--- Positions used for SL Check ({self.trade_uid}) ---")
                for pos in sorted(snapshot['live_positions'], key=lambda p: (p.get('strike', 0), p.get('option_type', ''))):
                    logger.info(
                        f"  - {pos.get('action', 'N/A')} {pos.get('quantity', 0)} {pos.get('option_type', '')} {pos.get('strike', 0)} "
                        f"| Entry: {pos.get('entry_price', 0):.2f} | LTP: {pos.get('ltp', 0):.2f} "
                        f"| PnL: ₹{pos.get('pnl', 0):.2f}"
                    )
                logger.info("-----------------------------------------------------")
            else:
                logger.info("--- No live positions found in snapshot for SL Check ---")

            loop = asyncio.get_event_loop()
            db_trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, self.trade_uid)
            if not db_trade or db_trade.get('status') != 'ACTIVE':
                logger.warning(f"SLMonitor: Trade {self.trade_uid} not active. Stopping.")
                await self.stop()
                return

            pnl_per_straddle = snapshot.get('pnl_per_straddle', 0.0)
            live_spot_price  = snapshot.get('spot_price', 0.0)

            if live_spot_price <= 0:
                logger.warning(f"SLMonitor: Invalid spot price for {self.trade_uid} ({live_spot_price}). Skipping.")
                return

            sl_pts         = (live_spot_price * self.sl_bps) / 10000
            sl_threshold   = -1 * sl_pts
            self.sl_points = sl_pts

            logger.info(f"🛡️  SL Params for {self.trade_uid}:")
            logger.info(f"   - PnL/Straddle: ₹{pnl_per_straddle:.2f}")
            logger.info(f"   - SL Threshold/Straddle: ₹{sl_threshold:.2f}")

            if pnl_per_straddle <= sl_threshold:
                logger.warning(
                    f"🚨 STOP-LOSS HIT: {self.trade_uid} | "
                    f"PnL/Straddle: ₹{pnl_per_straddle:.2f} <= Threshold/Straddle: ₹{sl_threshold:.2f}"
                )
                eb = self.event_bus
                if eb is None:
                    logger.error(f"❌ SLMonitor: event_bus is None for {self.trade_uid}")
                    return
                await eb.emit(
                    event_type="sl_triggered",
                    trade_uid=self.trade_uid,
                    priority=EventPriority.STOP_LOSS,
                )
                await self.stop()
            else:
                logger.info(f"🛡️ SL Check OK: PnL is above threshold.")

        except Exception as e:
            logger.error(f"❌ SLMonitor check error for {self.trade_uid}: {e}", exc_info=True)
