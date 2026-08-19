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
        self.tp_monitor_interval = float(
            config.get("tp_monitor_interval", 60)
        )

        # TP configured directly in basis points.
        # Example:
        #   tp_bps = 1.0
        #   entry_spot = 24232.65
        #   TP points = 24232.65 * 1 / 10000
        self.tp_bps = config.get("tp_bps")

        try:
            self.tp_bps = (
                float(self.tp_bps)
                if self.tp_bps is not None
                else None
            )
        except (TypeError, ValueError):
            self.tp_bps = None

        self.tp_sl_multiplier = float(
            config.get("tp_sl_multiplier", 2.0)
        )

        self.tp_sqf_percentage = float(
            config.get("tp_sqf_percentage", 25.0)
        )
        self.running = False
        self.triggered = False # Add a flag to ensure it only triggers once

        logger.info(f"✅ TPMonitor initialized: {trade_uid}")
        logger.info(
            f"   Interval: {self.tp_monitor_interval}s"
        )
        logger.info(
            f"   TP BPS: {self.tp_bps}"
        )
        logger.info(
            f"   TP/SL Multiplier: {self.tp_sl_multiplier}x"
        )
        logger.info(
            f"   SQF % on TP: {self.tp_sqf_percentage}"
        )

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
        # ========================================================
        # MINUTE-END / MINUTE-BOUNDARY GATE
        # ========================================================
        # TP must run ONCE per minute together with the other
        # runtime monitors.
        #
        # We allow the first 2 seconds of the new minute so the
        # check is robust against scheduler jitter:
        #
        #   17:06:00 / 17:06:01 -> CHECK
        #   17:06:02+          -> WAIT
        #
        # The minute key prevents duplicate checks within the
        # same minute.
        # ========================================================

        now = get_ist_now()

        if now.second > 1:
            return

        current_minute_key = now.strftime(
            "%Y-%m-%d %H:%M"
        )

        if getattr(
            self,
            "_last_check_minute",
            None
        ) == current_minute_key:
            return

        self._last_check_minute = (
            current_minute_key
        )

        try:
            if self.triggered: # Don't check again if it has already fired
                await self.stop() # Stop the monitor after it has triggered
                return

            snapshot_time = get_ist_now()

            # TP is intentionally checked in the same runtime monitor
            # cycle as SL / HEDGE / ROLL / STRADDLE-EXIT.
            # The scheduler/TradeManager controls the monitor cadence.
            logger.info(
                f"🎯 TP Check at "
                f"{snapshot_time.strftime('%H:%M:%S')} "
                f"for {self.trade_uid}"
            )

            snapshot = state.trade_snapshots.get(self.trade_uid)
            if not snapshot:
                logger.warning(f"TPMonitor: No snapshot for {self.trade_uid}. Skipping.")
                return

            pnl_per_straddle = float(
                snapshot.get(
                    'pnl_per_straddle',
                    0.0
                ) or 0.0
            )

            # ========================================================
            # TP BPS DIRECT CALCULATION
            # ========================================================
            #
            # tp_bps is the user's configured TP.
            #
            # Example:
            #   Entry Spot = 24232.65
            #   TP BPS     = 1.0
            #
            #   TP points = 24232.65 * 1 / 10000
            #             = 2.423265
            #
            # If tp_bps is missing/<=0, TP is disabled.
            # ========================================================

            if (
                self.tp_bps is None
                or self.tp_bps <= 0
            ):
                logger.info(
                    f"🎯 TP DISABLED for {self.trade_uid} | "
                    f"tp_bps={self.tp_bps}"
                )
                return

            entry_spot = snapshot.get(
                'entry_spot'
            )

            if entry_spot is None:
                entry_spot = snapshot.get(
                    'build_spot'
                )

            try:
                entry_spot = float(
                    entry_spot
                )
            except (
                TypeError,
                ValueError
            ):
                entry_spot = 0.0

            if entry_spot <= 0:
                logger.warning(
                    f"⚠️ TP skipped for {self.trade_uid}: "
                    f"invalid entry_spot={entry_spot}"
                )
                return

            tp_threshold_points = (
                entry_spot
                * self.tp_bps
                / 10000.0
            )

            logger.info(
                f"🎯 TP Check | "
                f"Trade={self.trade_uid} | "
                f"TP BPS={self.tp_bps:.4f} | "
                f"EntrySpot={entry_spot:.2f} | "
                f"Target={tp_threshold_points:.4f} | "
                f"PnL/Straddle={pnl_per_straddle:.4f}"
            )

            if (
                pnl_per_straddle
                >= tp_threshold_points
            ):

                logger.warning(
                    f"✅ TAKE PROFIT TRIGGERED: "
                    f"{self.trade_uid} | "
                    f"PnL/Straddle=₹"
                    f"{pnl_per_straddle:.2f} >= "
                    f"TP Target=₹"
                    f"{tp_threshold_points:.2f} | "
                    f"TP BPS={self.tp_bps:.4f}"
                )

                self.triggered = True

                await get_event_bus().emit(
                    event_type="partial_square_off_needed",
                    trade_uid=self.trade_uid,
                    priority=EventPriority.SQUARE_OFF,
                    data={
                        'percentage':
                            self.tp_sqf_percentage
                    }
                )

                await self.stop()

            else:

                logger.info(
                    f"🎯 TP Check OK for "
                    f"{self.trade_uid}: "
                    f"PnL/Straddle ₹"
                    f"{pnl_per_straddle:.2f} < "
                    f"TP Target ₹"
                    f"{tp_threshold_points:.2f}"
                )

        except Exception as e:
            logger.error(f"❌ TPMonitor check error for {self.trade_uid}: {e}", exc_info=True)
