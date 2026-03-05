"""
Stop-Loss Monitor with Initial Delay
"""
import asyncio
from datetime import datetime, timedelta
from utils.logger import logger
from models.state import state
from trading.pnl_calculator import calculate_aggregate_pnl
from trading.event_bus import get_event_bus, EventPriority
from utils.helpers import get_ist_now

class SLMonitor:
    """
    Monitors the PnL of a trade and triggers a stop-loss event if the
    loss threshold is breached. Includes an initial delay.
    """
    def __init__(self, trade_uid: str, config: dict):
        self.trade_uid = trade_uid
        self.config = config
        self.running = False
        self.event_bus = get_event_bus()

        self.sl_bps = config.get('sl_bps', 14)
        self.sl_monitor_interval = config.get('sl_monitor_interval', 60)
        self.sl_points = 0.0
        
        logger.info(f"✅ SLMonitor initialized: {self.trade_uid}")
        logger.info(f"   SL BPS: {self.sl_bps} ({self.sl_bps/10000:.2%}) of spot price")
        logger.info(f"   Check Interval: {self.sl_monitor_interval}s")

    async def start(self):
        """Enables the monitor to be checked by the orchestrator."""
        if self.running:
            logger.info(f"🛡️ SLMonitor for {self.trade_uid} is already running.")
            return
        self.running = True
        logger.info(f"🛡️ SLMonitor enabled: {self.trade_uid}")

    async def stop(self):
        """Disables the monitor."""
        if not self.running:
            return
        self.running = False
        logger.info(f"🛑 SLMonitor disabled: {self.trade_uid}")

    async def check(self):
        """
        Performs a single stop-loss check. Called by the TradeManager orchestrator.
        """
        try:
            snapshot_time = get_ist_now()
            logger.info(f"🛡️ SL Check at {snapshot_time.strftime('%H:%M:%S')} for {self.trade_uid}")

            # --- Get latest data from snapshot ---
            snapshot = state.trade_snapshots.get(self.trade_uid)
            if not snapshot:
                logger.warning(f"SLMonitor: No snapshot available for {self.trade_uid}. Skipping check.")
                return

            # --- Log positions used for PnL calculation ---
            if snapshot.get('live_positions'):
                logger.info(f"--- Positions used for SL Check ({self.trade_uid}) ---")
                sorted_positions = sorted(snapshot['live_positions'], key=lambda p: (p.get('strike', 0), p.get('option_type', '')))
                for pos in sorted_positions:
                    logger.info(
                        f"  - {pos.get('action', 'N/A')} {pos.get('quantity', 0)} {pos.get('option_type', '')} {pos.get('strike', 0)} "
                        f"| Entry: {pos.get('entry_price', 0):.2f} | LTP: {pos.get('ltp', 0):.2f} | PnL: ₹{pos.get('pnl', 0):.2f}"
                    )
                logger.info("-----------------------------------------------------")
            else:
                logger.info("--- No live positions found in snapshot for SL Check ---")
            # --- END LOGGING ---

            # Run synchronous DB call in an executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            db_trade = await loop.run_in_executor(
                None, state.db.get_straddle_by_id, self.trade_uid
            )
            if not db_trade or db_trade.get('status') != 'ACTIVE':
                logger.warning(f"SLMonitor: Trade {self.trade_uid} not active or not found. Stopping.")
                await self.stop()
                return

            # --- NEW SL LOGIC: Per-straddle PnL vs Per-straddle SL from spot ---
            # FIX: Use the robust PnL/Straddle calculated in the snapshot (based on net open quantity)
            # instead of re-calculating it here with potentially stale 'total_contracts'.
            pnl_per_straddle = snapshot.get('pnl_per_straddle', 0.0)
            live_spot_price = snapshot.get('spot_price', 0.0)

            if live_spot_price <= 0:
                logger.warning(f"SLMonitor: Invalid spot price for {self.trade_uid} ({live_spot_price}). Skipping check.")
                return

            # Calculate SL threshold per straddle (in points/currency) based on spot
            sl_points_per_straddle = (live_spot_price * self.sl_bps) / 10000
            sl_threshold_per_straddle = -1 * sl_points_per_straddle

            # Update sl_points for UI display
            self.sl_points = sl_points_per_straddle

            logger.info(f"🛡️  SL Params for {self.trade_uid}:")
            logger.info(f"   - PnL/Straddle: ₹{pnl_per_straddle:.2f}")
            logger.info(f"   - SL Threshold/Straddle: ₹{sl_threshold_per_straddle:.2f}")

            if pnl_per_straddle <= sl_threshold_per_straddle:
                logger.warning(f"🚨 STOP-LOSS HIT: {self.trade_uid} | PnL/Straddle: ₹{pnl_per_straddle:.2f} <= Threshold/Straddle: ₹{sl_threshold_per_straddle:.2f}")
                await self.event_bus.emit(
                    event_type="sl_triggered", 
                    trade_uid=self.trade_uid, 
                    priority=EventPriority.STOP_LOSS,
                )
                await self.stop() # Stop after triggering
            else:
                logger.info(f"🛡️ SL Check OK: PnL is above threshold.")
        except Exception as e:
            logger.error(f"❌ SLMonitor check error for {self.trade_uid}: {e}", exc_info=True)