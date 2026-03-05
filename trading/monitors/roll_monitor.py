"""
Roll Monitor
Checks if position needs to be rolled to next expiry
Runs at configured interval (roll_monitor_interval)
"""
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Optional
from utils.logger import logger
from trading.event_bus import get_event_bus, EventPriority
from models.state import state
from utils.helpers import get_ist_now


class RollMonitor:
    """
    🔄 ROLL MONITOR
    
    Features:
    - Monitors days to expiry
    - Triggers roll event when threshold reached
    - Runs at configured interval
    """
    
    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid = trade_uid
        self.config = config
        self.roll_monitor_interval = config.get('roll_monitor_interval', 60)
        self.roll_flag_check_interval = config['roll_flag_check_interval']
        self.roll_straddle_div = config.get('roll_straddle_div', 2.0)
        self.running = False
        
        logger.info(f"✅ RollMonitor initialized: {trade_uid}")
        logger.info(f"   Check Interval: {self.roll_monitor_interval}s")
    
    async def start(self):
        """Enables the monitor to be checked by the orchestrator."""
        if self.running:
            logger.info(f"🔄 RollMonitor for {self.trade_uid} is already running.")
            return
        self.running = True
        logger.info(f"🔄 RollMonitor enabled: {self.trade_uid}")

    async def stop(self):
        """Disables the monitor."""
        if not self.running:
            return
        self.running = False
        logger.info(f"🛑 RollMonitor disabled: {self.trade_uid}")

    async def check(self):
        """
        Performs a single roll check. Called by the TradeManager orchestrator.
        """
        try:
            snapshot_time = get_ist_now()
            logger.info(f"🔄 Roll Check at {snapshot_time.strftime('%H:%M:%S')} for {self.trade_uid}")

            # Run synchronous DB call in an executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            trade = await loop.run_in_executor(
                None, state.db.get_straddle_by_id, self.trade_uid
            )

            if not trade or trade.get('status') != 'ACTIVE':
                logger.info(f"✅ Trade not active, stopping roll monitor")
                await self.stop()
                return

            # --- Get latest data from snapshot ---
            snapshot = state.trade_snapshots.get(self.trade_uid)
            if not snapshot:
                logger.warning(f"RollMonitor: No snapshot available for {self.trade_uid}. Skipping check.")
                return

            # --- Log positions used for roll calculation ---
            if snapshot.get('live_positions'):
                logger.info(f"--- Positions used for Roll Check ({self.trade_uid}) ---")
                sorted_positions = sorted(snapshot['live_positions'], key=lambda p: (p.get('strike', 0), p.get('option_type', '')))
                for pos in sorted_positions:
                    logger.info(
                        f"  - {pos.get('action', 'N/A')} {pos.get('quantity', 0)} {pos.get('option_type', '')} {pos.get('strike', 0)} "
                        f"| LTP: {pos.get('ltp', 0):.2f}"
                    )
                logger.info("-----------------------------------------------------")
            else:
                logger.info("--- No live positions found in snapshot for Roll Check ---")
            # --- END LOGGING ---

            # --- Roll Condition: Strike Distance ---
            # days_to_expiry = snapshot.get('days_to_expiry', -1) # Kept for logging
            entry_strike = float(trade.get("strike", 0.0))
            spot_price = float(snapshot.get("spot_price", 0.0))
            roll_distance = abs(spot_price - entry_strike) if entry_strike > 0 and spot_price > 0 else 0

            symbol = trade.get('symbol', 'NIFTY').upper()
            option_chain = state.option_chains.get(symbol)
            atm_straddle = 0.0
            if option_chain:
                atm_row = next((row for row in option_chain['chain'] if row['is_atm']), None)
                if atm_row:
                    atm_straddle = atm_row.get('ce_ltp', 0) + atm_row.get('pe_ltp', 0)
            
            # --- SYMBOL-SPECIFIC MINIMUM ROLL THRESHOLD ---
            roll_threshold = atm_straddle / self.roll_straddle_div if atm_straddle > 0 and self.roll_straddle_div > 0 else float('inf')
            
            from market_data import SYMBOL_CONFIG
            base_symbol = next((key for key in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if key in symbol), 'NIFTY')
            min_roll_threshold = SYMBOL_CONFIG.get(base_symbol, {}).get('min_roll_threshold', 30) # Default to 30
            final_roll_threshold = max(roll_threshold, min_roll_threshold)

            needs_roll_strike = roll_distance > final_roll_threshold

            logger.info(f"🔄 Roll Params for {self.trade_uid}:")
            # logger.info(f"   - Days to Expiry (DTE): {days_to_expiry:.2f} (Roll condition disabled)")
            logger.info(f"   - Spot Price: {spot_price:.2f}")
            logger.info(f"   - Entry Strike: {entry_strike:.2f}")
            logger.info(f"   - Strike Distance: {roll_distance:.2f}")
            logger.info(f"   - Roll Threshold: {final_roll_threshold:.2f} (Condition: Distance > Threshold)")

            if needs_roll_strike:
                reason = "Strike Distance"
                logger.warning(f"🔄 ROLL NEEDED ({reason}): {self.trade_uid}")
                logger.info(f"   Strike Distance: {roll_distance:.2f} > {roll_threshold:.2f}")

                event_bus = get_event_bus()
                await event_bus.emit(event_type="roll_needed", trade_uid=self.trade_uid, priority=EventPriority.ROLL, data={'reason': reason})
            else:
                logger.info(f"🔄 Roll Check OK: Conditions not met.")
        except Exception as e:
            logger.error(f"❌ RollMonitor check error for {self.trade_uid}: {e}", exc_info=True)
