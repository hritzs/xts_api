"""
Roll Monitor - Start Time + Interval gates, lazy event_bus
"""
import time
from datetime import datetime
import asyncio
from typing import Dict
from utils.logger import logger
from trading.event_bus import get_event_bus, EventPriority
from models.state import state
from utils.helpers import get_ist_now
from market_data import SYMBOL_CONFIG


class RollMonitor:
    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid                = trade_uid
        self.config                   = config
        self.interval                 = float(config.get('roll_monitor_interval', 60))
        self.roll_flag_check_interval = config.get('roll_flag_check_interval', 60)
        self.roll_straddle_div        = float(config.get('roll_straddle_div', 2.0))
        self.roll_start_time_str      = config.get('roll_start_time')  # "HH:MM:SS"
        self.running                  = False
        self._last_check_time         = 0.0

        logger.info(f"✅ RollMonitor initialized: {self.trade_uid}")
        logger.info(f"   Interval: {self.interval}s | Start: {self.roll_start_time_str} | Div: {self.roll_straddle_div}")

    @property
    def event_bus(self):
        return get_event_bus()

    async def start(self):
        if self.running:
            return
        self.running = True

        try:
            now = get_ist_now()
            parts = list(map(int, self.roll_start_time_str.split(':')))
            configured_start = now.replace(
                hour=parts[0],
                minute=parts[1],
                second=parts[2] if len(parts) > 2 else 0,
                microsecond=0
            )
            elapsed = (now - configured_start).total_seconds()
            
            if elapsed < 0:
                self._last_check_time = time.monotonic() - self.interval
            else:
                self._last_check_time = time.monotonic() - (elapsed % self.interval)
        except Exception:
            self._last_check_time = time.monotonic()
            elapsed = 0.0

        try:
            delay = self.interval - (elapsed % self.interval) if elapsed >= 0 else -elapsed
        except Exception:
            delay = self.interval

        logger.info(
            f"🔄 RollMonitor enabled: {self.trade_uid} | "
            f"First check in {delay:.0f}s"
        )

    async def stop(self):
        if not self.running:
            return
        self.running = False
        logger.info(f"🛑 RollMonitor disabled: {self.trade_uid}")

    async def check(self):
        if not self.running:
            return

        # ── Gate 1: Start time ────────────────────────────────────────────────
        if self.roll_start_time_str:
            now_time = get_ist_now().time()
            try:
                from datetime import time as dt_time
                parts = list(map(int, self.roll_start_time_str.split(':')))
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
            logger.info(f"🔄 Roll Check at {get_ist_now().strftime('%H:%M:%S')} for {self.trade_uid}")

            loop = asyncio.get_event_loop()
            trade = await loop.run_in_executor(None, state.db.get_straddle_by_id, self.trade_uid)
            if not trade or trade.get('status') != 'ACTIVE':
                logger.info(f"✅ Trade not active, stopping roll monitor")
                await self.stop()
                return

            snapshot = state.trade_snapshots.get(self.trade_uid)
            if not snapshot:
                logger.warning(f"RollMonitor: No snapshot available for {self.trade_uid}. Skipping check.")
                return

            if snapshot.get('live_positions'):
                logger.info(f"--- Positions used for Roll Check ({self.trade_uid}) ---")
                for pos in sorted(snapshot['live_positions'], key=lambda p: (p.get('strike', 0), p.get('option_type', ''))):
                    logger.info(
                        f"  - {pos.get('action', 'N/A')} {pos.get('quantity', 0)} {pos.get('option_type', '')} {pos.get('strike', 0)} "
                        f"| LTP: {pos.get('ltp', 0):.2f}"
                    )
                logger.info("-----------------------------------------------------")
            else:
                logger.info("--- No live positions found in snapshot for Roll Check ---")

            # --- FIX: Handle straddles vs strangles for entry_strike calculation ---
            strike_val = trade.get("strike", "0.0")
            entry_strike = 0.0

            if isinstance(strike_val, str) and '/' in strike_val:
                # For strangles, use the ATM at the time of entry as the center.
                entry_spot = trade.get("entry_spot")
                symbol = trade.get("symbol", "").upper()
                base_symbol = next((k for k in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if k in symbol), None)
                
                if entry_spot and base_symbol and base_symbol in SYMBOL_CONFIG:
                    try:
                        gap = SYMBOL_CONFIG[base_symbol]['gap']
                        entry_strike = round(float(entry_spot) / gap) * gap
                        logger.info(f"Roll check for strangle: using entry ATM {entry_strike} derived from entry_spot {entry_spot}.")
                    except (ValueError, TypeError, KeyError) as e:
                        logger.warning(f"Could not calculate entry ATM for strangle {self.trade_uid}: {e}. Falling back to midpoint.")
                        try:
                            pe_s, ce_s = map(float, strike_val.split('/'))
                            entry_strike = (pe_s + ce_s) / 2.0
                        except (ValueError, TypeError):
                            logger.error(f"Could not parse strangle strike '{strike_val}' for fallback. Using 0.")
                else:
                    logger.warning(f"Strangle {self.trade_uid} missing entry_spot or symbol. Falling back to midpoint.")
                    try:
                        pe_s, ce_s = map(float, strike_val.split('/'))
                        entry_strike = (pe_s + ce_s) / 2.0
                    except (ValueError, TypeError):
                        logger.error(f"Could not parse strangle strike '{strike_val}' for fallback. Using 0.")
            else:
                entry_strike = float(strike_val)

            spot_price    = float(snapshot.get("spot_price", 0.0))
            roll_distance = abs(spot_price - entry_strike) if entry_strike > 0 and spot_price > 0 else 0

            symbol       = trade.get('symbol', 'NIFTY').upper()
            option_chain = state.option_chains.get(symbol)
            atm_straddle = 0.0
            if option_chain:
                atm_row = next((r for r in option_chain['chain'] if r['is_atm']), None)
                if atm_row:
                    atm_straddle = atm_row.get('ce_ltp', 0) + atm_row.get('pe_ltp', 0)

            roll_threshold = (
                atm_straddle / self.roll_straddle_div
                if atm_straddle > 0 and self.roll_straddle_div > 0
                else float('inf')
            )

            from market_data import SYMBOL_CONFIG
            base_symbol = next(
                (k for k in sorted(SYMBOL_CONFIG.keys(), key=len, reverse=True) if k in symbol),
                'NIFTY'
            )
            min_roll_threshold   = SYMBOL_CONFIG.get(base_symbol, {}).get('min_roll_threshold', 30)
            final_roll_threshold = max(roll_threshold, min_roll_threshold)

            logger.info(f"🔄 Roll Params for {self.trade_uid}:")
            logger.info(f"   - Spot Price: {spot_price:.2f}")
            logger.info(f"   - Entry Strike: {entry_strike:.2f}")
            logger.info(f"   - Strike Distance: {roll_distance:.2f}")
            logger.info(f"   - Roll Threshold: {final_roll_threshold:.2f} (Condition: Distance > Threshold)")

            if roll_distance > final_roll_threshold:
                logger.warning(
                    f"🔄 ROLL NEEDED (Strike Distance): {self.trade_uid} | "
                    f"Distance {roll_distance:.2f} > {final_roll_threshold:.2f}"
                )
                eb = self.event_bus
                if eb is None:
                    logger.error(f"❌ RollMonitor: event_bus is None for {self.trade_uid}")
                    return
                await eb.emit(
                    event_type="roll_needed",
                    trade_uid=self.trade_uid,
                    priority=EventPriority.ROLL,
                    data={'reason': 'Strike Distance'}
                )
            else:
                logger.info(f"🔄 Roll Check OK: Conditions not met.")

        except Exception as e:
            logger.error(f"❌ RollMonitor check error for {self.trade_uid}: {e}", exc_info=True)
