from pathlib import Path

path = Path("trading/monitors/entry_straddle_monitor.py")
if path.exists():
    new_content = '''import time
from typing import Dict
from utils.logger import logger
from models.state import state
from utils.helpers import get_ist_now, _safe_float
from trading.builder import execute_prepared_build

class EntryStraddleMonitor:
    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid = trade_uid
        self.config = config
        self.running = False
        self.target_premium = 0.0
        
        entry_target = self.config.get("entry_at_straddle")
        if entry_target not in ("", None, 0, "0"):
            try:
                self.target_premium = float(entry_target)
            except Exception:
                pass

        # 60-second polling interval for entry checks
        self.interval = float(config.get('entry_monitor_interval', 60))
        self._last_check_time = 0.0
        logger.info(f"✅ EntryStraddleMonitor initialized: {trade_uid} | Target: {self.target_premium} | Interval: {self.interval}s")

    async def start(self):
        self.running = True
        self._last_check_time = 0.0  # Force immediate check on first loop
        logger.info(f"🎯 EntryStraddleMonitor enabled for {self.trade_uid}")

    async def stop(self):
        self.running = False
        logger.info(f"🛑 EntryStraddleMonitor stopped for {self.trade_uid}")

    async def check(self):
        if not self.running:
            return

        # If no target premium is set, execute immediately
        if self.target_premium <= 0:
            logger.info(f"[ENTRY STRADDLE] {self.trade_uid}: No target configured. Executing immediately.")
            await execute_prepared_build(self.trade_uid)
            await self.stop()
            return

        # ── Gate: Interval (Run immediately first time, then every interval) ────────────────
        now_mono = time.monotonic()
        if self._last_check_time > 0 and (now_mono - self._last_check_time < self.interval):
            return

        self._last_check_time = now_mono

        trade_data = state.db.get_straddle_by_id(self.trade_uid)
        if not trade_data or trade_data.get('status') != 'PENDING_ENTRY':
            await self.stop()
            return

        symbol = trade_data.get("symbol")
        chain_data = state.get_published_option_chain(symbol)
        if not chain_data:
            logger.warning(f"[{self.trade_uid}] [ENTRY POLL] No published chain available for {symbol}. Waiting...")
            return

        atm = chain_data.get("atm")
        if not atm:
            return

        atm_row = next((r for r in chain_data.get("chain", []) if r.get("strike") == atm), None)
        if not atm_row:
            return

        ce_ltp = _safe_float(atm_row.get("ce_ltp", 0.0))
        pe_ltp = _safe_float(atm_row.get("pe_ltp", 0.0))
        current_straddle = ce_ltp + pe_ltp

        logger.info("=" * 100)
        logger.info(f"⏳ [{self.trade_uid}] [ENTRY CHECK]")
        logger.info(f"Current Straddle : {current_straddle:.2f}")
        logger.info(f"Target Straddle  : {self.target_premium:.2f}")
        logger.info("=" * 100)

        # Entry Condition: Trigger when current straddle hits or drops below the target (or your specific threshold logic)
        # Adjust comparison operator if your strategy requires >= or <=
        if current_straddle <= self.target_premium:
            logger.info(f"✅ [ENTRY HIT] {self.trade_uid} target reached ({current_straddle:.2f} <= {self.target_premium:.2f}). Executing build.")
            await execute_prepared_build(self.trade_uid)
            await self.stop()
        else:
            logger.info(f"⏳ [WAITING FOR ENTRY] Current ({current_straddle:.2f}) > Target ({self.target_premium:.2f})")
'''
    path.write_text(new_content, encoding="utf-8")
    print("✅ Successfully patched trading/monitors/entry_straddle_monitor.py")
else:
    print("❌ entry_straddle_monitor.py not found.")
