from pathlib import Path

# 1. Instrument trade_manager.py
tm_path = Path("trading/trade_manager.py")
if tm_path.exists():
    content = tm_path.read_text(encoding="utf-8")
    
    old_run = '''        if status == "PENDING_ENTRY":
            await self.entry_straddle_monitor.check()'''
            
    new_run = '''        logger.info(f"[RUN LOOP] {self.trade_uid}")
        if status == "PENDING_ENTRY":
            logger.info(f"[CALL ENTRY CHECK] {self.trade_uid} status={status}")
            await self.entry_straddle_monitor.check()'''
            
    if old_run in content and "[RUN LOOP]" not in content:
        content = content.replace(old_run, new_run, 1)
        tm_path.write_text(content, encoding="utf-8")
        print("✅ Instrumented trade_manager.py run_condition_tasks")
    else:
        print("⚡ trade_manager loop already instrumented or structure differs.")

# 2. Instrument entry_straddle_monitor.py
mon_path = Path("trading/monitors/entry_straddle_monitor.py")
if mon_path.exists():
    code = '''import time
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

        self.interval = float(config.get('entry_monitor_interval', 60))
        self._last_check_time = 0.0
        logger.info(f"✅ EntryStraddleMonitor initialized: {trade_uid} | Target: {self.target_premium} | Interval: {self.interval}s")

    async def start(self):
        self.running = True
        self._last_check_time = 0.0
        logger.info(f"🎯 EntryStraddleMonitor enabled for {self.trade_uid}")

    async def stop(self):
        self.running = False
        logger.info(f"🛑 EntryStraddleMonitor stopped for {self.trade_uid}")

    async def check(self):
        logger.info(f"[ENTRY CHECK EXECUTING] {self.trade_uid} running={self.running} target={self.target_premium}")
        
        if not self.running:
            logger.warning(f"[ENTRY] Monitor not running for {self.trade_uid}")
            return

        if self.target_premium <= 0:
            logger.warning(f"[ENTRY] Invalid target premium {self.target_premium}")
            await execute_prepared_build(self.trade_uid)
            await self.stop()
            return

        now_mono = time.monotonic()
        if self._last_check_time > 0 and (now_mono - self._last_check_time < self.interval):
            return

        self._last_check_time = now_mono

        trade_data = state.db.get_straddle_by_id(self.trade_uid)
        if not trade_data or trade_data.get('status') != 'PENDING_ENTRY':
            logger.info(f"[ENTRY] Trade {self.trade_uid} status changed or not found. Stopping.")
            await self.stop()
            return

        symbol = trade_data.get("symbol")
        chain_data = state.get_published_option_chain(symbol)
        
        logger.info(f"[ENTRY] Chain={'FOUND' if chain_data else 'NONE'}")
        
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
        logger.info(f"[ENTRY] Premium={current_straddle:.2f} Target={self.target_premium:.2f}")
        logger.info("=" * 100)

        if current_straddle <= self.target_premium:
            logger.info(f"[ENTRY HIT] {self.trade_uid}")
            logger.info(f"✅ [ENTRY HIT] Target reached ({current_straddle:.2f} <= {self.target_premium:.2f}). Executing build.")
            await execute_prepared_build(self.trade_uid)
            await self.stop()
        else:
            logger.info(f"⏳ [WAITING FOR ENTRY] Current ({current_straddle:.2f}) > Target ({self.target_premium:.2f})")
'''
    mon_path.write_text(code, encoding="utf-8")
    print("✅ Successfully updated entry_straddle_monitor.py with full trace logging!")
else:
    print("❌ entry_straddle_monitor.py not found.")
