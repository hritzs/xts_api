import os

fpath = 'trading/monitors/entry_straddle_monitor.py'
os.makedirs(os.path.dirname(fpath), exist_ok=True)

new_monitor_code = '''import asyncio
from utils.logger import logger
from models.state import state
from utils.helpers import get_ist_now
from trading.builder import execute_prepared_build

class EntryStraddleMonitor:
    def __init__(self, trade_uid: str, config: dict):
        self.trade_uid = trade_uid
        self.config = config
        self.running = False
        self.target_premium = 0.0
        
        entry_target = self.config.get("entry_at_straddle")
        if entry_target not in ("", None, 0, "0"):
            try:
                self.target_premium = float(entry_target)
            except:
                pass
        self.last_checked_minute = None

    async def start(self):
        if self.target_premium > 0:
            self.running = True
            # Set to NOW so it waits for the next exact minute rollover to poll
            self.last_checked_minute = get_ist_now().replace(second=0, microsecond=0)
            logger.info(f"✅ EntryStraddleMonitor started for {self.trade_uid} | Target: {self.target_premium}")

    async def stop(self):
        self.running = False
        logger.info(f"🛑 EntryStraddleMonitor stopped for {self.trade_uid}")

    async def check(self):
        if not self.running or self.target_premium <= 0:
            return

        now = get_ist_now()
        current_minute = now.replace(second=0, microsecond=0)
        
        # Fire exactly once per minute when the minute rolls over
        if self.last_checked_minute == current_minute:
            return

        self.last_checked_minute = current_minute

        # 1. Verify trade is still in PENDING_ENTRY
        trade_data = state.db.get_straddle_by_id(self.trade_uid)
        if not trade_data or trade_data.get('status') != 'PENDING_ENTRY':
            await self.stop()
            return

        # 2. Get live premium from published chain
        symbol = trade_data.get("symbol")
        chain_data = state.get_published_option_chain(symbol)
        if not chain_data:
            logger.warning(f"[{self.trade_uid}] [MINUTE POLL] No published chain available for {symbol}. Waiting...")
            return

        atm = chain_data.get("atm")
        if not atm:
            return
            
        atm_row = next((r for r in chain_data.get("chain", []) if r.get("strike") == atm), None)
        if not atm_row:
            return

        ce_ltp = float(atm_row.get("ce_ltp", 0.0))
        pe_ltp = float(atm_row.get("pe_ltp", 0.0))
        current_straddle = ce_ltp + pe_ltp

        # 3. Log the check
        logger.info("-" * 80)
        logger.info(f"⏳ [{self.trade_uid}] [MINUTE POLL] Checking Entry Conditions...")
        logger.info(f"   Live ATM        : {atm}")
        logger.info(f"   Live Premium    : {current_straddle:.2f}")
        logger.info(f"   Target Premium  : {self.target_premium:.2f}")
        logger.info("-" * 80)

        # 4. Trigger build if target met
        if current_straddle >= self.target_premium:
            logger.info(f"🎯 [{self.trade_uid}] TARGET MET ({current_straddle:.2f} >= {self.target_premium:.2f})! Handing off to execution.")
            await self.stop()
            # Spawn task so we do not block the worker loop
            asyncio.create_task(execute_prepared_build(self.trade_uid))
'''

with open(fpath, 'w', encoding='utf-8') as f:
    f.write(new_monitor_code)

print("Successfully replaced trading/monitors/entry_straddle_monitor.py with the minute-poll visualizer!")
