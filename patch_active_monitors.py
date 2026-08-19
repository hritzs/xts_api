import ast
import py_compile
from pathlib import Path
import re

tm_path = Path('trading/trade_manager.py')
if not tm_path.exists():
    print("❌ trading/trade_manager.py not found")
    raise SystemExit(1)

txt = tm_path.read_text(encoding='utf-8')

# The exact clean function we want
clean_function = '''    async def run_condition_tasks(self):
        """
        Main recurring check loop called continuously by the trade process worker.
        Executes on every cycle (0.1s to 1s).
        """
        if not self.is_running:
            return

        try:
            # 1. Fetch latest trade state from DB
            trade_data = self.db.get_straddle_by_id(self.trade_uid)
            if not trade_data:
                return

            status = trade_data.get("status")

            # 2. PENDING_ENTRY: Auto-start & check EntryStraddleMonitor
            if status == "PENDING_ENTRY":
                mon = getattr(self, 'entry_straddle_monitor', None)
                if mon:
                    if hasattr(mon, 'start') and not getattr(mon, 'running', False):
                        from utils.logger import logger
                        logger.info(f"[{self.trade_uid}] 🚀 Starting entry_straddle_monitor...")
                        import asyncio
                        asyncio.create_task(mon.start())
                    await mon.check()

            # 3. ACTIVE / EXECUTED: Auto-start & check ALL active monitors
            elif status in {
                "ACTIVE",
                "PARTIAL",
                "SQUARING-OFF",
                "PARTIAL-SQF",
                "HEDGING",
                "ROLLING",
                "BUILDING",
            }:
                active_monitors = [
                    'sl_monitor',
                    'hedge_monitor',
                    'roll_monitor',
                    'square_off_monitor',
                    'straddle_price_monitor',
                    'tp_monitor',
                ]
                for mon_name in active_monitors:
                    mon = getattr(self, mon_name, None)
                    if mon and hasattr(mon, 'start') and not getattr(mon, 'running', False):
                        from utils.logger import logger
                        logger.info(f"[{self.trade_uid}] 🚀 Starting {mon_name}...")
                        import asyncio
                        asyncio.create_task(mon.start())

                # Execute individual active monitor checks
                if getattr(self, 'sl_monitor', None): await self.sl_monitor.check()
                if getattr(self, 'hedge_monitor', None): await self.hedge_monitor.check()
                if getattr(self, 'roll_monitor', None): await self.roll_monitor.check()
                if getattr(self, 'square_off_monitor', None): await self.square_off_monitor.check()
                if getattr(self, 'straddle_price_monitor', None): await self.straddle_price_monitor.check()
                if getattr(self, 'tp_monitor', None): await self.tp_monitor.check()

        except Exception as e:
            from utils.logger import logger
            logger.error(f"[{self.trade_uid}] Error in run_condition_tasks: {e}", exc_info=True)
'''

# Use regex to find the start of run_condition_tasks and the start of the *next* method
pattern = re.compile(r"([ \t]+async\s+def\s+run_condition_tasks\(self\):.*?)(?=\n[ \t]+(?:async\s+)?def\s+)", re.DOTALL)

# Replace it
new_txt, count = pattern.subn(clean_function, txt, count=1)

if count > 0:
    tm_path.write_text(new_txt, encoding='utf-8')
    try:
        ast.parse(new_txt)
        py_compile.compile(str(tm_path), doraise=True)
        print("✅ trade_manager.py patched and compiled successfully!")
    except Exception as e:
        print(f"❌ Syntax Error: {e}")
else:
    print("❌ Could not find run_condition_tasks to replace in trade_manager.py")

