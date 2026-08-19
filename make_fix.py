import ast
import py_compile
from pathlib import Path

tm_path = Path('trading/trade_manager.py')
if not tm_path.exists():
    print('❌ trading/trade_manager.py not found')
    raise SystemExit(1)

txt = tm_path.read_text(encoding='utf-8')

# Clean python function with explicit standard 4/8-space indentation
clean_function = '''    async def run_condition_tasks(self):
        \"\"\"
        Main recurring check loop called continuously by the trade process worker.
        \"\"\"
        if not self.is_running:
            return

        try:
            # 1. Fetch latest trade state from DB
            trade_data = self.db.get_straddle_by_id(self.trade_uid)
            if not trade_data:
                return

            # ── DYNAMIC POST-EXECUTION CONFIG RE-READ ──
            fresh_trade_data = self.db.get_straddle_by_id(self.trade_uid)
            if fresh_trade_data:
                if 'target_entry_price' in fresh_trade_data and fresh_trade_data['target_entry_price'] is not None:
                    if getattr(self, 'entry_straddle_monitor', None):
                        self.entry_straddle_monitor.target = float(fresh_trade_data['target_entry_price'])
                if 'target_exit_price' in fresh_trade_data and fresh_trade_data['target_exit_price'] is not None:
                    if getattr(self, 'tp_monitor', None):
                        self.tp_monitor.target = float(fresh_trade_data['target_exit_price'])

            status = trade_data.get("status")

            # 2. PENDING_ENTRY
            if status == "PENDING_ENTRY":
                mon = getattr(self, 'entry_straddle_monitor', None)
                if mon:
                    if hasattr(mon, 'start') and not getattr(mon, 'running', False):
                        from utils.logger import logger
                        logger.info(f"[{self.trade_uid}] 🚀 Starting entry_straddle_monitor...")
                        import asyncio
                        asyncio.create_task(mon.start())
                    await mon.check()

            # 3. ACTIVE / EXECUTED
            elif status in {
                "ACTIVE",
                "PARTIAL",
                "SQUARING-OFF",
                "PARTIAL-SQF",
                "HEDGING",
                "ROLLING",
                "BUILDING",
            }:
                import time
                import asyncio
                import httpx
                
                # ── THROTTLED INLINE SNAPSHOT FETCH ──
                current_time = time.time()
                last_fetch = getattr(self, '_last_snapshot_fetch_time', 0)
                
                if current_time - last_fetch >= 1.0:
                    if not getattr(self, '_http_client', None):
                        self._http_client = httpx.AsyncClient()
                    
                    try:
                        url = f"http://localhost:8003/api/snapshots/{self.trade_uid}"
                        resp = await self._http_client.get(url, timeout=2.0)
                        if resp.status_code == 200:
                            data = resp.json()
                            self.latest_snapshot = data.get("data", data)
                            
                            # Feed global state for monitors
                            try:
                                from models.state import state
                                if not hasattr(state, 'trade_snapshots'):
                                    state.trade_snapshots = {}
                                state.trade_snapshots[self.trade_uid] = self.latest_snapshot
                            except Exception:
                                pass
                    except Exception:
                        pass
                    finally:
                        self._last_snapshot_fetch_time = current_time
                
                if not getattr(self, 'latest_snapshot', None):
                    return

                # ── Auto-Boot Active Monitors ──
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
                        asyncio.create_task(mon.start())

                # ── Execute Monitor Checks ──
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

start_str = "    async def run_condition_tasks(self):"
start_idx = txt.find(start_str)

if start_idx == -1:
    print("❌ Could not find start of run_condition_tasks in trade_manager.py")
    raise SystemExit(1)

next_def_idx = txt.find("    def ", start_idx + 10)
next_async_def_idx = txt.find("    async def ", start_idx + 10)

candidates = [i for i in (next_def_idx, next_async_def_idx) if i != -1]
end_idx = min(candidates) if candidates else len(txt)

new_txt = txt[:start_idx] + clean_function + "\n" + txt[end_idx:]

tm_path.write_text(new_txt, encoding='utf-8')

try:
    ast.parse(new_txt)
    py_compile.compile(str(tm_path), doraise=True)
    print("✅ trade_manager.py successfully fixed and compiled with dynamic price reloading!")
except Exception as e:
    print(f"❌ Syntax Error: {e}")
