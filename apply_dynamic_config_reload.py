from pathlib import Path
import py_compile
import shutil
from datetime import datetime

path = Path("trading/trade_manager.py")

if not path.exists():
    print("ERROR: trading/trade_manager.py not found.")
    raise SystemExit(1)

# First make sure the CURRENT file is valid.
try:
    py_compile.compile(str(path), doraise=True)
except Exception as e:
    print("ERROR: trade_manager.py is already syntactically broken.")
    print(e)
    print("NO CHANGES WERE MADE.")
    raise SystemExit(2)

code = path.read_text(encoding="utf-8")

marker = "# ── DYNAMIC POST-EXECUTION CONFIG RE-READ ──"

if marker in code:
    print("OK: Dynamic post-execution config reload is already present.")
    raise SystemExit(0)

# Locate run_condition_tasks() only.
func_start = code.find("    async def run_condition_tasks(self):")

if func_start == -1:
    print("ERROR: Could not find run_condition_tasks().")
    raise SystemExit(3)

# Find the next class method so we only modify run_condition_tasks().
next_async = code.find("\n    async def ", func_start + 10)
next_sync = code.find("\n    def ", func_start + 10)

candidates = [x for x in (next_async, next_sync) if x != -1]
func_end = min(candidates) if candidates else len(code)

func = code[func_start:func_end]

old = "trade_data = self.db.get_straddle_by_id(self.trade_uid)"

if old not in func:
    print("ERROR: Could not find the trade_data DB fetch inside run_condition_tasks().")
    raise SystemExit(4)

injection = """trade_data = self.db.get_straddle_by_id(self.trade_uid)

            # ── DYNAMIC POST-EXECUTION CONFIG RE-READ ──
            # Reload entry/TP targets from DB for the running monitors.
            fresh_trade_data = self.db.get_straddle_by_id(self.trade_uid)

            if fresh_trade_data:
                if (
                    'target_entry_price' in fresh_trade_data
                    and fresh_trade_data['target_entry_price'] is not None
                ):
                    if getattr(self, 'entry_straddle_monitor', None):
                        self.entry_straddle_monitor.target = float(
                            fresh_trade_data['target_entry_price']
                        )

                if (
                    'target_exit_price' in fresh_trade_data
                    and fresh_trade_data['target_exit_price'] is not None
                ):
                    if getattr(self, 'tp_monitor', None):
                        self.tp_monitor.target = float(
                            fresh_trade_data['target_exit_price']
                        )
"""

new_func = func.replace(old, injection, 1)

new_code = code[:func_start] + new_func + code[func_end:]

# Create backup BEFORE modifying the real file.
stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup = path.with_name(
    f"{path.stem}.backup_{stamp}{path.suffix}"
)

shutil.copy2(path, backup)
print(f"Backup created: {backup}")

# Apply patch.
path.write_text(new_code, encoding="utf-8")

# Verify syntax.
try:
    py_compile.compile(str(path), doraise=True)
except Exception as e:
    print("ERROR: Patch introduced a syntax error.")
    print(e)

    # Automatically restore original.
    shutil.copy2(backup, path)

    print("Original trade_manager.py RESTORED.")
    raise SystemExit(5)

print("SUCCESS: Dynamic post-execution config reload added.")
print("SUCCESS: trade_manager.py compiles successfully.")
print(f"Backup: {backup}")
