import py_compile
from pathlib import Path
import re

tm_path = Path('trading/trade_manager.py')
if not tm_path.exists():
    print("❌ trading/trade_manager.py not found")
    raise SystemExit(1)

txt = tm_path.read_text(encoding='utf-8')

# Target the corrupted snapshot fetch call that is missing 'await'
target_pattern = r"snapshot_data\s*=\s*_get_snapshot_from_service\(self\.trade_uid\)"
replacement = r"snapshot_data = await _get_snapshot_from_service(self.trade_uid)"

if "await _get_snapshot_from_service" not in txt:
    new_txt, count = re.subn(target_pattern, replacement, txt)
    if count > 0:
        tm_path.write_text(new_txt, encoding='utf-8')
        try:
            py_compile.compile(str(tm_path), doraise=True)
            print("✅ Successfully patched TradeManager to await the snapshot data!")
        except Exception as e:
            print(f"❌ Syntax Error after replacement: {e}")
    else:
        print("❌ Could not find the snapshot fetch call. Was it removed or reformatted?")
else:
    print("⚡ The snapshot fetch call is already awaited.")

