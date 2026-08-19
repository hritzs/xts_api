import re
from pathlib import Path

p = Path("trading/monitors/tp_monitor.py")
if p.exists():
    txt = p.read_text(encoding='utf-8')
    
    # Ensure we don't double-patch
    if "_last_check_minute" not in txt:
        pattern = r"(async\s+def\s+check\s*\(\s*self\s*\)\s*:)"
        replacement = r'''\1
        import datetime
        now = datetime.datetime.now()
        
        # 1. Only execute at the end of the minute (e.g., 58s or 59s)
        if now.second < 58:
            return
            
        # 2. Ensure it strictly runs only ONCE per minute
        current_minute = now.minute
        if getattr(self, '_last_check_minute', -1) == current_minute:
            return
        self._last_check_minute = current_minute
'''
        new_txt, count = re.subn(pattern, replacement, txt, count=1)
        
        if count > 0:
            p.write_text(new_txt, encoding='utf-8')
            print("✅ TPMonitor successfully patched to run ONLY at minute end (>= 58s).")
        else:
            print("❌ Could not find 'async def check(self):' in tp_monitor.py")
    else:
        print("⚡ TPMonitor is already patched for minute-end throttling.")
else:
    print("❌ trading/monitors/tp_monitor.py not found.")
