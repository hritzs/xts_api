from pathlib import Path
from datetime import datetime
import shutil
import py_compile

p = Path("trading/config_builder.py")

if not p.exists():
    raise SystemExit("ERROR: trading/config_builder.py not found.")

# ============================================================
# BACKUP
# ============================================================
stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup = p.with_name(
    f"{p.stem}.before_dt_dt_fix_{stamp}{p.suffix}"
)

shutil.copy2(p, backup)
print(f"BACKUP CREATED: {backup}")

# ============================================================
# READ
# ============================================================
code = p.read_text(encoding="utf-8")

# ============================================================
# FIX ACCIDENTAL dt.dt.*
# ============================================================
count = code.count("dt.dt.")

if count == 0:
    print("OK: No dt.dt. occurrences found.")
else:
    code = code.replace("dt.dt.", "dt.")
    print(f"FIXED: {count} occurrence(s) of dt.dt. -> dt.")

# ============================================================
# WRITE
# ============================================================
p.write_text(code, encoding="utf-8")

# ============================================================
# COMPILE
# ============================================================
try:
    py_compile.compile(
        str(p),
        doraise=True
    )
except Exception as e:
    print("")
    print("ERROR: config_builder.py failed compilation.")
    print(e)
    print("")
    print("RESTORING BACKUP...")
    shutil.copy2(backup, p)
    py_compile.compile(str(p), doraise=True)
    print("BACKUP RESTORED SUCCESSFULLY.")
    raise SystemExit(2)

# ============================================================
# VERIFY
# ============================================================
final_code = p.read_text(encoding="utf-8")

print("")
print("=" * 80)
print("DATETIME FIX COMPLETE")
print("=" * 80)

if "dt.dt." in final_code:
    print("ERROR: dt.dt. STILL EXISTS")
    raise SystemExit(3)
else:
    print("OK: No dt.dt. references remain.")

if "dt.datetime.combine(" in final_code:
    print("OK: dt.datetime.combine() is present.")
else:
    print("WARNING: dt.datetime.combine() was not found.")

print("OK: config_builder.py compiles successfully.")
print(f"BACKUP: {backup}")
print("=" * 80)
