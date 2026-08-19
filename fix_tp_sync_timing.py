from pathlib import Path
from datetime import datetime
import shutil
import py_compile

p = Path("trading/monitors/tp_monitor.py")

if not p.exists():
    raise SystemExit("ERROR: trading/monitors/tp_monitor.py not found.")

stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup = p.with_name(
    f"{p.stem}.before_tp_sync_timing_{stamp}{p.suffix}"
)

shutil.copy2(p, backup)

code = p.read_text(encoding="utf-8")

print("=" * 100)
print("TP MONITOR — SYNCHRONIZE WITH OTHER MONITORS")
print("=" * 100)
print("BACKUP:", backup)

# ============================================================
# REMOVE THE SPECIAL 58/59 SECOND GATE
# ============================================================

old = """        import datetime
        now = datetime.datetime.now()
        
        # 1. Only execute at the end of the minute (e.g., 58s or 59s)
        if now.second < 58:
            return
            
        # 2. Ensure it strictly runs only ONCE per minute
        current_minute = now.minute
        if getattr(self, '_last_check_minute', -1) == current_minute:
            return
        self._last_check_minute = current_minute

        try:
"""

new = """        try:
"""

if old not in code:
    raise SystemExit(
        "ERROR: Expected TP timing gate not found. "
        "NO CHANGES MADE."
    )

code = code.replace(
    old,
    new,
    1
)

print(
    "REMOVED: TP-specific 58/59-second timing gate."
)

# ============================================================
# ADD A CLEAR SYNC COMMENT
# ============================================================

anchor = """            snapshot_time = get_ist_now()
            logger.info(f"🎯 TP Check at {snapshot_time.strftime('%H:%M:%S')} for {self.trade_uid}")
"""

replacement = """            snapshot_time = get_ist_now()

            # TP is intentionally checked in the same runtime monitor
            # cycle as SL / HEDGE / ROLL / STRADDLE-EXIT.
            # The scheduler/TradeManager controls the monitor cadence.
            logger.info(
                f"🎯 TP Check at "
                f"{snapshot_time.strftime('%H:%M:%S')} "
                f"for {self.trade_uid}"
            )
"""

if anchor not in code:
    raise SystemExit(
        "ERROR: TP check log anchor not found. "
        "NO CHANGES MADE."
    )

code = code.replace(
    anchor,
    replacement,
    1
)

# ============================================================
# WRITE
# ============================================================

p.write_text(
    code,
    encoding="utf-8"
)

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
    print("COMPILE FAILED:")
    print(e)

    print("")
    print("RESTORING BACKUP...")

    shutil.copy2(
        backup,
        p
    )

    py_compile.compile(
        str(p),
        doraise=True
    )

    print("BACKUP RESTORED")
    raise SystemExit(2)

# ============================================================
# VERIFY
# ============================================================

final = p.read_text(
    encoding="utf-8"
)

checks = {
    "TP check exists":
        "TP Check at" in final,

    "58-second gate removed":
        "now.second < 58" not in final,

    "last-minute custom gate removed":
        "_last_check_minute" not in final,

    "TP bps retained":
        "self.tp_bps" in final,

    "TP formula retained":
        "/ 10000.0" in final,

    "TP trigger retained":
        "partial_square_off_needed" in final,
}

print("")
print("=" * 100)
print("VERIFICATION")
print("=" * 100)

failed = []

for name, ok in checks.items():
    print(
        f"{name:<35}: "
        f"{'OK' if ok else 'FAILED'}"
    )

    if not ok:
        failed.append(name)

if failed:
    print("")
    print(
        "FAILED:",
        ", ".join(failed)
    )

    print("")
    print("RESTORING BACKUP...")

    shutil.copy2(
        backup,
        p
    )

    py_compile.compile(
        str(p),
        doraise=True
    )

    print("BACKUP RESTORED")
    raise SystemExit(3)

print("")
print("=" * 100)
print("SUCCESS")
print("=" * 100)
print("")
print("TP now uses the same scheduler/cycle as the other monitors.")
print("")
print("Expected runtime:")
print("  SL")
print("  HEDGE")
print("  ROLL")
print("  TP")
print("  STRADDLE EXIT")
print("")
print("No special :58/:59 TP execution remains.")
print("")
print("PY_COMPILE: SUCCESS")
print("BACKUP:", backup)
print("=" * 100)
