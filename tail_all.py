"""
tail_all.py — Opens a live-tailing Windows Terminal tab for each service log.
Usage: python tail_all.py
Run AFTER start_all.py is already running.
"""
import subprocess
import os
import glob
from datetime import datetime

LOG_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs", "services")
SERVICES = ["marketdata", "reconciler", "main"]

COLORS = {
    "marketdata" : "Blue",
    "reconciler" : "Magenta",
    "main"       : "Green",
}

date_str = datetime.now().strftime("%Y%m%d")

for name in SERVICES:
    log_path = os.path.join(LOG_DIR, f"{name}_{date_str}.log")

    if not os.path.exists(log_path):
        print(f"[WARN] Log not found for {name}: {log_path} — skipping")
        continue

    subprocess.Popen([
        "wt", "new-tab",
        "--title", f"[{name}]",
        "--",
        "powershell", "-NoExit", "-Command",
        f"Get-Content '{log_path}' -Wait -Tail 50"
    ])
    print(f"[OK] Opened tab for {name} -> {log_path}")
