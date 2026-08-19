from pathlib import Path

path = Path(r"models/state.py")

text = path.read_text(encoding="utf-8")

if "pending_entry_builds" in text:
    print("Already patched.")
    raise SystemExit

marker = "self.trade_processes = {}"

if marker not in text:
    raise RuntimeError(f"Couldn't find: {marker}")

replacement = marker + """

        # Pending manual-entry builds
        # trade_uid -> {"context": dict, "task": asyncio.Task}
        self.pending_entry_builds = {}
"""

text = text.replace(marker, replacement, 1)

path.write_text(text, encoding="utf-8")

print("Patched models/state.py")