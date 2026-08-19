"""
STANDALONE CONFIG TRADE CANCELLER
---------------------------------
Safe to run while main app is stopped.

1. Connects directly to your Local DB (straddle_trades.db).
2. Lists today's NIFTY PENDING straddles.
3. Cancels the configured trade UID by setting status = 'CANCELLED'.
"""

import asyncio
import sys
import os

try:
    import config
    from database.db_manager import Database
    from utils.logger import logger
except ImportError as e:
    print(f"❌ CRITICAL ERROR: Could not import project modules.")
    print(f"   Make sure you run this script from the project root directory.")
    print(f"   Error details: {e}")
    sys.exit(1)


# 👉 EDIT THIS CONSTANT BEFORE RUNNING
# Use the full ny... ID from logs/DB, e.g. "ny240626091800a"
TARGET_UID = "ny240626091800a"


async def main():
    print("\n" + "=" * 80)
    print("🛑  STANDALONE CONFIG TRADE CANCELLER")
    print("=" * 80)

    # 1. Point config.DATABASE_NAME to your specific DB file
    db_path = r"C:\Users\Administrator\Desktop\api_v2_main_19_5_26 - Copy (Copy)\straddle_trades.db"
    config.DATABASE_NAME = db_path

    if not os.path.exists(db_path):
        print(f"❌ DB file not found: {db_path}")
        return

    print(f"🔹 Using DB: {db_path}")

    # 2. Initialize Database
    try:
        db = Database()
        print("   ✅ DB Connected.")
    except Exception as e:
        print(f"   ❌ DB Connection Failed: {e}")
        return

    # 3. Fetch today's NIFTY PENDING straddles
    print("🔹 Fetching today's NIFTY PENDING trades...")
    straddles = await asyncio.to_thread(db.get_todays_straddles)
    nifty_pending = [
        s for s in straddles
        if (s.get("symbol") == "NIFTY" and str(s.get("status", "")).upper() == "PENDING")
    ]

    if not nifty_pending:
        print("   ℹ️ No NIFTY PENDING trades found for today.")
        return

    print(f"   ✅ Found {len(nifty_pending)} NIFTY PENDING trade(s):")
    print("\n" + "-" * 80)
    for s in nifty_pending:
        uid = s.get("trade_uid") or s.get("straddle_id")
        symbol = s.get("symbol")
        status = s.get("status")
        strike = s.get("strike")
        print(f"   UID: {uid} | Symbol: {symbol} | Status: {status} | Strike: {strike}")
    print("-" * 80 + "\n")

    # 4. Make sure TARGET_UID exists in today's NIFTY PENDING list
    target = next(
        (s for s in nifty_pending
         if s.get("trade_uid") == TARGET_UID or s.get("straddle_id") == TARGET_UID),
        None,
    )

    if not target:
        print(f"   ❌ Configured UID '{TARGET_UID}' not found among today's NIFTY PENDING trades.")
        print("      Update TARGET_UID in this script to the correct ny... ID.")
        return

    print(f"   ✅ Found trade: {TARGET_UID}. Status will be set to CANCELLED.")

    # 5. Perform the cancellation using your helper
    try:
        db.update_straddle_status(TARGET_UID, "CANCELLED")
        print(f"\n🎯 DONE: Trade {TARGET_UID} marked as CANCELLED in DB.\n")
    except Exception as e:
        print(f"   ❌ Update status failed: {e}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Cancel script aborted by user.")