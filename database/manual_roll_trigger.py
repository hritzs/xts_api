"""
Manual Roll Trigger Script

This script connects to the trading database and updates a specific trade's
configuration to force an immediate roll to the current At-The-Money (ATM) strike.

It works by setting the `force_roll_to_atm` flag to `true` in the trade's
JSON config stored in the database. The `RollMonitor` for that trade will
detect this flag on its next check, trigger the roll, and then stop itself.

Usage:
    python tools/manual_roll_trigger.py <trade_uid>

Example:
    python tools/manual_roll_trigger.py ny290626091900a

"""
import sqlite3
import json
import argparse
import os
import sys

# Add the project root to the Python path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    import config
except ImportError:
    print("Error: Could not import config.py. Make sure the script is in a 'tools' subdirectory.")
    # Define a fallback if config is not found
    class FallbackConfig:
        DATABASE_NAME = "trading.db"
    config = FallbackConfig()


def trigger_roll_to_atm(db_path: str, trade_uid: str):
    """
    Updates the specified trade's config in the database to trigger a roll.

    Args:
        db_path (str): The path to the SQLite database file.
        trade_uid (str): The unique ID of the trade to roll.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. Fetch the current trade data
        cursor.execute("SELECT config, sl_points FROM straddles WHERE trade_uid = ?", (trade_uid,))
        row = cursor.fetchone()

        if not row:
            print(f"❌ Error: Trade with UID '{trade_uid}' not found in the database.")
            return

        # 2. Load the existing config, or create a new one if it's missing/invalid
        try:
            current_config = json.loads(row['config']) if row['config'] else {}
        except (json.JSONDecodeError, TypeError):
            print(f"⚠️ Warning: Could not parse existing config for {trade_uid}. Starting with a new one.")
            current_config = {}

        # 3. Add the force_roll_to_atm flag
        current_config['force_roll_to_atm'] = True
        updated_config_json = json.dumps(current_config)

        # 4. Update the database record
        cursor.execute(
            "UPDATE straddles SET config = ? WHERE trade_uid = ?",
            (updated_config_json, trade_uid)
        )
        conn.commit()

        print("="*50)
        print(f"✅ Success! Roll-to-ATM has been triggered for trade: {trade_uid}")
        print("   The RollMonitor will execute the roll on its next scheduled check.")
        print("="*50)

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Manually trigger a roll-to-ATM for a trade.")
    parser.add_argument("trade_uid", help="The unique ID of the trade to roll (e.g., ny290626091900a).")
    args = parser.parse_args()

    trigger_roll_to_atm(config.DATABASE_NAME, args.trade_uid)