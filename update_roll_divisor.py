"""
Hardcoded Roll Divisor Update Script

This script connects to the trading database and updates the 'roll_straddle_div'
parameter for a specific trade's configuration.

This allows for live adjustment of the roll sensitivity without modifying other
trade parameters. The RollMonitor will use this new value on its next check.

Usage:
    python tools/update_roll_divisor.py
"""
import sqlite3
import json
import os
import sys

# Add the project root to the Python path to allow importing config
# This assumes the script is in a 'tools' subdirectory.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    import config
except ImportError:
    print("Error: Could not import config.py. Make sure the script is in a 'tools' subdirectory or the project root is in PYTHONPATH.")
    # Define a fallback if config is not found
    class FallbackConfig:
        DATABASE_NAME = "straddle_trades.db"
    config = FallbackConfig()


def update_roll_divisor(db_path: str, trade_uid: str, new_divisor: float):
    """
    Updates the 'roll_straddle_div' in the specified trade's config.

    Args:
        db_path (str): The path to the SQLite database file.
        trade_uid (str): The unique ID of the trade to update.
        new_divisor (float): The new value for roll_straddle_div.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # 1. Fetch the current trade data to get the existing config
        cursor.execute("SELECT config FROM straddles WHERE trade_uid = ?", (trade_uid,))
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

        # 3. Update the roll_straddle_div value
        current_config['roll_straddle_div'] = new_divisor
        updated_config_json = json.dumps(current_config)

        # 4. Update the database record with the modified config
        cursor.execute(
            "UPDATE straddles SET config = ? WHERE trade_uid = ?",
            (updated_config_json, trade_uid)
        )
        conn.commit()

        print("="*60)
        print(f"✅ Success! 'roll_straddle_div' for trade {trade_uid} updated to {new_divisor}")
        print("   The RollMonitor will use this new value on its next check.")
        print("="*60)

    except sqlite3.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()

if __name__ == "__main__":
    # --- Hardcoded values for direct execution ---
    TRADE_TO_UPDATE = "ny290626091900a"
    NEW_DIVISOR_VALUE = 2
    # ---------------------------------------------

    print(f"Running script with hardcoded values:")
    print(f"  - Trade UID:   {TRADE_TO_UPDATE}")
    print(f"  - New Divisor: {NEW_DIVISOR_VALUE}")
    print("-" * 60)
    update_roll_divisor(config.DATABASE_NAME, TRADE_TO_UPDATE, NEW_DIVISOR_VALUE)
