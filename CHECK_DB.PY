import sqlite3
import os

DB_NAME = "straddle_trades.db"

def check_database():
    if not os.path.exists(DB_NAME):
        print(f"❌ Database file '{DB_NAME}' not found.")
        return

    try:
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()

        print(f"\n{'='*20} TRADES (straddles table) {'='*20}")
        cursor.execute("SELECT straddle_id, trade_uid, status, created_at FROM straddles")
        trades = cursor.fetchall()
        if not trades:
            print("No trades found.")
        for row in trades:
            print(f"ID: {row[0]} | UID: {row[1]} | Status: {row[2]} | Created: {row[3]}")

        print(f"\n{'='*20} ORDERS (Unique IDs in orders table) {'='*20}")
        # We check order_unique_id to see if they are truncated (missing the 'a' or 'b' suffix)
        cursor.execute("SELECT DISTINCT order_unique_id FROM orders")
        orders = cursor.fetchall()
        if not orders:
            print("No orders found.")
        for row in orders:
            uid = row[0]
            print(f"UID: {uid} (Length: {len(uid) if uid else 0})")

        conn.close()
    except Exception as e:
        print(f"❌ Database error: {e}")

if __name__ == "__main__":
    check_database()
