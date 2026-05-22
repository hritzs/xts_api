"""
inspect_trade.py - Tool to inspect order history for a specific trade.
Usage: python inspect_trade.py <TRADE_UID>
"""
import sqlite3
import sys
import os
import pandas as pd

# Database configuration
DB_NAME = "straddle_trades.db"

def get_orders(trade_uid):
    """Fetches orders for a given trade_uid from the database."""
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    try:
        conn = sqlite3.connect(DB_NAME)
        # Search by order_unique_id matching the base trade_uid to catch truncated orders
        base_uid = trade_uid[:-1] if len(trade_uid) > 1 else trade_uid
        query = f"""
            SELECT 
                created_at, 
                order_unique_id, 
                app_order_id, 
                exchange_instrument_id as token, 
                order_side, 
                order_quantity as qty, 
                order_status, 
                order_avg_price as avg_price, 
                cancel_reject_reason
            FROM orders 
            WHERE order_unique_id LIKE '%{base_uid}%'
            ORDER BY created_at ASC
        """
        df = pd.read_sql_query(query, conn)
        conn.close()
        return df
    except Exception as e:
        print(f"Error fetching orders: {e}")
        return None

def display_orders(df, trade_uid):
    """Displays the orders in a readable format."""
    print(f"\n🔍 INSPECTING TRADE: {trade_uid}")
    
    if df is None or df.empty:
        print("❌ No orders found for this trade.")
        return

    # Adjust pandas display settings for better CLI visibility
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.max_colwidth', 30)

    print("="*120)
    # Print the DataFrame (to_string prevents truncation)
    print(df.to_string(index=False))
    print("="*120)

    # Summary analysis
    print("\n📊 SUMMARY:")
    print(f"Total Orders Generated: {len(df)}")

    # --- FIX: Use .str.upper() for case-insensitive matching ---
    status_upper = df['order_status'].str.upper()

    filled = df[status_upper.isin(['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED'])]
    print(f"✅ Filled Orders:        {len(filled)}")
    
    cancelled = df[status_upper.isin(['CANCELLED', 'CANCELED', 'REJECTED'])]
    print(f"❌ Cancelled/Rejected:   {len(cancelled)}")
    
    pending = df[~status_upper.isin(['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED', 'CANCELLED', 'CANCELED', 'REJECTED'])]
    if not pending.empty:
        print(f"⚠️  Pending/Unknown:      {len(pending)}")

    if not filled.empty:
        print("\n📈 NET FILLED POSITIONS (By Token & Side):")
        # Use a temporary series for grouping to avoid modifying the 'filled' DataFrame
        side_upper = filled['order_side'].str.upper()
        print(filled.groupby(['token', side_upper])['qty'].sum().reset_index().to_string(index=False))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python inspect_trade.py <TRADE_UID>")
        print("Example: python inspect_trade.py ny240523103000a")
    else:
        uid = sys.argv[1]
        df = get_orders(uid)
        display_orders(df, uid)
