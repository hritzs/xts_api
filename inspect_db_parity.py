"""
Script to Inspect DB Parity.

Compares the 'straddle' document (summary) vs the 'orders' table (ledger)
to identify discrepancies in quantity without making any API calls.

Usage:
    python inspect_db_parity.py <trade_uid>
"""
import asyncio
import sys
from database.db_manager import Database

async def inspect_trade_parity(trade_uid: str):
    print(f"🔍 Inspecting DB Parity for Trade: {trade_uid}")
    print("=" * 60)
    
    try:
        db = Database()
    except Exception as e:
        print(f"❌ Failed to connect to DB: {e}")
        return

    # 1. Fetch the Straddle Document (The "Head" state)
    # This is what the dashboard shows and what monitors use
    straddle = await asyncio.to_thread(db.get_straddle_by_id, trade_uid)
    if not straddle:
        print(f"❌ Trade {trade_uid} not found in 'straddles' collection.")
        return

    ce_token = straddle.get('ce_token')
    pe_token = straddle.get('pe_token')

    print("\n1️⃣  STRADDLE DOCUMENT (Summary View)")
    print(f"   Status:      {straddle.get('status')}")
    print(f"   CE Token:    {ce_token}")
    print(f"   PE Token:    {pe_token}")
    print(f"   CE Qty:      {straddle.get('ce_quantity')} (Stored Open Position)")
    print(f"   PE Qty:      {straddle.get('pe_quantity')} (Stored Open Position)")

    # 2. Fetch the Order History (The "Ledger")
    # This is the list of everything actually executed
    orders = await asyncio.to_thread(db.get_orders_by_trade_id, trade_uid)
    
    filled_orders = [
        o for o in orders 
        if str(o.get('order_status') or o.get('OrderStatus')).upper() in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']
    ]

    print(f"\n2️⃣  ORDER HISTORY (Transaction View)")
    print(f"   Total Orders Found: {len(orders)}")
    print(f"   Filled Orders:      {len(filled_orders)}")
    print("\n   --- Net Position Calculation from Orders ---")

    calc_ce_qty = 0
    calc_pe_qty = 0

    for o in filled_orders:
        # Determine Token
        token = o.get('exchange_instrument_id') or o.get('ExchangeInstrumentID')
        try:
            token = int(token)
        except:
            continue
            
        # Determine Qty and Side
        qty = int(o.get('cumulative_quantity') or o.get('CumulativeQuantity') or 0)
        side = str(o.get('order_side') or o.get('OrderSide')).upper()
        
        # Calculate Net Change:
        # SELL adds to open position (+), BUY reduces open position (-)
        change = qty if side == 'SELL' else -qty
        
        if token == ce_token:
            calc_ce_qty += change
        elif token == pe_token:
            calc_pe_qty += change
        else:
            # Handle rolls where token might have changed, or simply list as other
            pass

    print(f"   Calculated CE Net Open: {calc_ce_qty}")
    print(f"   Calculated PE Net Open: {calc_pe_qty}")

    # 3. Compare Results
    print("\n3️⃣  PARITY CHECK RESULTS")
    print(f"   {'TYPE':<10} | {'STRADDLE DB':<15} | {'ORDERS CALC':<15} | {'DIFF':<10}")
    print("-" * 60)
    
    ce_diff = straddle.get('ce_quantity', 0) - calc_ce_qty
    pe_diff = straddle.get('pe_quantity', 0) - calc_pe_qty
    
    print(f"   {'CE':<10} | {straddle.get('ce_quantity', 0):<15} | {calc_ce_qty:<15} | {ce_diff:<10}")
    print(f"   {'PE':<10} | {straddle.get('pe_quantity', 0):<15} | {calc_pe_qty:<15} | {pe_diff:<10}")
    
    if ce_diff == 0 and pe_diff == 0:
        print("\n✅ PARITY OK: The straddle document perfectly matches the order history.")
    else:
        print("\n❌ PARITY ERROR: Discrepancy detected!")
        print("   The 'straddle' document (used by monitors) does NOT match the sum of executed orders.")

if __name__ == "__main__":
    trade_uid = sys.argv[1] if len(sys.argv) > 1 else input("Enter Trade UID: ")
    asyncio.run(inspect_trade_parity(trade_uid))