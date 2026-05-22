"""
STANDALONE PARITY CHECKER
-------------------------
Safe to run while main app is stopped.
1. Connects directly to XTS (Broker) to fetch the full Order Book.
2. Connects directly to your Local DB to fetch today's Straddles.
3. Matches them by Trade UID.
4. Calculates the REAL net position from the Broker's filled orders.
5. Compares it with the DB's stored quantity.
"""

import asyncio
import sys
import re
import os
from collections import defaultdict

# --- Imports (Mocking config/cred if needed or loading from file) ---
try:
    import cred
    import config
    from Connect import XTSConnect
    from database.db_manager import Database
except ImportError as e:
    print(f"❌ CRITICAL ERROR: Could not import project modules.")
    print(f"   Make sure you run this script from the project root directory.")
    print(f"   Error details: {e}")
    sys.exit(1)

# Regex to find Trade UID in OrderUniqueIdentifier
# Looks for patterns like 'ny240724100000a' inside strings like 'SQF_ny240724100000a_CHUNK1'
# Made suffix optional to catch UIDs truncated by XTS 20-char limit
UID_PATTERN = re.compile(r'((?:ny|sx|bn|fn|mc)\d{12}[a-z]?)(?:_.*)?')

async def main():
    print("\n" + "="*80)
    print("🕵️  STANDALONE PARITY CHECKER (READ-ONLY)")
    print("="*80)

    # 1. Initialize Database
    print("🔹 Connecting to Database...")
    try:
        db = Database()
        # Fetch all straddles for today (Active, Closed, etc.)
        straddles = await asyncio.to_thread(db.get_todays_straddles)
        print(f"   ✅ DB Connected. Found {len(straddles)} trades in local DB for today.")
    except Exception as e:
        print(f"   ❌ DB Connection Failed: {e}")
        return

    # 2. Connect to XTS (Broker)
    print("🔹 Connecting to Broker (XTS Interactive)...")
    try:
        xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        login_resp = xt.interactive_login()
        if login_resp.get('type') != 'success':
            print(f"   ❌ Login Failed: {login_resp.get('description')}")
            return
        
        # Get Client ID for order book fetch
        user_id = login_resp['result'].get('userID')
        # FIX: Use clientID from creds if available (Pro/Dealer setup), else fallback to login userID
        client_id = getattr(cred, 'clientID', None) or user_id
        
        print(f"   ✅ Login Successful. User: {user_id}")
        if client_id != user_id:
            print(f"   ℹ️  Using Configured Client ID: {client_id}")

        # Fetch Order Book
        print("🔹 Fetching Full Broker Order Book...")
        # FIX: Force isInvestorClient to False to ensure clientID parameter is respected
        xt.isInvestorClient = False
        order_book_resp = xt.get_order_book(clientID=client_id)
        if order_book_resp.get('type') != 'success':
            print(f"   ❌ Failed to fetch order book: {order_book_resp}")
            return
        
        broker_orders = order_book_resp.get('result', [])
        print(f"   ✅ Fetched {len(broker_orders)} orders from broker.")
    
    except Exception as e:
        print(f"   ❌ Broker Connection/Fetch Failed: {e}")
        return

    # 3. Process Broker Data
    print("🔹 Analyzing Broker Data...")
    
    # Create reverse map for truncated UIDs
    truncated_to_full_uid = {s.get('trade_uid', '')[:-1]: s.get('trade_uid', '') for s in straddles if s.get('trade_uid')}

    # Map: trade_uid -> list of filled orders
    broker_trade_map = defaultdict(list)
    
    for order in broker_orders:
        # Only care about FILLS
        status = str(order.get('OrderStatus', '')).upper()
        if status not in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']:
            continue
            
        # Extract Trade UID
        ouid = order.get('OrderUniqueIdentifier', '')
        match = UID_PATTERN.search(ouid)
        if match:
            extracted_uid = match.group(1)
            trade_uid = truncated_to_full_uid.get(extracted_uid, extracted_uid)
            broker_trade_map[trade_uid].append(order)
        # Note: Orders without a UID pattern in OrderUniqueIdentifier cannot be mapped 
        # back to a specific algo trade easily and are skipped here.

    # 4. Compare and Report
    print("\n" + "="*100)
    print(f"{'TRADE UID':<18} | {'STATUS':<10} | {'TOKEN':<8} | {'DB QTY':<8} | {'REAL QTY':<8} | {'DIFF':<5} | {'RESULT'}")
    print("="*100)

    mismatches_found = 0

    # Combine UIDs from DB and Broker to catch ghost trades
    all_uids = set(straddles_map.get('trade_uid') or straddles_map.get('straddle_id') for straddles_map in straddles)
    all_uids.update(broker_trade_map.keys())

    for uid in sorted(all_uids):
        # Get DB Data
        db_record = next((s for s in straddles if (s.get('trade_uid') == uid or s.get('straddle_id') == uid)), None)
        
        # Defaults if not in DB
        db_status = "UNKNOWN"
        db_ce_qty = 0
        db_pe_qty = 0
        ce_token = 0
        pe_token = 0
        
        if db_record:
            db_status = str(db_record.get('status', ''))[:10]
            db_ce_qty = int(db_record.get('ce_quantity', 0))
            db_pe_qty = int(db_record.get('pe_quantity', 0))
            ce_token = int(db_record.get('ce_token', 0))
            pe_token = int(db_record.get('pe_token', 0))

        # Calculate Real Net Position from Broker Orders
        real_ce_net = 0
        real_pe_net = 0
        
        orders = broker_trade_map.get(uid, [])
        for order in orders:
            token = int(order.get('ExchangeInstrumentID', 0))
            qty = int(order.get('CumulativeQuantity') or order.get('FilledQty') or 0)
            side = str(order.get('OrderSide', '')).upper()
            
            # Net Position Logic: SELL is + (Open), BUY is - (Close)
            # Assuming Short Straddle Logic
            signed_qty = qty if side == 'SELL' else -qty
            
            if token == ce_token and ce_token != 0:
                real_ce_net += signed_qty
            elif token == pe_token and pe_token != 0:
                real_pe_net += signed_qty
            else:
                # If token doesn't match current DB tokens (e.g. previous rolls), 
                # we can't easily map it to current CE/PE columns, but strict parity
                # checks usually care about the currently active legs.
                pass

        # Calculate Diffs
        diff_ce = db_ce_qty - real_ce_net
        diff_pe = db_pe_qty - real_pe_net
        
        # Print Row for CE
        if ce_token > 0 or real_ce_net != 0:
            res_ce = "✅ OK" if diff_ce == 0 else "❌ FAIL"
            if diff_ce != 0: mismatches_found += 1
            print(f"{uid:<18} | {db_status:<10} | CE {ce_token:<5} | {db_ce_qty:<8} | {real_ce_net:<8} | {diff_ce:<5} | {res_ce}")

        # Print Row for PE
        if pe_token > 0 or real_pe_net != 0:
            res_pe = "✅ OK" if diff_pe == 0 else "❌ FAIL"
            if diff_pe != 0: mismatches_found += 1
            print(f"{'':<18} | {'':<10} | PE {pe_token:<5} | {db_pe_qty:<8} | {real_pe_net:<8} | {diff_pe:<5} | {res_pe}")
            
        if (ce_token > 0 or pe_token > 0):
            print("-" * 100)

    print("\n" + "="*100)
    if mismatches_found == 0:
        print("✅ INTEGRITY CHECK PASSED: Database matches Broker Order Book perfectly.")
    else:
        print(f"❌ INTEGRITY CHECK FAILED: Found {mismatches_found} discrepancies.")
        print("   Positive Diff (+): DB has phantom positions (DB says open, Broker says closed).")
        print("   Negative Diff (-): DB is missing positions (Broker says open, DB says closed).")
    print("="*100 + "\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Check cancelled by user.")
