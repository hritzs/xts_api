"""
Script to check full parity between Broker Order Book and Local Database.
Fetches all orders from the broker, groups them by Trade UID, and compares
quantities against the local DB.

Usage:
    python check_full_parity.py
"""
import requests
import json
import config
import sys

def main():
    host = getattr(config, 'HOST', '127.0.0.1')
    port = getattr(config, 'PORT', 5000)
    if host == '0.0.0.0': host = '127.0.0.1'

    url = f"http://{host}:{port}/api/diagnostics/parity-check"
    
    print(f"Connecting to {url}...")
    print("Fetching full order book and comparing. This may take a few seconds...")

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"❌ Error contacting API: {e}")
        sys.exit(1)

    if not data.get('success'):
        print(f"❌ Server reported error: {data.get('detail') or data}")
        sys.exit(1)

    total = data.get('total_trades_checked', 0)
    discrepancies = data.get('discrepancies', [])

    print("\n" + "="*80)
    print(f"🔍 PARITY CHECK REPORT")
    print("="*80)
    print(f"Total Trades Checked: {total}")
    print(f"Discrepancies Found:  {len(discrepancies)}")
    print("-" * 80)

    if not discrepancies:
        print("✅ ALL TRADES ARE IN PARITY.")
        print("   Database quantities match Broker Net quantities exactly.")
    else:
        # Format Output
        # Header
        header = f"{'UID':<18} | {'STATUS':<10} | {'DB CE':<7} {'Real CE':<7} {'Diff':<5} | {'DB PE':<7} {'Real PE':<7} {'Diff':<5}"
        print(header)
        print("-" * len(header))

        for d in discrepancies:
            uid = d['trade_uid']
            st = d['status'][:10]
            
            db_ce = d['db_ce']
            br_ce = d['broker_ce']
            df_ce = d['diff_ce']
            
            db_pe = d['db_pe']
            br_pe = d['broker_pe']
            df_pe = d['diff_pe']

            line = f"{uid:<18} | {st:<10} | {db_ce:<7} {br_ce:<7} {df_ce:<5} | {db_pe:<7} {br_pe:<7} {df_pe:<5}"
            print(line)
        
        print("-" * 80)
        print("NOTE: 'Diff' = DB Qty - Broker Net Qty.")
        print("      Positive Diff (+): DB thinks we have MORE than broker (Phantom position).")
        print("      Negative Diff (-): DB thinks we have LESS than broker (Unrecorded position).")

if __name__ == "__main__":
    main()