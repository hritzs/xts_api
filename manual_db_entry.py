"""
manual_db_entry.py - Automatically fetches missing orders by OUI and injects them.
"""
import asyncio
import cred
from Connect import XTSConnect
from database.db_manager import Database

async def recover_specific_orders():
    trade_uid = "ny090726095000e"
    
    # The exact OrderUniqueIdentifiers from your broker logs
    target_ouis = [
        "HEDGE_sx130526095600"  # The UID of the hedge orders that were not linked
    ]

    db = Database()
    
    print("🔗 Connecting to Broker...")
    xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
    resp = xt.interactive_login()
    if resp.get('type') != 'success':
        print(f"❌ Login failed: {resp.get('description')}")
        return
        
    client_id = getattr(cred, 'clientID', resp['result']['userID'])
    xt.isInvestorClient = False
    ob_resp = xt.get_order_book(clientID=client_id)
    if ob_resp.get('type') != 'success':
        print(f"❌ Failed to get order book: {ob_resp.get('description')}")
        return

    broker_orders = ob_resp.get('result', [])
    print(f"📥 Fetched {len(broker_orders)} orders from broker.")

    recovered = 0
    for o in broker_orders:
        ouid = str(o.get('OrderUniqueIdentifier', ''))
        status = str(o.get('OrderStatus', '')).upper()
        
        if any(target in ouid for target in target_ouis) and status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']:
            original_ouid = o['OrderUniqueIdentifier']
            # Prefix the OUI with the trade_uid so the system naturally claims it
            o['OrderUniqueIdentifier'] = f"MANUAL_{trade_uid}_{original_ouid}"
            o['order_unique_id'] = o['OrderUniqueIdentifier']
            
            db.insert_order(o)
            recovered += 1
            print(f"   ✅ RECOVERED! AppOrderID: {o.get('AppOrderID')} | OUI: {original_ouid} | Price: ₹{o.get('OrderAverageTradedPrice')}")

    print(f"\n🎉 Finished! Permanently recovered and mapped {recovered} target orders to {trade_uid}.")

if __name__ == "__main__":
    asyncio.run(recover_specific_orders())