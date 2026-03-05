# from XTConnect import XTSConnect
from datetime import datetime
import json
import time
import pandas as pd
from unittest import result
from Connect import XTSConnect
import cred
# from MarketDataSocketClient import MDSocket_io
# logging.basicConfig(level=logging.DEBUG)

"""Investor client credentials"""
API_KEY_I = cred.API_KEY_I
API_SECRET_I = cred.API_SECRET_I

API_KEY_M = cred.API_KEY_M
API_SECRET_M = cred.API_SECRET_M

clientID = "TEST49"
XTS_API_BASE_URL = "https://developers.symphonyfintech.in/"
source = "WebAPI"
xt = XTSConnect(API_KEY_I, API_SECRET_I, source="WEBAPI")
xt_m = XTSConnect(API_KEY_M, API_SECRET_M,source="WEBAPI")

response_I = xt.interactive_login()
response_M = xt_m.marketdata_login()

def get_ltp(token):
    instruments = [{'exchangeSegment': exchangeSegment, 'exchangeInstrumentID': token}]
    # ✅ FIX: Use get_quote for a reliable one-time price fetch, not send_subscription.
    response = xt_m.get_quote(
        Instruments=instruments,
        xtsMessageCode=1501,  # 1501 for depth, 1512 for LTP only would also work
        publishFormat='JSON'
    )
    
    list_quotes = response.get('result', {}).get('listQuotes', [])
    # print(list_quotes)
    
    if list_quotes:
        # The quote is a JSON string inside the list
        data = json.loads(list_quotes[0])
        return data.get('LastTradedPrice')
    
    print(f"⚠️  get_ltp failed for token {token}. Full response: {response}")
    return None
option_chain = pd.DataFrame()
print("Login_M", response_M.get('type'), "| Login_I", response_I.get('type'))
symbol = "NIFTY"
gap = 50
exchangeSegment = 2
expiryDate = datetime.strptime((xt_m.get_expiry_date(exchangeSegment=exchangeSegment,series="FUTIDX",symbol = symbol))['result'][0],"%Y-%m-%dT%H:%M:%S").strftime("%d%b%Y")

fut_token = (xt_m.get_future_symbol(exchangeSegment = exchangeSegment,series = "FUTIDX",symbol = symbol,expiryDate = expiryDate)).get('result', [])
exchange_instrument_id = (fut_token[0].get('ExchangeInstrumentID')if fut_token else None)
fut_ltp = get_ltp(exchange_instrument_id)
atm = round(fut_ltp/gap)*gap if fut_ltp else None

print("Expiry Date: ", expiryDate, "| FUT LTP: ", fut_ltp, "| Strike Price: ", atm)
strikes = [atm + i * gap for i in range(-5, 5 + 1)] if atm is not None else []
print("Strikes: ", strikes)

for strike in strikes:
    ce_token = (xt_m.get_option_symbol(exchangeSegment = exchangeSegment,series="OPTIDX",symbol = symbol,expiryDate = expiryDate,optionType="CE",strikePrice = strike)).get('result', {})
    pe_token = (xt_m.get_option_symbol(exchangeSegment = exchangeSegment,series="OPTIDX",symbol = symbol,expiryDate = expiryDate,optionType="PE",strikePrice = strike)).get('result', {})

    ce_instrument_id = (ce_token[0].get('ExchangeInstrumentID')if ce_token else None)
    pe_instrument_id = (pe_token[0].get('ExchangeInstrumentID')if pe_token else None)
    ce_ltp = get_ltp(ce_instrument_id)
    pe_ltp = get_ltp(pe_instrument_id)
    row = {
        "strike": strike,
        "ce_token": ce_instrument_id,
        "pe_token": pe_instrument_id,
        "ce_ltp": ce_ltp,
        "pe_ltp": pe_ltp
    }
    option_chain = pd.concat([option_chain, pd.DataFrame([row])], ignore_index=True) if 'option_chain' in locals() else pd.DataFrame([row])

    # print(f"Strike: {strike} | CE Token: {ce_instrument_id} | PE Token: {pe_instrument_id} | CE LTP: {ce_ltp} | PE LTP: {pe_ltp}")

# clientID = "DV01"

# ORDER_DICT = []

# --- NEW TEST SEQUENCE FOR STATUSES ---

def run_status_tests():
    """Runs a sequence of trades to trigger various order statuses."""
    print("\n" + "="*60)
    print("🚀 STARTING COMPREHENSIVE ORDER STATUS TEST SEQUENCE 🚀")
    print("="*60 + "\n")

    # Use a far OTM PE for non-executing tests
    test_token = int(option_chain.iloc[-1]['pe_token'])
    lot_size = 65 # NIFTY
    print(f"Using test token: {test_token} with Lot Size: {lot_size}")

    # --- Test 1: PendingNew -> Open -> PendingReplace -> Replaced ---
    print("\n--- TEST 1: OPEN & REPLACE ---")
    open_replace_uid = "test_open_replace"
    test_ltp = get_ltp(test_token)
    if not test_ltp:
        print("❌ Could not get LTP. Skipping Open/Replace test.")
        return

    place_resp = xt.place_order(
        exchangeSegment=xt.EXCHANGE_NSEFO, exchangeInstrumentID=test_token, productType=xt.PRODUCT_MIS,
        orderType=xt.ORDER_TYPE_LIMIT, orderSide=xt.TRANSACTION_TYPE_SELL, timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0, orderQuantity=lot_size, limitPrice=round(test_ltp + 20, 2), stopPrice=0,
        orderUniqueIdentifier=open_replace_uid, clientID=clientID)
    
    order_id_1 = place_resp.get('result', {}).get('AppOrderID')
    if not order_id_1:
        print("❌ Failed to place order for Test 1. Aborting.")
        return

    print(f"Placed order {order_id_1} for Open/Replace test.")
    time.sleep(1)
    history1 = xt.get_order_history(appOrderID=order_id_1)
    print(f"  -> Initial History for {order_id_1}: Status should be PendingNew/New. Found: {history1['result'][-1]['OrderStatus']}")

    # Poll until Open
    for _ in range(5):
        book = xt.get_order_book(clientID=clientID)
        found = next((o for o in book.get('result', []) if str(o.get('AppOrderID')) == str(order_id_1)), None)
        if found and found.get('OrderStatus') in ['Open', 'New']:
            print(f"  -> Order {order_id_1} is now {found.get('OrderStatus')}.")
            break
        time.sleep(1)

    print(f"  -> Modifying order {order_id_1}...")
    xt.modify_order(
        appOrderID=order_id_1, modifiedProductType=xt.PRODUCT_MIS, modifiedOrderType=xt.ORDER_TYPE_LIMIT,
        modifiedOrderQuantity=lot_size, modifiedDisclosedQuantity=0, modifiedLimitPrice=round(test_ltp + 15, 2),
        modifiedStopPrice=0, modifiedTimeInForce=xt.VALIDITY_DAY, orderUniqueIdentifier=open_replace_uid, clientID=clientID)
    
    time.sleep(1)
    history2 = xt.get_order_history(appOrderID=order_id_1)
    print(f"  -> History for {order_id_1} after modify: Status should be PendingReplace. Found: {history2['result'][-1]['OrderStatus']}")

    time.sleep(2)
    book2 = xt.get_order_book(clientID=clientID)
    found2 = next((o for o in book2.get('result', []) if str(o.get('AppOrderID')) == str(order_id_1)), None)
    if found2 and found2.get('OrderStatus') == 'Replaced':
        print(f"  -> ✅ SUCCESS: Order {order_id_1} final status is Replaced.")

    # --- Test 2: PendingCancel -> Cancelled ---
    print("\n--- TEST 2: CANCEL ---")
    cancel_uid = "test_cancel"
    place_resp_2 = xt.place_order(
        exchangeSegment=xt.EXCHANGE_NSEFO, exchangeInstrumentID=test_token, productType=xt.PRODUCT_MIS,
        orderType=xt.ORDER_TYPE_LIMIT, orderSide=xt.TRANSACTION_TYPE_SELL, timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0, orderQuantity=lot_size, limitPrice=round(test_ltp + 20, 2), stopPrice=0,
        orderUniqueIdentifier=cancel_uid, clientID=clientID)

    order_id_2 = place_resp_2.get('result', {}).get('AppOrderID')
    if not order_id_2:
        print("❌ Failed to place order for Test 2. Aborting.")
        return
    
    print(f"Placed order {order_id_2} for Cancel test. Waiting for it to be Open...")
    time.sleep(3) # Wait for it to be open

    print(f"  -> Cancelling order {order_id_2}...")
    xt.cancel_order(appOrderID=order_id_2, orderUniqueIdentifier=cancel_uid, clientID=clientID)
    time.sleep(1)
    history3 = xt.get_order_history(appOrderID=order_id_2)
    print(f"  -> History for {order_id_2} after cancel: Status should be PendingCancel. Found: {history3['result'][-1]['OrderStatus']}")

    time.sleep(2)
    book3 = xt.get_order_book(clientID=clientID)
    found3 = next((o for o in book3.get('result', []) if str(o.get('AppOrderID')) == str(order_id_2)), None)
    if found3 and found3.get('OrderStatus') == 'Cancelled':
        print(f"  -> ✅ SUCCESS: Order {order_id_2} final status is Cancelled.")

    # --- Test 3: PartiallyFilled -> Filled ---
    print("\n--- TEST 3: PARTIAL FILL & FILL ---")
    atm_ce_token = int(option_chain.set_index('strike').at[atm, 'ce_token'])
    atm_ce_ltp = get_ltp(atm_ce_token)
    if not atm_ce_ltp:
        print("❌ Could not get ATM CE LTP. Skipping Fill test.")
        return

    fill_uid = "test_fill"
    # Place a large BUY order slightly below the ask price to try and get a partial fill
    fill_qty = lot_size * 5 # 5 lots
    partial_fill_price = round(atm_ce_ltp - 1, 2)
    print(f"Placing large BUY order for {fill_qty} qty at price ₹{partial_fill_price} to get a partial fill...")
    place_resp_3 = xt.place_order(
        exchangeSegment=xt.EXCHANGE_NSEFO, exchangeInstrumentID=atm_ce_token, productType=xt.PRODUCT_MIS,
        orderType=xt.ORDER_TYPE_LIMIT, orderSide=xt.TRANSACTION_TYPE_BUY, timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0, orderQuantity=fill_qty, limitPrice=partial_fill_price, stopPrice=0,
        orderUniqueIdentifier=fill_uid, clientID=clientID)
    
    order_id_3 = place_resp_3.get('result', {}).get('AppOrderID')
    if not order_id_3:
        print("❌ Failed to place order for Test 3.")
        return

    has_been_partially_filled = False
    has_been_modified = False

    print(f"Placed order {order_id_3} for Fill test. Polling status...")
    for i in range(20): # Increased polling attempts
        book4 = xt.get_order_book(clientID=clientID)
        found4 = next((o for o in book4.get('result', []) if str(o.get('AppOrderID')) == str(order_id_3)), None)
        if found4:
            status = found4.get('OrderStatus')
            filled_qty = found4.get('CumulativeQuantity')
            print(f"  -> Poll {i+1}: Status is {status}, Filled Qty: {filled_qty}")

            if status == 'PartiallyFilled' and not has_been_modified:
                print(f"  -> ✅ SUCCESS: Captured PartiallyFilled status.")
                has_been_partially_filled = True
                
                # Now, modify the order to get the rest filled
                aggressive_price = round(atm_ce_ltp + 5, 2)
                print(f"  -> Modifying partially filled order {order_id_3} to aggressive price ₹{aggressive_price}...")
                xt.modify_order(
                    appOrderID=order_id_3, modifiedProductType=xt.PRODUCT_MIS, modifiedOrderType=xt.ORDER_TYPE_LIMIT,
                    modifiedOrderQuantity=fill_qty, # Keep original total quantity
                    modifiedDisclosedQuantity=0, modifiedLimitPrice=aggressive_price,
                    modifiedStopPrice=0, modifiedTimeInForce=xt.VALIDITY_DAY, 
                    orderUniqueIdentifier=fill_uid, clientID=clientID
                )
                has_been_modified = True
                print(f"  -> Modification request sent. Continuing to poll for full fill.")

            if status == 'Filled':
                print(f"  -> ✅ SUCCESS: Order {order_id_3} is Filled.")
                if has_been_partially_filled and has_been_modified:
                    print("  -> ✅ SUCCESS: Full lifecycle (Partial -> Modify -> Fill) complete.")
                break
        time.sleep(1)

    # --- Test 4: Rejected ---
    print("\n--- TEST 4: REJECTED ---")
    rejected_uid = "test_rejected"
    rejected_resp = xt.place_order(
        exchangeSegment=xt.EXCHANGE_NSEFO, exchangeInstrumentID=test_token, productType=xt.PRODUCT_MIS,
        orderType=xt.ORDER_TYPE_MARKET, orderSide=xt.TRANSACTION_TYPE_SELL, timeInForce=xt.VALIDITY_DAY,
        disclosedQuantity=0, orderQuantity=1, limitPrice=0, stopPrice=0,
        orderUniqueIdentifier=rejected_uid, clientID=clientID)
    
    order_id_4 = rejected_resp.get('result', {}).get('AppOrderID')
    if order_id_4:
        print(f"Placed order {order_id_4} for Rejection test.")
        time.sleep(2)
        book5 = xt.get_order_book(clientID=clientID)
        found5 = next((o for o in book5.get('result', []) if str(o.get('AppOrderID')) == str(order_id_4)), None)
        if found5 and found5.get('OrderStatus') == 'Rejected':
            print(f"  -> ✅ SUCCESS: Order {order_id_4} has status Rejected. Reason: {found5.get('CancelRejectReason')}")
    else:
        print(f"  -> ✅ SUCCESS: Order placement failed immediately as expected. Response: {rejected_resp.get('description')}")

    print("\n" + "="*60)
    print("✅ TEST SEQUENCE COMPLETE ✅")
    print("="*60 + "\n")
    time.sleep(2)

    print("\n--- FINAL ORDER BOOK ---")
    final_book = xt.get_order_book(clientID=clientID)
    with open('order_book_response.txt', 'w') as f:
        f.write(json.dumps(final_book, indent=4))
    # print("Final order book saved to order_book_response.txt")
    # print(json.dumps(final_book, indent=2))


run_status_tests()

# OrderStatus
# CancelRejectReason



# response = xt.cancelall_order(exchangeInstrumentID=2885,exchangeSegment=xt.EXCHANGE_NSECM)
# print("Cancel all Orders: ", response)
# response = xt.get_trade(clientID=clientID)
# print("Trade Book: ", response)
# response = xt.get_position_daywise(clientID=clientID)
# print("Position by Day: ", response)

"""Get Order History Request"""
# response = xt.get_order_history(appOrderID=1110025806,clientID=clientID)
# print("Order History: ", response)




# response = xt.get_balance(clientID=213)
# # print("Balance: ", response)

# response = xt.get_profile(" ")
# print("Profile: ", response)
