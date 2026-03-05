# from XTConnect import XTSConnect
from datetime import datetime
import json
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
xt_i = XTSConnect(API_KEY_I, API_SECRET_I, source)
xt_m = XTSConnect(API_KEY_M, API_SECRET_M, source)

response_I = xt_i.interactive_login()
response_M = xt_m.marketdata_login()

def get_ltp(token):
    instruments = [{'exchangeSegment': exchangeSegment, 'exchangeInstrumentID': token}]
    response = (xt_m.send_subscription(instruments, "1512")).get('result', {}).get('listQuotes', [])
    if response:
        data = json.loads(response[0])
        return data.get('LastTradedPrice')
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

    print(f"Strike: {strike} | CE Token: {ce_instrument_id} | PE Token: {pe_instrument_id} | CE LTP: {ce_ltp} | PE LTP: {pe_ltp}")

# clientID = "DV01"

# ORDER_DICT = []

# """Order book Request"""
# print(option_chain.set_index('strike').at[atm, 'ce_token'])
# ce_token = option_chain.set_index('strike').at[atm, 'ce_token']
# response = xt_i.get_order_book(clientID)
# print("Order Book: ", response)
response = xt_i.place_order(
    exchangeSegment=xt_i.EXCHANGE_NSEFO,
    exchangeInstrumentID=int(option_chain.set_index('strike').at[atm, 'ce_token']),
    productType=xt_i.PRODUCT_MIS,
    orderType=xt_i.ORDER_TYPE_MARKET,
    orderSide=xt_i.TRANSACTION_TYPE_BUY,
    timeInForce=xt_i.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=65,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test_buy",
    clientID=clientID)

response = xt_i.place_order(
    exchangeSegment=xt_i.EXCHANGE_NSEFO,
    exchangeInstrumentID=int(option_chain.set_index('strike').at[atm, 'pe_token']),
    productType=xt_i.PRODUCT_MIS,
    orderType=xt_i.ORDER_TYPE_MARKET,
    orderSide=xt_i.TRANSACTION_TYPE_SELL,
    timeInForce=xt_i.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=65,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test",
    clientID=clientID)
print("Place Order: ", response)

# oid = response['result']['AppOrderID']
# ORDER_DICT.append(oid)



"""Order book Request"""
# response = xt_i.get_order_book(clientID)
# print("Order Book: ", response)
# response = xt.place_order(
#     exchangeSegment=xt.EXCHANGE_NSEFO,
#     exchangeInstrumentID=72164,
#     productType=xt.PRODUCT_MIS,
#     orderType=xt.ORDER_TYPE_MARKET,
#     orderSide=xt.TRANSACTION_TYPE_BUY,
#     timeInForce=xt.VALIDITY_DAY,
#     disclosedQuantity=0,
#     orderQuantity=75,
#     limitPrice=0,
#     stopPrice=0,
#     orderUniqueIdentifier="abcdef",
#     clientID=clientID)
# print("Place Order: ", response)

# oid = response['result']['AppOrderID']
# ORDER_DICT.append(oid)



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

def check_enabled_exchanges():
    """Check which exchanges are enabled for your account"""
    try:
        # Try to get holdings/profile
        response = xt_i.get_profile(clientID=clientID)
        
        if response:
            print("Enabled exchanges:",response)
            # Check the profile for enabled segments
            # Usually shows: NSECM, NSEFO, etc.
    except Exception as e:
        print(f"Check failed: {e}")

check_enabled_exchanges()