# from XTConnect import XTSConnect
from datetime import datetime
import json
import pandas as pd
from unittest import result
from Connect import XTSConnect
import cred

"""Investor client credentials"""
API_KEY_I = cred.API_KEY_I
API_SECRET_I = cred.API_SECRET_I

API_KEY_M = cred.API_KEY_M
API_SECRET_M = cred.API_SECRET_M

clientID = "*****"
XTS_API_BASE_URL = "http://14.143.199.34:3000/"
source = "WebAPI"
xt = XTSConnect(API_KEY_I, API_SECRET_I, source)
xt_m = XTSConnect(API_KEY_M, API_SECRET_M, source)

response_I = xt.interactive_login()
response_M = xt_m.marketdata_login()

def get_ltp(token, exchangeSegment):
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
exchangeSegment_nse_fo = 2
exchangeSegment_nse_cm = 1
exchangeSegment_bse_fo = 12
exchangeSegment_bse_cm = 11

expiryDate_nse_fo = datetime.strptime((xt_m.get_expiry_date(exchangeSegment=exchangeSegment_nse_fo,series="FUTIDX",symbol = symbol))['result'][0],"%Y-%m-%dT%H:%M:%S").strftime("%d%b%Y")

fut_token_nse_fo = (xt_m.get_future_symbol(exchangeSegment = exchangeSegment_nse_fo,series = "FUTIDX",symbol = symbol,expiryDate = expiryDate_nse_fo)).get('result', [])
exchange_instrument_id_nse_fo = (fut_token_nse_fo[0].get('ExchangeInstrumentID')if fut_token_nse_fo else None)

exchange_instrument_id_nse_fo = 58072
ORDER_DICT = []

response = xt.place_order(
    exchangeSegment="NSEFO",
    exchangeInstrumentID=exchange_instrument_id_nse_fo,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_BUY,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=65,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test_buy",
    clientID=clientID)
print("Place Order: ", response)

response = xt.place_order(
    exchangeSegment="NSEFO",
    exchangeInstrumentID=exchange_instrument_id_nse_fo,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_SELL,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=65,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test",
    clientID=clientID)
print("Place Order: ", response)
symbol = "SENSEX"
expiryDate_bse_fo = datetime.strptime((xt_m.get_expiry_date(exchangeSegment=exchangeSegment_bse_fo,series="IF",symbol = symbol))['result'][0],"%Y-%m-%dT%H:%M:%S").strftime("%d%b%Y")

fut_token_bse_fo = (xt_m.get_future_symbol(exchangeSegment = exchangeSegment_bse_fo,series = "IF",symbol = symbol,expiryDate = expiryDate_bse_fo)).get('result', [])
exchange_instrument_id_bse_fo = (fut_token_bse_fo[0].get('ExchangeInstrumentID')if fut_token_bse_fo else None)
exchange_instrument_id_bse_fo = 1144507
response = xt.place_order(
    exchangeSegment="BSEFO",
    exchangeInstrumentID=exchange_instrument_id_bse_fo,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_BUY,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=20,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test_buy",
    clientID=clientID)
print("Place Order: ", response)

response = xt.place_order(
    exchangeSegment="BSEFO",
    exchangeInstrumentID=exchange_instrument_id_bse_fo,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_SELL,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=20,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test",
    clientID=clientID)
print("Place Order: ", response)


reliance_nse = 2885
response = xt.place_order(
    exchangeSegment="NSECM",
    exchangeInstrumentID=reliance_nse,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_BUY,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=1,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test_buy",
    clientID=clientID)
print("Place Order: ", response)

response = xt.place_order(
    exchangeSegment="NSECM",
    exchangeInstrumentID=reliance_nse,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_SELL,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=1,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test",
    clientID=clientID)
print("Place Order: ", response)


reliacnce_bse = 500325

response = xt.place_order(
    exchangeSegment="BSECM",
    exchangeInstrumentID=reliacnce_bse,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_BUY,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=1,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test_buy",
    clientID=clientID)
print("Place Order: ", response)

response = xt.place_order(
    exchangeSegment="BSECM",
    exchangeInstrumentID=reliacnce_bse,
    productType=xt.PRODUCT_MIS,
    orderType=xt.ORDER_TYPE_MARKET,
    orderSide=xt.TRANSACTION_TYPE_SELL,
    timeInForce=xt.VALIDITY_DAY,
    disclosedQuantity=0,
    orderQuantity=1,
    limitPrice=0,
    stopPrice=0,
    orderUniqueIdentifier="test",
    clientID=clientID)
print("Place Order: ", response)
