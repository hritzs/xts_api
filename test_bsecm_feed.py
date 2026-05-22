# test_bsecm_feed.py

import time
import json
import logging

# Configure logging to be clear and informative
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

try:
    from Connect import XTSConnect
    import cred
except ImportError as e:
    print(f"Error: Missing required libraries. Please ensure Connect.py and cred.py are in the same directory. Details: {e}")
    exit()

# --- Configuration ---
TEST_INSTRUMENTS = [
    {
        'name': 'SENSEX',
        'exchangeSegment': 11,  # BSECM
        'exchangeInstrumentID': 1
    },
    {
        'name': 'BANKEX',
        'exchangeSegment': 11,  # BSECM
        'exchangeInstrumentID': 12
    },
    {
        'name': 'NIFTY',
        'exchangeSegment': 1,   # NSECM
        'exchangeInstrumentID': 26000
    }
]

# Message codes to subscribe to, to see what data is available
# 1501: Market Depth (Bid/Ask)
# 1502: Index Data (Often used for indices like SENSEX)
# 1512: LTP (Last Traded Price)
MESSAGE_CODES_TO_TEST = [1512, 1501, 1502]

# --- Main Logic ---

def main():
    """Main function to test REST API calls for BSECM."""
    logging.info("🚀 Starting BSECM REST API Data Feed Test...")

    # 1. Login to Market Data API
    try:
        xt_m = XTSConnect(cred.API_KEY_M, cred.API_SECRET_M, source="WEBAPI")
        response_m = xt_m.marketdata_login()
        if response_m.get('type') != 'success':
            logging.error(f"❌ Market data login failed: {response_m.get('description')}")
            return
        logging.info("✅ Market Data API Login Successful.")
    except Exception as e:
        logging.error(f"❌ Exception during login: {e}")
        return

    # 2. Test get_quote for each instrument
    for instrument in TEST_INSTRUMENTS:
        logging.info("=" * 80)
        logging.info(f"🔬 TESTING INSTRUMENT: {instrument['name']} (Token: {instrument['exchangeInstrumentID']}, Segment: {instrument['exchangeSegment']})")
        logging.info("=" * 80)

        for code in MESSAGE_CODES_TO_TEST:
            logging.info("-" * 60)
            logging.info(f"➡️ Testing get_quote with xtsMessageCode = {code}...")
            try:
                response = xt_m.get_quote(
                    Instruments=[{'exchangeSegment': instrument['exchangeSegment'], 'exchangeInstrumentID': instrument['exchangeInstrumentID']}],
                    xtsMessageCode=code,
                    publishFormat='JSON'
                )
                
                logging.info(f"Response for code {code}:")
                # Pretty print the JSON response
                print(json.dumps(response, indent=4))

                # Try to parse and find a value
                if response and response.get('type') == 'success':
                    result = response.get('result', {})
                    list_quotes = result.get('listQuotes', [])
                    if list_quotes:
                        quote_data = json.loads(list_quotes[0])
                        ltp = quote_data.get('LastTradedPrice')
                        index_val = quote_data.get('IndexValue')
                        bid = quote_data.get('BidInfo', {}).get('Price')
                        ask = quote_data.get('AskInfo', {}).get('Price')

                        if ltp and ltp > 0:
                            logging.info(f"✅ SUCCESS (Code {code}): Found LastTradedPrice: {ltp}")
                        elif index_val and index_val > 0:
                            logging.info(f"✅ SUCCESS (Code {code}): Found IndexValue: {index_val}")
                        elif bid and ask and bid > 0 and ask > 0:
                             logging.info(f"✅ SUCCESS (Code {code}): Found Bid/Ask: {bid}/{ask}")
                        else:
                            logging.warning(f"⚠️ Code {code} returned success, but no usable price data found in the first quote.")
                    else:
                        logging.warning(f"⚠️ Code {code} returned success, but 'listQuotes' was empty.")
                else:
                    logging.error(f"❌ Request for code {code} failed or returned an error type.")

            except Exception as e:
                logging.error(f"❌ Exception during get_quote for code {code}: {e}")
            
            time.sleep(0.5) # Small delay between API calls

    # 3. Test get_index_list for each relevant exchange
    for exchange in ['BSECM', 'NSECM']:
        logging.info("=" * 80)
        logging.info(f"🔬 TESTING get_index_list for {exchange}...")
        logging.info("=" * 80)
        try:
            index_list_response = xt_m.get_index_list(exchangeSegment=exchange)
            logging.info(f"Response from get_index_list(exchangeSegment='{exchange}'):")
            print(json.dumps(index_list_response, indent=4))

            if index_list_response.get('type') == 'success':
                index_list = index_list_response.get('result', [])
                if isinstance(index_list, list) and index_list:
                    logging.info(f"✅ SUCCESS: get_index_list for {exchange} returned {len(index_list)} indices.")
                else:
                    # Log a warning if the structure is not a list, as seen in your logs
                    logging.warning(f"⚠️ get_index_list for {exchange} returned success, but the result was empty or not a list.")
        except Exception as e:
            logging.error(f"❌ Exception during get_index_list for {exchange}: {e}")
        time.sleep(1)

    logging.info("-" * 60)
    logging.info("👋 Test finished.")

if __name__ == "__main__":
    main()
