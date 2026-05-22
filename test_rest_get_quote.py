# test_rest_get_quote.py
import time
import json
import logging

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
        'name': 'NIFTY Index',
        'exchangeSegment': 1,   # NSECM
        'exchangeInstrumentID': 26000
    },
    {
        'name': 'SENSEX Index',
        'exchangeSegment': 11,  # BSECM
        'exchangeInstrumentID': 26065 # Corrected Token
    },
    {
        'name': 'BANKEX Index',
        'exchangeSegment': 11,  # BSECM
        'exchangeInstrumentID': 26118 # Corrected Token from get_index_list
    },
    {
        'name': 'RELIANCE (NSE)',
        'exchangeSegment': 1,   # NSECM
        'exchangeInstrumentID': 2885
    },
    {
        'name': 'RELIANCE (BSE)',
        'exchangeSegment': 11,  # BSECM
        'exchangeInstrumentID': 500325
    }
]

# Exchange segments to test for index list
INDEX_LIST_SEGMENTS = [
    {'segment': 1,  'name': 'NSECM'},
    {'segment': 11, 'name': 'BSECM'},
    {'segment': 2,  'name': 'NSEFO'},
    {'segment': 10, 'name': 'BSEFO'},
]


def get_ltp_from_rest(xts_conn, instrument: dict):
    """
    Fetches LTP for a single instrument using a direct REST API call.
    Tries the direct LTP event (1512) first, falls back to depth event (1501).
    """
    try:
        # --- ATTEMPT 1: Use direct LTP event (1512) ---
        logging.info(f"  ➡️ Attempting to fetch LTP with xtsMessageCode = 1512 (LTP Event)...")
        response_1512 = xts_conn.get_quote(
            Instruments=[{
                'exchangeSegment': instrument['exchangeSegment'],
                'exchangeInstrumentID': instrument['exchangeInstrumentID']
            }],
            xtsMessageCode=1512,
            publishFormat='JSON'
        )
        logging.info(f"  Full API Response for {instrument['name']} (1512):")
        print(json.dumps(response_1512, indent=4))

        if response_1512 and response_1512.get('type') == 'success':
            result = response_1512.get('result', {})
            list_quotes = result.get('listQuotes', [])
            if list_quotes:
                quote_data = json.loads(list_quotes[0])
                ltp = quote_data.get('LastTradedPrice')
                if ltp and ltp > 0:
                    logging.info(f"✅ SUCCESS (1512): Found LTP for {instrument['name']}: {ltp}")
                    return ltp

        logging.warning(f"⚠️ LTP event (1512) did not yield a price for {instrument['name']}. Falling back to depth event (1501).")

        # --- ATTEMPT 2: Fallback to depth event (1501) ---
        logging.info(f"  ➡️ Attempting to fetch LTP with xtsMessageCode = 1501 (Depth Event)...")
        response_1501 = xts_conn.get_quote(
            Instruments=[{
                'exchangeSegment': instrument['exchangeSegment'],
                'exchangeInstrumentID': instrument['exchangeInstrumentID']
            }],
            xtsMessageCode=1501,
            publishFormat='JSON'
        )
        logging.info(f"  Full API Response for {instrument['name']} (1501):")
        print(json.dumps(response_1501, indent=4))

        if response_1501 and response_1501.get('type') == 'success':
            result = response_1501.get('result', {})
            list_quotes = result.get('listQuotes', [])
            if list_quotes:
                quote_data = json.loads(list_quotes[0])
                ltp = quote_data.get('LastTradedPrice')
                if ltp and ltp > 0:
                    logging.info(f"✅ SUCCESS (1501): Found LTP for {instrument['name']}: {ltp}")
                    return ltp

    except Exception as e:
        logging.error(f"❌ Exception during get_ltp_from_rest for {instrument['name']}: {e}", exc_info=True)

    return None

def main():
    """Main function to test direct REST API calls."""
    logging.info("🚀 Starting Direct REST API Data Feed Test...")

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
    quote_results = {}
    for instrument in TEST_INSTRUMENTS:
        logging.info("=" * 80)
        logging.info(
            f"🔬 TESTING: {instrument['name']} "
            f"(Token: {instrument['exchangeInstrumentID']}, Segment: {instrument['exchangeSegment']})"
        )
        ltp = get_ltp_from_rest(xt_m, instrument)
        quote_results[instrument['name']] = 'SUCCESS' if ltp else 'FAILED'
        time.sleep(1)

    # 3. Print final summary
    logging.info("=" * 80)
    logging.info("📊 FINAL TEST SUMMARY")
    logging.info("=" * 80)

    logging.info("  [get_quote (LTP) Results]")
    for name, status in quote_results.items():
        if status == 'SUCCESS':
            logging.info(f"  - {name:<20} ✅ {status}")
        else:
            logging.error(f"  - {name:<20} ❌ {status}")

    logging.info("=" * 80)
    logging.info("👋 Test finished.")


if __name__ == "__main__":
    main()
