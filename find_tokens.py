"""
find_tokens.py
Run this once to find the correct cash index tokens for all symbols on your XTS broker.
Usage: python find_tokens.py
"""
import sys
import csv
import io
import json

# ── Import your existing XTS login setup ─────────────────────────────────────
# Adjust the import path to match your project structure
try:
    from trading.chain_provider import get_xts_market_api, xt_m
    market_api = xt_m
except ImportError:
    market_api = None

if market_api is None:
    print("❌ Could not import xt_m. Trying manual login...")
    try:
        # --- FIX: Use the project's central cred.py and config.ini ---
        import cred
        from Connect import XTSConnect
        import configparser

        cfg = configparser.ConfigParser()
        cfg.read('config.ini')
        # The root URL must be read from config.ini, as it's not available in config.py
        root_url = cfg.get('root_url', 'root')

        # Use the Market Data API credentials from cred.py
        xt = XTSConnect(apiKey=cred.API_KEY_M, secretKey=cred.API_SECRET_M, source="WEBAPI", root=root_url)
        resp = xt.marketdata_login()
        if resp.get('type') != 'success':
            print(f"❌ Login failed: {resp}")
            sys.exit(1)
        print(f"✅ Logged in. Token: {resp['result']['token'][:10]}...")
        market_api = xt
    except (ImportError, configparser.Error, KeyError) as e:
        print(f"❌ Login error: Could not load credentials or config. Make sure cred.py and config.ini are correct.")
        print(f"   Details: {e}")
        sys.exit(1)


# ── Symbols we care about ─────────────────────────────────────────────────────
TARGET_KEYWORDS = {
    'NIFTY 50':        'NIFTY',
    'NIFTY BANK':      'BANKNIFTY',
    'NIFTY FIN':       'FINNIFTY',
    'NIFTY MID':       'MIDCPNIFTY',
    'SENSEX':          'SENSEX',
    'BANKEX':          'BANKEX',
    'NIFTY50':         'NIFTY',
    'BANKNIFTY':       'BANKNIFTY',
    'FINNIFTY':        'FINNIFTY',
    'MIDCPNIFTY':      'MIDCPNIFTY',
    'MIDCAP SELECT':   'MIDCPNIFTY',
    'NIFTY MIDCAP':    'MIDCPNIFTY',
}

SEGMENTS = {
    1:  "NSECM  (NSE Cash/Index)",
    11: "BSECM  (BSE Cash/Index)",
    2:  "NSEFO  (NSE F&O)",
    12: "BSEFO  (BSE F&O)",
}

INSTRUMENT_TYPES = ['INDEX', 'UNDIND', 'FUTIDX', 'OPTIDX']

# --- NEW: Heuristics for better matching ---
PRICE_RANGES = {
    'NIFTY':      (15000, 35000),
    'BANKNIFTY':  (30000, 80000),
    'FINNIFTY':   (15000, 35000),
    'MIDCPNIFTY': (5000,  20000),
    'SENSEX':     (50000, 100000),
    'BANKEX':     (40000, 80000),
}

EXACT_NAMES = {
    'NIFTY':      'NIFTY 50',
    'BANKNIFTY':  'NIFTY BANK',
    'FINNIFTY':   'NIFTY FIN SERVICE',
    'MIDCPNIFTY': 'NIFTY MIDCAP SELECT',
    'SENSEX':     'SENSEX',
    'BANKEX':     'BANKEX',
}

def get_score(match, ltp):
    """Scores a potential match. Lower is better."""
    score = 100
    sym = match['symbol']
    name = match['name'].upper()

    min_price, max_price = PRICE_RANGES.get(sym, (0, float('inf')))
    if not (min_price < ltp < max_price):
        return float('inf')

    if name == EXACT_NAMES.get(sym, '').upper():
        score -= 50
    if match['type'] in ['INDEX', 'UNDIND']:
        score -= 20
    score += len(name.split()) - len(EXACT_NAMES.get(sym, '').split())
    return score


def fetch_master(segment_list):
    print(f"\n📡 Fetching master for: {segment_list}  (may take a few seconds)...")
    try:
        resp = market_api.get_master(exchangeSegmentList=segment_list)
        if not resp or resp.get('type') != 'success':
            print(f"  ⚠️  get_master failed: {resp}")
            return []
        lines = resp['result'].strip().split('\n')
        print(f"  ✅ Got {len(lines)} instruments")
        return lines
    except Exception as e:
        print(f"  ❌ Error: {e}")
        return []


def parse_and_search(lines, segment_id):
    """
    XTS master pipe-delimited format:
    col[0]=ExchangeSegment  col[1]=ExchangeInstrumentID  col[2]=InstrumentType
    col[3]=Name/Series      col[4]=...
    """
    matches = []
    for line in lines:
        cols = line.split('|')
        if len(cols) < 4:
            continue

        instrument_id   = cols[1].strip()
        instrument_type = cols[2].strip().upper()
        name            = cols[3].strip().upper()

        # Only care about index-type instruments
        if instrument_type not in INSTRUMENT_TYPES:
            continue

        for keyword, symbol in TARGET_KEYWORDS.items():
            if keyword.upper() in name:
                matches.append({
                    'segment':    segment_id,
                    'token':      instrument_id,
                    'type':       instrument_type,
                    'name':       cols[3].strip(),
                    'symbol':     symbol,
                    'raw':        line
                })
                break  # avoid duplicate matches per line

    return matches


def search_by_script(name):
    """Alternative: search directly by script name if get_master is slow."""
    results = []
    try:
        # Call once per name, without segment. The API searches globally.
        resp = market_api.search_by_scriptname(searchString=name)
        if resp and resp.get('type') == 'success':
            for item in resp.get('result', []):
                # The item from the response contains the segment
                segment_from_item = item.get('ExchangeSegment')
                # Only care about cash segments (1=NSECM, 11=BSECM)
                if segment_from_item in [1, 11]:
                    # --- FIX: Determine symbol from keywords, not search term ---
                    item_name_upper = item.get('Name', '').upper()
                    found_symbol = None
                    best_match_len = 0
                    for keyword, symbol_val in TARGET_KEYWORDS.items():
                        if keyword.upper() in item_name_upper:
                            if len(keyword) > best_match_len:
                                found_symbol = symbol_val
                                best_match_len = len(keyword)
                    
                    if found_symbol:
                        results.append({
                            'segment': segment_from_item,
                            'token':   item.get('ExchangeInstrumentID'),
                            'type':    item.get('InstrumentType', ''),
                            'name':    item.get('Name', name),
                            'symbol':  found_symbol, # Use the symbol from TARGET_KEYWORDS
                        })
    except Exception as e:
        print(f"  ⚠️  search_by_scriptname({name}) failed: {e}")
    return results


def verify_token_ltp(token, segment):
    """Quick verification: does this token return a valid LTP?"""
    try:
        resp = market_api.get_quote(
            Instruments=[{"exchangeSegment": segment, "exchangeInstrumentID": int(token)}],
            xtsMessageCode=1512,
            publishFormat='JSON'
        )
        if resp and resp.get('type') == 'success':
            quotes = resp['result'].get('listQuotes', [])
            if quotes:
                q = json.loads(quotes[0]) if isinstance(quotes[0], str) else quotes[0]
                ltp = float(q.get('IndexValue', 0) or q.get('LastTradedPrice', 0))
                return ltp

        # Retry with 1502 for index
        resp = market_api.get_quote(
            Instruments=[{"exchangeSegment": segment, "exchangeInstrumentID": int(token)}],
            xtsMessageCode=1502,
            publishFormat='JSON'
        )
        if resp and resp.get('type') == 'success':
            quotes = resp['result'].get('listQuotes', [])
            if quotes:
                q = json.loads(quotes[0]) if isinstance(quotes[0], str) else quotes[0]
                ltp = float(q.get('IndexValue', 0) or q.get('LastTradedPrice', 0))
                return ltp
    except Exception:
        pass
    return 0.0


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "="*65)
print("  XTS Cash Index Token Finder")
print("="*65)

all_matches = []

# Step 1: get_master for cash segments
for seg_id, seg_label in [(1, "NSECM"), (11, "BSECM")]:
    lines = fetch_master([seg_label])
    if lines:
        matches = parse_and_search(lines, seg_id)
        all_matches.extend(matches)

# Step 2: If get_master returned nothing useful, fallback to search_by_scriptname
if not all_matches:
    print("\n⚠️  get_master returned no matches. Falling back to search_by_scriptname...")
    # --- FIX: Use broader search terms to cover all indices ---
    SEARCH_TERMS = ['NIFTY', 'SENSEX', 'BANKEX', 'BANK', 'FIN', 'MIDCAP']
    for name in SEARCH_TERMS:
        results = search_by_script(name)
        all_matches.extend(results)

# Step 3: Deduplicate
seen = set()
unique_matches = []
for m in all_matches:
    key = (m['segment'], m['token'])
    if key not in seen:
        seen.add(key)
        unique_matches.append(m)

# Step 4: Verify LTPs
print(f"\n🔍 Found {len(unique_matches)} candidate instruments. Verifying LTPs...\n")
print(f"{'Symbol':<15} {'Token':<10} {'Seg':<6} {'Type':<10} {'LTP':>12}  Name")
print("-" * 75)

verified = {} # symbol -> { 'score': score, 'match': match_data }
for m in sorted(unique_matches, key=lambda x: x['symbol']):
    ltp = verify_token_ltp(m['token'], m['segment'])
    status = f"₹{ltp:,.2f}" if ltp > 0 else "  NO DATA"
    print(f"{m['symbol']:<15} {m['token']:<10} {m['segment']:<6} {m['type']:<10} {status:>12}  {m['name']}")

    if ltp > 0:
        sym = m['symbol']
        score = get_score(m, ltp)

        if score == float('inf'):
            continue

        if sym not in verified or score < verified[sym]['score']:
            verified[sym] = {
                'score': score,
                'match': {**m, 'ltp': ltp}
            }

# Step 5: Print SYMBOL_CONFIG patch
print("\n" + "="*65)
print("  ✅ RECOMMENDED SYMBOL_CONFIG UPDATES")
print("="*65)

# Match this with trading/chain_provider.py for an accurate diff
CURRENT = {
    'NIFTY':      (26000, 1),
    'BANKNIFTY':  (26001, 1),
    'FINNIFTY':   (26034, 1),
    'MIDCPNIFTY': (26121, 1),
    'SENSEX':     (26065, 11),
    'BANKEX':     (26118, 11),
}

for sym, data in sorted(verified.items()):
    match_data = data['match']
    old_token, old_seg = CURRENT.get(sym, (None, None))
    new_token = int(match_data['token'])
    new_seg   = match_data['segment']
    changed   = (new_token != old_token or new_seg != old_seg)
    marker    = " ← CHANGE THIS" if changed else " (same)"
    print(f"  '{sym}': cash_index_token={new_token}, cash_index_segment={new_seg}  |  LTP=₹{match_data['ltp']:,.2f}{marker}")

print()
print("Paste the changed tokens into SYMBOL_CONFIG in trading/chain_provider.py")
print("="*65)
