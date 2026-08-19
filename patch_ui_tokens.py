import os

js_path = 'static/straddles.js'
if not os.path.exists(js_path):
    js_path = 'static/js/straddles.js'

if os.path.exists(js_path):
    with open(js_path, 'r', encoding='utf-8') as f:
        code = f.read()

    # Fallback mapper for tokens if ce_token/pe_token are missing at the root level
    old_snippet = '''        const ce_ltp_display = isClosed || isPending
            ? '—' : \₹\\;
        const pe_ltp_display = isClosed || isPending
            ? '—' : \₹\\;'''

    new_snippet = '''        // Fallback token extraction from live_positions if root tokens are missing
        let ceTok = straddle.ce_token;
        let peTok = straddle.pe_token;
        if ((!ceTok || !peTok) && straddle.live_positions) {
            straddle.live_positions.forEach(p => {
                if (p.option_type === 'CE' && !ceTok) ceTok = p.token || p.exchangeInstrumentID;
                if (p.option_type === 'PE' && !peTok) peTok = p.token || p.exchangeInstrumentID;
            });
        }
        const ce_ltp_display = isClosed || isPending
            ? '—' : \₹\\;
        const pe_ltp_display = isClosed || isPending
            ? '—' : \₹\\;'''

    if old_snippet in code:
        code = code.replace(old_snippet, new_snippet)
        with open(js_path, 'w', encoding='utf-8') as f:
            f.write(code)
        print(f"✅ Successfully patched {js_path} token mapping fallback!")
    else:
        print("⚡ Snippet already modified or structure differs slightly. Trying secondary match...")
        # Fallback loose replacement if indentation varies
        if 'straddle.ce_token' in code and 'priceMap' in code:
            code = code.replace('priceMap[straddle.ce_token]', 'priceMap[straddle.ce_token || (straddle.live_positions?.find(p => p.option_type === "CE")?.token)]')
            code = code.replace('priceMap[straddle.pe_token]', 'priceMap[straddle.pe_token || (straddle.live_positions?.find(p => p.option_type === "PE")?.token)]')
            with open(js_path, 'w', encoding='utf-8') as f:
                f.write(code)
            print(f"✅ Successfully applied secondary loose patch to {js_path}!")
        else:
            print("❌ Could not match snippet automatically.")
else:
    print("❌ static/straddles.js not found.")
