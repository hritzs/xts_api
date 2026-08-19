import re, os

fpath = 'trading/builder.py'
if not os.path.exists(fpath):
    print(f"Error: {fpath} not found.")
    exit(1)

with open(fpath, 'r', encoding='utf-8') as f:
    code = f.read()

# The ultimate "Kitchen Sink" dictionary that guarantees no KeyErrors in the DB
full_pending_data = '''pending_data = {
                    'straddle_id': trade_uid,
                    'trade_uid': trade_uid,
                    'symbol': symbol,
                    'strike': atm,
                    'expiry': chain_data.get('expiry', ''),
                    'expiry_date': chain_data.get('expiry_date'),
                    'chain_publish_seq': chain_data.get('publish_seq', 0),
                    'chain_published_at': chain_data.get('published_at', ''),
                    'exchange_segment': exchange_segment,
                    'exchange_name': exchange_name,
                    'product_type': product_type,
                    'lot_size': lot_size,
                    'lots': lots,
                    'initial_pe_quantity': 0,
                    'initial_ce_quantity': 0,
                    'pe_lots': 0,
                    'ce_lots': 0,
                    'pe_quantity': 0,
                    'ce_quantity': 0,
                    'quantity': 0,
                    'total_quantity': 0,
                    'ce_token': ce_token,
                    'ce_symbol': ce_symbol,
                    'ce_entry_price': 0.0,
                    'ce_delta': 0.5,
                    'ce_gamma': 0.0,
                    'ce_theta': 0.0,
                    'ce_vega': 0.0,
                    'ce_iv': 0.0,
                    'pe_token': pe_token,
                    'pe_symbol': pe_symbol,
                    'pe_entry_price': 0.0,
                    'pe_delta': -0.5,
                    'pe_gamma': 0.0,
                    'pe_theta': 0.0,
                    'pe_vega': 0.0,
                    'pe_iv': 0.0,
                    'net_delta': 0.0,
                    'delta_neutral': delta_neutral,
                    'total_premium': 0.0,
                    'status': 'PENDING_ENTRY',
                    'execution_time': 0.0,
                    'entry_spot': 0.0,
                    'spot_price': 0.0,
                    'fut_token': chain_data.get('fut_token'),
                    'entry_timestamp': get_ist_now().isoformat(),
                    'closed_at': None,
                    'config': trade_config or {},
                    'ce_orders': [],
                    'pe_orders': [],
                    'all_verified_orders': [],
                }'''

# Find the exact pending_data dictionary and overwrite it entirely
code = re.sub(r"pending_data\s*=\s*\{[^\}]*'status':\s*'PENDING_ENTRY'[^\}]*\}", full_pending_data, code, flags=re.DOTALL)

with open(fpath, 'w', encoding='utf-8') as f:
    f.write(code)

print("Successfully injected the bulletproof pending_data payload into builder.py!")
