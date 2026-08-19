
from datetime import datetime
from trading.config_builder import _get_current_lut_file

tests = [
    datetime(2026,8,3,9,16,0),
    datetime(2026,8,3,9,16,59),
    datetime(2026,8,3,9,17,0),
    datetime(2026,8,3,9,17,59),
    datetime(2026,8,3,9,18,0),
    datetime(2026,8,3,9,19,0),
    datetime(2026,8,3,9,20,0),
    datetime(2026,8,3,10,45,0),
]

for t in tests:
    print("="*70)
    print(t.strftime("%H:%M:%S"))
    print(_get_current_lut_file(t))
