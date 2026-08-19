from pathlib import Path
import re
import py_compile

path = Path(r"market_data/tasks.py")

text = path.read_text(encoding="utf-8")

pattern = r'''
[ \t]*if\s+bool\(new_row\.get\("is_atm"\)\):\n
(?:[ \t]+.*\n)+?
(?=[ \t]*_zero_row_derived_fields)
'''

text, count = re.subn(
    pattern,
    "",
    text,
    flags=re.MULTILINE | re.VERBOSE,
)

path.write_text(text, encoding="utf-8")

py_compile.compile(str(path), doraise=True)

print(f"✅ Removed {count} broken debug block(s)")
