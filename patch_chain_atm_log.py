from pathlib import Path
import py_compile

path = Path(r"market_data/tasks.py")
if not path.exists():
    print("❌ market_data/tasks.py not found.")
    raise SystemExit(1)

text = path.read_text(encoding="utf-8")

target = '''                                logger.info(
                                    f"[CHAIN PUBLISH] "
                                    f"Symbol={symbol} "
                                    f"Seq={published.get('publish_seq')} "
                                    f"Published={published.get('published_at')} "
                                    f"Rows={len(published.get('chain', []))}"
                                )'''

replacement = '''                                logger.info(
                                    f"[CHAIN PUBLISH] "
                                    f"Symbol={symbol} "
                                    f"Seq={published.get('publish_seq')} "
                                    f"Published={published.get('published_at')} "
                                    f"Rows={len(published.get('chain', []))}"
                                )
                                atm = published.get("atm")
                                row = next(
                                    (r for r in published.get("chain", [])
                                     if r.get("strike") == atm),
                                    None,
                                )
                                if row:
                                    logger.info(
                                        "[CHAIN ATM] "
                                        f"ATM={atm} "
                                        f"CE_TS={row.get('ce_quote_ts')} "
                                        f"PE_TS={row.get('pe_quote_ts')} "
                                        f"CE={row.get('ce_ltp')} "
                                        f"PE={row.get('pe_ltp')}"
                                    )'''

if target not in text:
    print("❌ Could not locate publish logger target block.")
    raise SystemExit(1)

text = text.replace(target, replacement, 1)
path.write_text(text, encoding="utf-8")
py_compile.compile(str(path), doraise=True)
print("✅ Added ATM publish logging successfully.")
