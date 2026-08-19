from pathlib import Path
import py_compile

path = Path(r"market_data/tasks.py")
text = path.read_text(encoding="utf-8")

old = '''
                    logger.info(
                        f"[GREEKS PUBLISH] "
                        f"Symbol={symbol} "
                        f"Seq={published.get('publish_seq')} "
                        f"ATM={published.get('atm')} "
                        f"Rows={len(published.get('chain', []))}"
                    )
'''

new = '''
                    logger.info(
                        f"[GREEKS PUBLISH] "
                        f"Symbol={symbol} "
                        f"Seq={published.get('publish_seq')} "
                        f"ATM={published.get('atm')} "
                        f"Rows={len(published.get('chain', []))}"
                    )

                    atm = published.get("atm")
                    atm_row = next(
                        (
                            r
                            for r in published.get("chain", [])
                            if r.get("strike") == atm
                        ),
                        None,
                    )

                    if atm_row:
                        logger.info(
                            "[GREEKS ATM] "
                            f"CE_IV={atm_row.get('ce_iv')} "
                            f"PE_IV={atm_row.get('pe_iv')} "
                            f"CE_DELTA={atm_row.get('ce_delta')} "
                            f"PE_DELTA={atm_row.get('pe_delta')} "
                            f"CE_THETA={atm_row.get('ce_theta')} "
                            f"PE_THETA={atm_row.get('pe_theta')} "
                            f"CE_VEGA={atm_row.get('ce_vega')} "
                            f"PE_VEGA={atm_row.get('pe_vega')}"
                        )
'''

count = text.count(old)

if count != 1:
    print(f"❌ Expected exactly one publish logger, found {count}.")
    raise SystemExit(1)

text = text.replace(old, new, 1)

path.write_text(text, encoding="utf-8")
py_compile.compile(str(path), doraise=True)

print("✅ Added ATM Greeks logging.")
