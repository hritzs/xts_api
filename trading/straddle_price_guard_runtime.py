from trading.straddle_price_guard import StraddlePriceGuardController
from trading.data_client import get_option_chain_from_service

class LiveStraddleDataClient:
    def __init__(self, symbol):
        self.symbol = str(symbol or "NIFTY").upper()

    async def get_current_straddle_price(self, trade_uid):
        chain = await get_option_chain_from_service(self.symbol)
        if not chain:
            return None
        atm = chain.get("atm")
        rows = chain.get("chain", [])
        atm_row = None
        for row in rows:
            if row.get("strike") == atm or str(row.get("strike")) == str(atm):
                atm_row = row
                break
        if not atm_row:
            return None

        def safe_float(*values):
            for value in values:
                try:
                    if value is not None and value != "":
                        return float(value)
                except (TypeError, ValueError):
                    continue
            return 0.0

        ce_ltp = safe_float(atm_row.get("ce_ltp"), atm_row.get("ceLtp"), atm_row.get("call_ltp"), atm_row.get("callLtp"), atm_row.get("CE_LTP"))
        pe_ltp = safe_float(atm_row.get("pe_ltp"), atm_row.get("peLtp"), atm_row.get("put_ltp"), atm_row.get("putLtp"), atm_row.get("PE_LTP"))
        price = ce_ltp + pe_ltp
        return price if price > 0 else None

def _target_enabled(target):
    if target in (None, "", 0, 0.0, "0"):
        return False
    try:
        return float(target) > 0
    except (TypeError, ValueError):
        return False

async def build_chunk_price_allowed(trade_uid, symbol, target_entry_price):
    if not _target_enabled(target_entry_price):
        return True
    guard = StraddlePriceGuardController(trade_uid=trade_uid, db_client=None, data_client=LiveStraddleDataClient(symbol), order_client=None)
    return await guard.verify_build_condition(target_entry_price)

async def exit_chunk_price_allowed(trade_uid, symbol, target_exit_price):
    if not _target_enabled(target_exit_price):
        return True
    guard = StraddlePriceGuardController(trade_uid=trade_uid, db_client=None, data_client=LiveStraddleDataClient(symbol), order_client=None)
    return await guard.verify_exit_condition(target_exit_price)
