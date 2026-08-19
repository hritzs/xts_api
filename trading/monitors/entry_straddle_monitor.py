import time
from typing import Dict
from utils.logger import logger
from models.state import state
from utils.helpers import get_ist_now, _safe_float
from trading.builder import execute_prepared_build
from trading.data_client import get_option_chain_from_service
class EntryStraddleMonitor:
    def __init__(self, trade_uid: str, config: Dict):
        self.trade_uid = trade_uid
        self.config = config
        self.running = False
        self.target_premium = 0.0
        
        entry_target = self.config.get("entry_at_straddle")
        if entry_target not in ("", None, 0, "0"):
            try:
                self.target_premium = float(entry_target)
            except Exception:
                pass

        self.interval = float(
            config.get('entry_monitor_interval', 1.0)
        )

        # Never allow the entry monitor to poll slower than 1 second.
        # This affects ONLY EntryStraddleMonitor.
        if self.interval > 1.0:
            self.interval = 1.0
        self._last_check_time = 0.0
        logger.info(f"✅ EntryStraddleMonitor initialized: {trade_uid} | Target: {self.target_premium} | Interval: {self.interval}s")

    async def start(self):
        self.running = True
        self._last_check_time = 0.0
        logger.info(f"🎯 EntryStraddleMonitor enabled for {self.trade_uid}")

    async def stop(self):
        self.running = False
        logger.info(f"🛑 EntryStraddleMonitor stopped for {self.trade_uid}")

    async def check(self):
        logger.debug(f"[ENTRY CHECK EXECUTING] "
            f"trade={self.trade_uid} "
            f"running={self.running} "
            f"target={self.target_premium}"
        )

        if not self.running:
            logger.warning(f"[ENTRY] Monitor not running for {self.trade_uid}")
            return

        if self.target_premium <= 0:
            logger.warning(f"[ENTRY] Invalid target premium {self.target_premium}")
            await execute_prepared_build(self.trade_uid)
            await self.stop()
            return

        import time
        now_mono = time.monotonic()
        if self._last_check_time > 0 and (now_mono - self._last_check_time < self.interval):
            return

        self._last_check_time = now_mono

        trade_data = state.db.get_straddle_by_id(self.trade_uid)
        logger.info(
            f"[ENTRY] "
            f"DB Status="
            f"{trade_data.get('status') if trade_data else 'NONE'}"
        )
        if not trade_data or trade_data.get('status') != 'PENDING_ENTRY':
            logger.info(f"[ENTRY] Trade {self.trade_uid} status changed or not found. Stopping.")
            await self.stop()
            return

        symbol = trade_data.get("symbol")
        
        # STEP 1 : Local cache lookup
        chain_data = state.get_published_option_chain(symbol)
        if chain_data:
            logger.info(f"[ENTRY CACHE HIT] {symbol}")

        # STEP 2 : Cache miss fallback
        if not chain_data:
            logger.info(f"[ENTRY CACHE MISS] {symbol}")
            try:
                fetched_chain = await get_option_chain_from_service(symbol)
                if fetched_chain:
                    logger.info(f"[ENTRY SERVICE FETCH SUCCESS] {symbol}")
                    if hasattr(state, "publish_option_chain"):
                        chain_data = state.publish_option_chain(symbol, fetched_chain)
                    else:
                        chain_data = fetched_chain
                else:
                    logger.warning(f"[ENTRY SERVICE EMPTY] {symbol}")
            except Exception as e:
                logger.exception(f"[ENTRY SERVICE ERROR] {symbol} {e}")

        logger.info(f"[ENTRY CHAIN RESULT] {'FOUND' if chain_data else 'NONE'} {symbol}")
        
        if not chain_data:
            logger.warning(f"[{self.trade_uid}] [ENTRY POLL] No published chain available for {symbol}. Waiting...")
            return

        atm = chain_data.get("atm")
        if not atm:
            return

        atm_row = next((r for r in chain_data.get("chain", []) if r.get("strike") == atm), None)
        if not atm_row:
            return

        logger.info(f"[ENTRY ATM ROW DUMP] {atm_row}")
        
        # Robust key fallback for CE/PE LTP (supporting ltp, price, or ltp keys)
        from utils.helpers import _safe_float
        ce_ltp = _safe_float(atm_row.get("ce_ltp") or atm_row.get("ce_price") or atm_row.get("ce_ltp_price", 0.0))
        pe_ltp = _safe_float(atm_row.get("pe_ltp") or atm_row.get("pe_price") or atm_row.get("pe_ltp_price", 0.0))
        
        # If still 0, try looking up via token in shared memory prices array
        if ce_ltp == 0.0 and atm_row.get("ce_token"):
            try:
                ce_token = int(atm_row.get("ce_token"))
                if hasattr(state, "shared_data") and state.shared_data and hasattr(state.shared_data, "get_price_for_token"):
                    ce_ltp = _safe_float(state.shared_data.get_price_for_token(ce_token))
            except Exception:
                pass
                
        if pe_ltp == 0.0 and atm_row.get("pe_token"):
            try:
                pe_token = int(atm_row.get("pe_token"))
                if hasattr(state, "shared_data") and state.shared_data and hasattr(state.shared_data, "get_price_for_token"):
                    pe_ltp = _safe_float(state.shared_data.get_price_for_token(pe_token))
            except Exception:
                pass

        current_straddle = ce_ltp + pe_ltp
        logger.info(
            f"[ENTRY] "
            f"Premium={current_straddle:.2f} "
            f"Target={self.target_premium:.2f}"
        )

        if current_straddle >= self.target_premium:
            logger.info(f"[ENTRY HIT] {self.trade_uid}")
            logger.info(f"✅ [ENTRY HIT] Target reached ({current_straddle:.2f} >= {self.target_premium:.2f}). Executing build.")
            await execute_prepared_build(self.trade_uid)
            await self.stop()
        else:
            logger.info(f"⏳ [WAITING FOR ENTRY] Current ({current_straddle:.2f}) < Target ({self.target_premium:.2f})")
