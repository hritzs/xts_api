import asyncio
from utils.logger import logger

class StraddlePriceGuardController:
    """
    Completely isolated optional price guard.
    Triggered strictly when entry/exit straddle target values are configured (> 0).
    """
    def __init__(self, trade_uid, db_client, data_client, order_client):
        self.trade_uid = trade_uid
        self.db = db_client
        self.data_client = data_client
        self.order_client = order_client

    @staticmethod
    def _enabled(target):
        if target in (None, "", 0, "0", 0.0):
            return False
        try:
            return float(target) > 0
        except (TypeError, ValueError):
            return False

    async def get_current_price(self):
        try:
            return await self.data_client.get_current_straddle_price(self.trade_uid)
        except Exception as e:
            logger.error(f"[{self.trade_uid}] Failed to get current straddle price: {e}", exc_info=True)
            return None

    async def verify_build_condition(self, target_entry_price):
        if not self._enabled(target_entry_price):
            return True
        target_entry_price = float(target_entry_price)
        current_price = await self.get_current_price()
        if current_price is None:
            logger.warning(f"[{self.trade_uid}] BUILD PAUSED: Current straddle price unavailable.")
            return False
        if current_price < target_entry_price:
            logger.warning(f"[{self.trade_uid}] BUILD PAUSED | Current={current_price:.2f} < Target={target_entry_price:.2f}")
            return False
        return True

    async def verify_exit_condition(self, target_exit_price):
        if not self._enabled(target_exit_price):
            return True
        target_exit_price = float(target_exit_price)
        current_price = await self.get_current_price()
        if current_price is None:
            logger.warning(f"[{self.trade_uid}] EXIT PAUSED: Current straddle price unavailable.")
            return False
        if current_price > target_exit_price:
            logger.warning(f"[{self.trade_uid}] EXIT PAUSED | Current={current_price:.2f} > Target={target_exit_price:.2f}")
            return False
        return True

    async def get_build_remaining(self, total_target_ce, total_target_pe):
        filled_status = await self.order_client.get_verified_fills_for_trade(self.trade_uid)
        filled_ce = max(0, int(filled_status.get("net_ce", 0) or 0))
        filled_pe = max(0, int(filled_status.get("net_pe", 0) or 0))
        remaining_ce = max(0, int(total_target_ce) - filled_ce)
        remaining_pe = max(0, int(total_target_pe) - filled_pe)
        return remaining_ce, remaining_pe

    async def get_exit_remaining(self):
        position_status = await self.order_client.get_open_positions_for_trade(self.trade_uid)
        open_ce = max(0, int(position_status.get("open_ce", 0) or 0))
        open_pe = max(0, int(position_status.get("open_pe", 0) or 0))
        return open_ce, open_pe

    async def execute_guarded_build_loop(self, target_entry_price, total_target_ce, total_target_pe, chunk_executor_func, poll_seconds=1.0):
        if not self._enabled(target_entry_price):
            return await chunk_executor_func(total_target_ce, total_target_pe)
        target_entry_price = float(target_entry_price)
        logger.info(f"[{self.trade_uid}] 🛡️ BUILD PRICE GUARD ENABLED | Target={target_entry_price:.2f}")
        while True:
            remaining_ce, remaining_pe = await self.get_build_remaining(total_target_ce, total_target_pe)
            if remaining_ce == 0 and remaining_pe == 0:
                logger.info(f"[{self.trade_uid}] ✅ BUILD COMPLETE | All target quantities filled.")
                return True
            safe = await self.verify_build_condition(target_entry_price)
            if not safe:
                await asyncio.sleep(poll_seconds)
                continue
            logger.info(f"[{self.trade_uid}] ▶️ BUILD RESUME | Remaining CE={remaining_ce}, PE={remaining_pe}")
            await chunk_executor_func(remaining_ce, remaining_pe)
            await asyncio.sleep(poll_seconds)

    async def execute_guarded_exit_loop(self, target_exit_price, exit_executor_func, poll_seconds=1.0):
        if not self._enabled(target_exit_price):
            open_ce, open_pe = await self.get_exit_remaining()
            if open_ce == 0 and open_pe == 0:
                return True
            return await exit_executor_func(open_ce, open_pe)
        target_exit_price = float(target_exit_price)
        logger.info(f"[{self.trade_uid}] 🛡️ EXIT PRICE GUARD ENABLED | Target={target_exit_price:.2f}")
        while True:
            open_ce, open_pe = await self.get_exit_remaining()
            if open_ce == 0 and open_pe == 0:
                logger.info(f"[{self.trade_uid}] ✅ EXIT COMPLETE | Position fully squared off.")
                return True
            safe = await self.verify_exit_condition(target_exit_price)
            if not safe:
                await asyncio.sleep(poll_seconds)
                continue
            logger.info(f"[{self.trade_uid}] ▶️ EXIT RESUME | Open CE={open_ce}, Open PE={open_pe}")
            await exit_executor_func(open_ce, open_pe)
            await asyncio.sleep(poll_seconds)
