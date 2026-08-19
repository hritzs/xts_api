import unittest
from unittest.mock import AsyncMock

from models.state import state
from trading.trade_manager import TradeManager


class TradeManagerMonitoringTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        state.condition_tasks = {}

    async def test_run_condition_tasks_checks_entry_and_exit_monitors(self):
        manager = TradeManager("test_trade", {"config": {}})
        manager.entry_straddle_monitor.check = AsyncMock()
        manager.straddle_price_monitor.check = AsyncMock()

        await manager.run_condition_tasks()

        manager.entry_straddle_monitor.check.assert_awaited_once()
        manager.straddle_price_monitor.check.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
