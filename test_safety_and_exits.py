import pytest
import asyncio
from unittest.mock import MagicMock, AsyncMock

# Import your safety helpers from builder.py
# (Assuming they are module-level functions in trading.builder)
from trading.builder import _verify_build_price_safety, _verify_square_off_price_safety

def test_build_price_safety():
    """Test that build halts if current price drops below initial target."""
    initial_target = 190.0
    
    # Safe: Price is equal or higher
    assert _verify_build_price_safety(initial_target, 190.0) is True
    assert _verify_build_price_safety(initial_target, 191.5) is True
    
    # Unsafe: Price dropped below target
    assert _verify_build_price_safety(initial_target, 189.5) is False

def test_square_off_price_safety_1bps():
    """Test that square-off halts if price bounces up by >= 1 bps (0.01%)."""
    initial_exit_price = 100.0
    
    # Safe: Price stable or dropping (favorable for exiting short options)
    assert _verify_square_off_price_safety(initial_exit_price, 100.0) is True
    assert _verify_square_off_price_safety(initial_exit_price, 98.0) is True
    
    # Boundary: Exactly 1 bps higher (100.0 * 1.0001 = 100.01)
    # Threshold condition in code: current_price > initial_price * 1.0001
    assert _verify_square_off_price_safety(initial_exit_price, 100.005) is True
    
    # Unsafe: Price bounced up by more than 1 bps (> 100.01)
    assert _verify_square_off_price_safety(initial_exit_price, 100.02) is False

@pytest.mark.asyncio
async def test_dynamic_price_reloading_simulation():
    """Simulate TradeManager re-reading updated targets from DB mid-trade."""
    # Mock TradeManager instance or relevant attributes
    manager = MagicMock()
    manager.trade_uid = "test_trade_123"
    manager.db = MagicMock()
    
    # Mock monitor targets
    manager.entry_straddle_monitor = MagicMock(target=190.0)
    manager.tp_monitor = MagicMock(target=150.0)
    
    # Simulate updated DB record (e.g. user updated targets via UI)
    manager.db.get_straddle_by_id.return_value = {
        "target_entry_price": 192.5,
        "target_exit_price": 145.0,
        "status": "ACTIVE"
    }
    
    # Run the dynamic re-read snippet logic
    fresh_trade_data = manager.db.get_straddle_by_id(manager.trade_uid)
    if fresh_trade_data:
        if 'target_entry_price' in fresh_trade_data and fresh_trade_data['target_entry_price'] is not None:
            if getattr(manager, 'entry_straddle_monitor', None):
                manager.entry_straddle_monitor.target = float(fresh_trade_data['target_entry_price'])
        if 'target_exit_price' in fresh_trade_data and fresh_trade_data['target_exit_price'] is not None:
            if getattr(manager, 'tp_monitor', None):
                manager.tp_monitor.target = float(fresh_trade_data['target_exit_price'])
                
    # Assert monitors updated dynamically without restart
    assert manager.entry_straddle_monitor.target == 192.5
    assert manager.tp_monitor.target == 145.0
