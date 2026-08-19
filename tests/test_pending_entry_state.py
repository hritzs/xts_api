from models.state import DashboardState


def test_pending_entry_context_is_available_via_shared_trade_cache():
    state = DashboardState()
    state.trade_data_cache = {}

    state.set_pending_entry_context("trade-123", {"trade_uid": "trade-123"})

    assert state.pending_entry_builds["trade-123"]["trade_uid"] == "trade-123"
    assert state.trade_data_cache["__pending_entry_builds__"]["trade-123"]["trade_uid"] == "trade-123"
    assert state.get_pending_entry_context("trade-123")["trade_uid"] == "trade-123"

    popped = state.pop_pending_entry_context("trade-123")
    assert popped["trade_uid"] == "trade-123"
    assert "trade-123" not in state.pending_entry_builds
    assert "trade-123" not in state.trade_data_cache["__pending_entry_builds__"]
