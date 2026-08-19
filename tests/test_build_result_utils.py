from trading.build_result_utils import get_trade_uid_from_build_result, is_pending_entry_result


def test_pending_entry_result_returns_trade_uid():
    result = {
        "success": True,
        "pending_entry": True,
        "trade_uid": "ny123",
        "current_straddle": 100.0,
        "target_straddle": 120.0,
    }

    assert is_pending_entry_result(result) is True
    assert get_trade_uid_from_build_result(result) == "ny123"


def test_normal_success_result_uses_straddle_payload():
    result = {
        "success": True,
        "straddle_data": {"trade_uid": "ny456"},
    }

    assert is_pending_entry_result(result) is False
    assert get_trade_uid_from_build_result(result) == "ny456"
