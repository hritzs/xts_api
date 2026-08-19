from typing import Any, Dict, Optional


def is_pending_entry_result(build_result: Optional[Dict[str, Any]]) -> bool:
    return bool(build_result and build_result.get("pending_entry") is True)


def get_trade_uid_from_build_result(build_result: Optional[Dict[str, Any]]) -> Optional[str]:
    if not build_result:
        return None

    if is_pending_entry_result(build_result):
        return build_result.get("trade_uid")

    straddle_data = build_result.get("straddle_data") or {}
    if isinstance(straddle_data, dict):
        trade_uid = straddle_data.get("trade_uid") or build_result.get("trade_uid")
        if trade_uid:
            return straddle_data.get("trade_uid") or build_result.get("trade_uid")

    return build_result.get("trade_uid")
