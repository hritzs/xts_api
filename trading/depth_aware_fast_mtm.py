"""
Depth-aware fast MTM.

Prices an open position at what it would realistically cost to close it now,
by walking up to 5 levels of market depth for the position size instead of
marking at LTP:

  - short position (net sold)  -> closed by BUYING  -> walk the ASKS
  - long position  (net bought) -> closed by SELLING -> walk the BIDS

Quantity beyond the visible depth is priced at the worst visible level.

Fallback order when depth is missing or stale:
  depth5 -> top (best bid/ask) -> ltp -> none

This runs alongside the LTP MTM; it does not replace it.
"""
import time
from typing import Any, Dict, List, Optional

from models.state import state

DEFAULT_MAX_DEPTH_AGE = 3.0  # seconds


def _f(v, default: float = 0.0) -> float:
    try:
        return float(v) if v not in (None, "") else default
    except (TypeError, ValueError):
        return default


def _clean_levels(levels: Any) -> List[Dict[str, float]]:
    out = []
    if not isinstance(levels, list):
        return out
    for lvl in levels:
        if not isinstance(lvl, dict):
            continue
        price = _f(lvl.get("price", lvl.get("Price")))
        qty = _f(lvl.get("qty", lvl.get("Size")))
        if price > 0 and qty > 0:
            out.append({"price": price, "qty": qty})
    return out


def walk_depth(levels: List[Dict[str, float]], qty: float) -> Optional[Dict[str, float]]:
    """
    Fill `qty` against depth `levels` (best first). Returns VWAP, how much the
    visible depth covered and how many levels were used. Any remainder is
    priced at the worst visible level.
    """
    levels = _clean_levels(levels)
    qty = abs(qty)
    if not levels or qty <= 0:
        return None

    remaining = qty
    notional = 0.0
    used = 0
    for lvl in levels:
        if remaining <= 0:
            break
        take = min(remaining, lvl["qty"])
        notional += take * lvl["price"]
        remaining -= take
        used += 1

    filled = qty - remaining
    if remaining > 0:
        notional += remaining * levels[-1]["price"]

    return {
        "price": notional / qty,
        "depth_filled_qty": filled,
        "levels_used": used,
    }


def _lookup_depth(token: int, max_age: float) -> Optional[Dict[str, Any]]:
    """Fresh depth for `token` from state.market_depth, else state.quotes."""
    now = time.time()

    depth = state.get_market_depth(token)
    if depth:
        ts = state.get_depth_timestamp(token) or _f(depth.get("_ts"))
        if ts and now - ts <= max_age:
            return depth

    quotes = getattr(state, "quotes", None)
    if isinstance(quotes, dict):
        q = quotes.get(int(token))
        if isinstance(q, dict):
            # quote_ts is ISO on some paths; only trust numeric timestamps
            ts = _f(q.get("_ts") or q.get("ts") or q.get("quote_ts"))
            if ts and now - ts <= max_age:
                return q
    return None


def exit_price(
    token: int,
    net_qty: float,
    ltp: float = 0.0,
    depth: Optional[Dict[str, Any]] = None,
    max_age: float = DEFAULT_MAX_DEPTH_AGE,
) -> Dict[str, Any]:
    """
    Price to close `net_qty` of `token` (positive = short, negative = long,
    matching sell_qty - buy_qty in the PnL code).

    Pass `depth` to skip the state lookup (it is then assumed fresh).
    Returns {price, source, depth_filled_qty, levels_used}; price is 0.0 and
    source "none" when nothing usable is available.
    """
    result = {"price": 0.0, "source": "none", "depth_filled_qty": 0.0, "levels_used": 0}
    if not net_qty:
        return result

    short = net_qty > 0
    if depth is None and token:
        depth = _lookup_depth(int(token), max_age)

    if depth:
        side_levels = depth.get("asks" if short else "bids")
        walked = walk_depth(side_levels, net_qty)
        if walked:
            result.update(walked)
            result["source"] = "depth5"
            return result

        top = _f(
            depth.get("ask") or depth.get("ask_price") or depth.get("best_ask")
            if short else
            depth.get("bid") or depth.get("bid_price") or depth.get("best_bid")
        )
        if top > 0:
            result.update(price=top, source="top")
            return result

    if ltp and ltp > 0:
        result.update(price=float(ltp), source="ltp")
    return result


def exit_mtm(entry_price: float, net_qty: float, px: float) -> float:
    """MTM at exit price `px`, same sign convention as leg_mtm_pnl."""
    if not net_qty or px <= 0 or entry_price <= 0:
        return 0.0
    if net_qty > 0:
        return (entry_price - px) * net_qty
    return (px - entry_price) * abs(net_qty)
