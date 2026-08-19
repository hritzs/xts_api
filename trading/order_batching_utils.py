import math
import time
from typing import List, Dict, Optional
from utils.logger import logger


CHUNK_DIVISOR = 7  # always fixed


def generate_chunked_orders(
    trade_uid_prefix: str,
    legs_data: List[Dict],
    base_lots_for_trade: int,
    chunk_divisor: int = CHUNK_DIVISOR,       # kept for signature compat, value ignored
    max_order_qty: int = 1755,
    order_lots_per_call: Optional[int] = None,
    aggressive: bool = False,                 # True only for SL-triggered SQF → max lots/order
) -> List[List[Dict]]:
    """
    Generates orders grouped into exactly 7 execution chunks.

    chunk_divisor is ALWAYS 7.

    min_lots_per_order priority (highest → lowest):
      1. aggressive=True  — SL SQF: use broker max lots/order (fastest exit)
      2. order_lots_per_call  — manual UI override
      3. range-based auto — ceil(base_lots / 100):
                               1–100  lots → 1 lot/order
                               101–200     → 2 lots/order
                               201–300     → 3 lots/order  etc.

    All paths capped at max_lots_per_order (broker limit).
    SQF / PSQF use the same range-based auto path as regular BUILD.
    """
    if not legs_data:
        return []

    n_chunks = CHUNK_DIVISOR

    lot_size_for_calc  = legs_data[0]['lot_size']
    max_lots_per_order = max(1, max_order_qty // lot_size_for_calc)

    # ── min_lots_per_order ────────────────────────────────────────────────────
    if aggressive:
        # SL SQF — use maximum possible order size to exit fastest
        min_lots_per_order = max_lots_per_order
        path = f"AGGRESSIVE/SL-SQF (max={max_lots_per_order})"

    elif order_lots_per_call and order_lots_per_call > 0:
        # Manual UI override
        min_lots_per_order = min(order_lots_per_call, max_lots_per_order)
        path = f"MANUAL order_lots_per_call={order_lots_per_call} → capped={min_lots_per_order}"

    else:
        # Range-based auto (BUILD default, SQF, PSQF — all same)
        raw  = math.ceil(base_lots_for_trade / 100) if base_lots_for_trade > 0 else 1
        min_lots_per_order = min(raw, max_lots_per_order)
        path = f"RANGE-AUTO ceil({base_lots_for_trade}/100)={raw} → capped={min_lots_per_order}"

    logger.debug(
        f"[chunking] {trade_uid_prefix} | "
        f"base_lots={base_lots_for_trade} | "
        f"chunks={n_chunks} (fixed) | "
        f"min_lots/order={min_lots_per_order} via {path}"
    )

    # ── Build chunk buckets ───────────────────────────────────────────────────
    all_chunks: List[List[Dict]] = [[] for _ in range(n_chunks)]
    ts_now        = int(time.time() * 1_000_000)
    order_counter = 0

    for leg in legs_data:
        total_lots_for_leg = leg['total_lots']
        lot_size           = leg['lot_size']

        if total_lots_for_leg == 0:
            continue

        lots_base      = total_lots_for_leg // n_chunks
        lots_remainder = total_lots_for_leg % n_chunks

        base_params = {k: v for k, v in leg.items() if k != 'total_lots'}

        for chunk_idx in range(n_chunks):
            lots_this_chunk = lots_base + (1 if chunk_idx < lots_remainder else 0)
            if lots_this_chunk == 0:
                continue

            n_full_orders  = lots_this_chunk // min_lots_per_order
            remainder_lots = lots_this_chunk %  min_lots_per_order

            for _ in range(n_full_orders):
                counter_str = f"{order_counter:03d}"
                if len(trade_uid_prefix) + len(counter_str) > 20:
                    excess = len(trade_uid_prefix) + len(counter_str) - 20
                    # Trim from the middle of the timestamp to preserve prefix and the 'a'/'b' suffix perfectly
                    safe_prefix = trade_uid_prefix[:3] + trade_uid_prefix[3+excess:]
                else:
                    safe_prefix = trade_uid_prefix
                uid = f"{safe_prefix}{counter_str}"
                all_chunks[chunk_idx].append({
                    **base_params,
                    'quantity': min_lots_per_order * lot_size,
                    'uid':      uid,
                })
                order_counter += 1

            if remainder_lots > 0:
                counter_str = f"{order_counter:03d}"
                if len(trade_uid_prefix) + len(counter_str) > 20:
                    excess = len(trade_uid_prefix) + len(counter_str) - 20
                    safe_prefix = trade_uid_prefix[:3] + trade_uid_prefix[3+excess:]
                else:
                    safe_prefix = trade_uid_prefix
                uid = f"{safe_prefix}{counter_str}"
                all_chunks[chunk_idx].append({
                    **base_params,
                    'quantity': remainder_lots * lot_size,
                    'uid':      uid,
                })
                order_counter += 1

    return [chunk for chunk in all_chunks if chunk]