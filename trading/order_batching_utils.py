import math
import time
from typing import List, Dict, Any
from utils.logger import logger

def generate_chunked_orders(
    trade_uid_prefix: str,
    legs_data: List[Dict], # Each dict: {'token', 'option_type', 'action', 'total_lots', 'lot_size', 'expected_price', 'exchange_segment', 'product_type'}
    base_lots_for_trade: int, # The largest initial lots for the trade, used to determine min_lots_per_order
    chunk_divisor: int = 10,
    max_order_qty: int = 1800 # The maximum number of contracts per single order
) -> List[List[Dict]]: # Returns a list of chunks, where each chunk is a list of orders
    """
    Generates orders grouped into chunks based on the specified batching strategy.
    
    Args:
        trade_uid_prefix: Prefix for order UIDs (e.g., "BUILD_trade123").
        legs_data: List of dictionaries, each representing a leg to be ordered.
                   Each dict must contain: 'token', 'option_type', 'action', 'total_lots' (total lots for this leg),
                   'lot_size', 'expected_price', 'exchange_segment', 'product_type'.
        base_lots_for_trade: The initial total lots for the entire trade (e.g., the 'lots' parameter in build).
                             Used to calculate min_lots_per_order.
        chunk_divisor: The number of chunks to divide the total quantity into.
        max_order_qty: The maximum number of contracts allowed in a single order by the broker.
    
    Returns:
        A list of lists of order dictionaries. Each inner list represents a chunk of orders.
    """
    if not legs_data:
        return []

    # --- UPDATED: Respect max_order_qty ---
    # The lot size can be different per leg, but for a single trade it's consistent.
    lot_size_for_calc = legs_data[0]['lot_size'] if legs_data else 1
    # Calculate the maximum number of lots that can fit in a single order.
    max_lots_per_order = max(1, max_order_qty // lot_size_for_calc)
    # Determine the desired size of our smallest order slice.
    min_lots_per_order_calc = max(1, base_lots_for_trade // 100) if base_lots_for_trade > 0 else 1
    # The actual slice size cannot exceed the broker's limit.
    min_lots_per_order = min(min_lots_per_order_calc, max_lots_per_order)
    logger.debug(f"Order chunking params: max_order_qty={max_order_qty}, max_lots_per_order={max_lots_per_order}, min_lots_per_order={min_lots_per_order}")

    all_chunks_orders: List[List[Dict]] = [[] for _ in range(chunk_divisor)]
    
    ts_now = int(time.time() * 1_000_000)
    order_counter = 0

    for leg_idx, leg in enumerate(legs_data):
        total_lots_for_leg = leg['total_lots']
        lot_size = leg['lot_size']
        
        if total_lots_for_leg == 0:
            continue

        lots_per_chunk_base = total_lots_for_leg // chunk_divisor
        lots_per_chunk_remainder = total_lots_for_leg % chunk_divisor

        for chunk_idx in range(chunk_divisor):
            lots_for_this_chunk = lots_per_chunk_base
            if chunk_idx < lots_per_chunk_remainder:
                lots_for_this_chunk += 1
            
            if lots_for_this_chunk == 0:
                continue

            num_full_orders = lots_for_this_chunk // min_lots_per_order
            remainder_lots_for_last_order = lots_for_this_chunk % min_lots_per_order

            # Create base order dict, then add quantity and uid
            base_order_params = {k: v for k, v in leg.items() if k not in ['total_lots']} # Copy all but total_lots

            for _ in range(num_full_orders):
                order_quantity = min_lots_per_order * lot_size
                order_uid = f"{trade_uid_prefix}_{ts_now + order_counter}"
                all_chunks_orders[chunk_idx].append({**base_order_params, 'quantity': order_quantity, 'uid': order_uid})
                order_counter += 1

            if remainder_lots_for_last_order > 0:
                order_quantity = remainder_lots_for_last_order * lot_size
                order_uid = f"{trade_uid_prefix}_{ts_now + order_counter}"
                all_chunks_orders[chunk_idx].append({**base_order_params, 'quantity': order_quantity, 'uid': order_uid})
                order_counter += 1
                
    # Filter out empty chunks
    return [chunk for chunk in all_chunks_orders if chunk]