from typing import List, Dict
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)

def calculate_net_positions(all_orders: List[Dict]) -> Dict[int, Dict]:
    """
    Calculates the net open positions from a list of all historical orders for a trade.
    An order is considered part of the position if its status is 'COMPLETE'.

    Args:
        all_orders: A list of order dictionaries from the database.

    Returns:
        A dictionary where keys are instrument tokens and values are summaries
        of the net position for that token. Returns an empty dict if no open
        positions are found.
    """
    net_positions = defaultdict(lambda: {'quantity': 0, 'total_value': 0.0})

    if not all_orders:
        logger.debug("calculate_net_positions received an empty list of orders.")
        return {}

    for order in all_orders:
        # The order dict is a row from the database
        order_status = order.get('order_status', '').upper()
        if order_status not in ['COMPLETE', 'FILLED', 'TRADED', 'EXECUTED']:
            continue

        token = int(order['exchange_instrument_id'])
        quantity = order.get('cumulative_quantity', 0)
        price = order.get('order_avg_price', 0.0)
        action = order.get('order_side', '').upper()

        if action == 'BUY':
            net_positions[token]['quantity'] -= quantity
            net_positions[token]['total_value'] -= quantity * price
        elif action == 'SELL':
            net_positions[token]['quantity'] += quantity
            net_positions[token]['total_value'] += quantity * price

        # Keep a reference to the original order for symbol/type parsing
        if 'order_ref' not in net_positions[token]:
            net_positions[token]['order_ref'] = order

    final_positions = {}
    for token, data in net_positions.items():
        if data['quantity'] != 0:
            order_ref = data.get('order_ref', {})
            trading_symbol = order_ref.get('trading_symbol', '')
            
            # Parse option type from trading symbol
            option_type = 'UNKNOWN'
            if 'CE' in trading_symbol.upper():
                option_type = 'CE'
            elif 'PE' in trading_symbol.upper():
                option_type = 'PE'

            final_positions[token] = {
                'token': token, 'symbol': order_ref.get('symbol', 'N/A'),
                'quantity': abs(data['quantity']),
                'entry_price': abs(data['total_value'] / data['quantity']) if data['quantity'] != 0 else 0,
                'action': 'SELL' if data['quantity'] > 0 else 'BUY',
                'option_type': option_type,
            }
    return final_positions