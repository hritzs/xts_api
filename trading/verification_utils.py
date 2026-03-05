"""
Verification Utilities - Helper functions for processing order verification results.
"""
from utils.logger import logger
from typing import List, Dict

def process_and_get_fills(
    trade_uid: str,
    context: str,
    successful_orders: List[Dict],
    verified_fills: List[Dict]
) -> List[Dict]:
    """
    Compares verified fills with successful placements and returns the best available data.

    If verification is successful, it returns the verified fills.
    If verification fails or returns no data, it constructs a 'best-effort'
    list of fallback fills based on the data from the successful order placements.

    Args:
        trade_uid: The UID of the parent trade for logging.
        context: A string describing the operation (e.g., "HEDGE_B1", "SQF_CHUNK2").
        successful_orders: The list of successfully placed orders from OrderExecutor.
        verified_fills: The list of successfully verified fills from the verification task.

    Returns:
        A list of fill data dictionaries to be processed and stored.
    """
    if verified_fills:
        logger.info(f"✅ Using {len(verified_fills)} verified fills for {context} on {trade_uid}.")
        return verified_fills

    logger.warning(f"⚠️ Verification for {context} on {trade_uid} returned no fills. Using fallback placement data.")
    
    fallback_fills = []
    if not successful_orders:
        logger.warning(f"⚠️ No successful orders to create fallback fills for {context} on {trade_uid}.")
        return []

    for order in successful_orders:
        fallback_fills.append({
            "AppOrderID": order.get("app_order_id"),
            "OrderUniqueIdentifier": order.get("uid"),
            "ExchangeInstrumentID": order.get("token"),
            "CumulativeQuantity": order.get("quantity"),
            "OrderAverageTradedPrice": order.get("expected_price", 0.0), # This is an assumption
            "OrderSide": order.get("action"),
            "OrderStatus": "FILLED", # Assumed status
            "TradingSymbol": f"TOKEN_{order.get('token')}", # Placeholder
        })
    
    logger.info(f"Generated {len(fallback_fills)} fallback fills for {context} on {trade_uid}.")
    return fallback_fills