"""
IV Filter - Check if current IV > IDV/divisor
"""
from utils.logger import logger


async def check_iv_filter(
    symbol: str,
    current_iv: float,
    idv: float,
    idv_divisor: float
) -> bool:
    """
    Check IV filter
    
    Args:
        symbol: Index symbol
        current_iv: Current implied volatility
        idv: Independent Delta Volatility threshold
        idv_divisor: IDV divisor
    
    Returns:
        True if filter passed (current_iv > idv/divisor)
    """
    threshold = idv / idv_divisor
    passed = current_iv > threshold
    
    logger.info("="*100)
    logger.info(f"📊 IV FILTER CHECK: {symbol}")
    logger.info(f"   Current IV: {current_iv:.2f}")
    logger.info(f"   IDV: {idv:.2f}")
    logger.info(f"   Divisor: {idv_divisor:.2f}")
    logger.info(f"   Threshold: {threshold:.2f}")
    logger.info(f"   Result: {'✅ PASS' if passed else '❌ FAIL'}")
    logger.info("="*100)
    
    return passed
