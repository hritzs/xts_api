"""
Straddle Filter - Check if ATM straddle price > threshold
"""
from utils.logger import logger
from trading.option_chain import get_option_chain


async def check_straddle_filter(
    symbol: str,
    straddle_filter: float
) -> tuple[bool, float]:
    """
    Check straddle price filter
    
    Args:
        symbol: Index symbol
        straddle_filter: Minimum straddle price
    
    Returns:
        Tuple of (passed, current_straddle_price)
    """
    try:
        # Get option chain
        chain_data = get_option_chain(symbol)
        if not chain_data:
            logger.error("❌ Failed to get option chain")
            return False, 0.0
        
        # Get ATM row
        atm_row = next((row for row in chain_data['chain'] if row['is_atm']), None)
        if not atm_row:
            logger.error("❌ ATM strike not found")
            return False, 0.0
        
        ce_ltp = atm_row['ce_ltp']
        pe_ltp = atm_row['pe_ltp']
        straddle_price = ce_ltp + pe_ltp
        
        passed = straddle_price > straddle_filter
        
        logger.info("="*100)
        logger.info(f"📊 STRADDLE FILTER CHECK: {symbol}")
        logger.info(f"   ATM Strike: {chain_data['atm']}")
        logger.info(f"   CE LTP: ₹{ce_ltp:.2f}")
        logger.info(f"   PE LTP: ₹{pe_ltp:.2f}")
        logger.info(f"   Straddle Price: ₹{straddle_price:.2f}")
        logger.info(f"   Filter: ₹{straddle_filter:.2f}")
        logger.info(f"   Result: {'✅ PASS' if passed else '❌ FAIL'}")
        logger.info("="*100)
        
        return passed, straddle_price
        
    except Exception as e:
        logger.error(f"❌ Straddle filter error: {e}")
        return False, 0.0
