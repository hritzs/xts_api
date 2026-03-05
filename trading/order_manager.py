"""
Order Manager - Fixed XTS API Methods
"""
from typing import Dict, List, Optional
from utils.logger import logger
from models.state import state
import cred
import time


xt_interactive = None
_positions_cache = None
_cache_timestamp = 0
CACHE_DURATION = 30  # seconds


def set_interactive_instance(xt_i):
    """Set XTS Interactive instance"""
    global xt_interactive
    xt_interactive = xt_i
    logger.info("✅ Order Manager initialized")


def get_positions() -> List[Dict]:
    """
    Get broker positions
    
    ✅ Using correct XTS API method: get_position_netwise()
    ✅ Added caching to prevent rate limiting (30s cache)
    """
    global _positions_cache, _cache_timestamp
    
    try:
        # Check cache first
        current_time = time.time()
        if _positions_cache is not None and (current_time - _cache_timestamp) < CACHE_DURATION:
            logger.info("📋 Using cached positions data")
            return _positions_cache
        
        if not xt_interactive:
            logger.error("❌ XTS Interactive not initialized")
            return []
        
        # ✅ Correct method name - NetWise for net positions
        response = xt_interactive.get_position_netwise()
        
        logger.info(f"Positions API response: {response}")
        
        if response and response.get('type') == 'success':
            result = response.get('result', {})
            
            # Handle both dict and list response formats
            if isinstance(result, dict):
                positions = result.get('positionList', [])
            elif isinstance(result, list):
                positions = result
            else:
                positions = []
            
            # Cache the result
            _positions_cache = positions
            _cache_timestamp = current_time
            
            logger.info(f"✅ Fetched {len(positions)} positions")
            return positions
        else:
            error_msg = response.get('description', 'Unknown error') if response else 'No response'
            
            # Handle "Data Not Available" as no positions (not an error)
            if error_msg == 'Data Not Available':
                logger.info("ℹ️  No positions available (Data Not Available)")
                _positions_cache = []
                _cache_timestamp = current_time
                return []
            
            logger.error(f"❌ Get positions failed: {error_msg}")
            return []
            
    except Exception as e:
        logger.error(f"❌ Get positions error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def get_order_book() -> List[Dict]:
    """
    Get broker order book
    
    ✅ XTS API doesn't have get_all_orders, we track orders in database instead
    """
    try:
        if not xt_interactive:
            logger.error("❌ XTS Interactive not initialized for get_order_book")
            return []
        
        # ✅ Correct method name - get_order_book()
        # This needs the clientID for non-investor clients.
        client_id = getattr(cred, 'clientID', None)
        response = xt_interactive.get_order_book(clientID=client_id)
        
        if response and response.get('type') == 'success':
            result = response.get('result', {})
            
            # Handle both dict and list response formats
            if isinstance(result, dict):
                orders = result.get('orderList', []) or result.get('OrderList', [])
            elif isinstance(result, list):
                orders = result
            else:
                orders = []
            
            logger.info(f"✅ Fetched {len(orders)} orders from broker order book.")
            return orders
        else:
            error_msg = response.get('description', 'Unknown error') if response else 'No response'
            logger.error(f"❌ Get order book failed: {error_msg}")
            return []
            
    except Exception as e:
        logger.error(f"❌ Get order book error: {e}", exc_info=True)
        return []
        
def get_order_book_with_token(token: str, userID: str, isInvestorClient: bool) -> List[Dict]:
    """
    Get broker order book using a specific token (for microservices).
    """
    try:
        from Connect import XTSConnect
        # Initialize XTSConnect with credentials (needed for structure)
        # but override the token immediately.
        xt_temp = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        
        # Manually set the common variables
        xt_temp._set_common_variables(token, userID, isInvestorClient)
        
        # Force isInvestorClient to False if we want to see all orders (Pro mode)
        xt_temp.isInvestorClient = False 
        
        client_id = getattr(cred, 'clientID', userID)
        response = xt_temp.get_order_book(clientID=client_id)
        
        if response and response.get('type') == 'success':
            result = response.get('result', {})
            if isinstance(result, dict):
                return result.get('orderList', []) or result.get('OrderList', [])
            elif isinstance(result, list):
                return result
        return []
    except Exception as e:
        logger.error(f"❌ get_order_book_with_token error: {e}")
        return []

def get_trade_book() -> List[Dict]:
    """Get trade book"""
    try:
        if not xt_interactive:
            return []
        
        response = xt_interactive.get_trade()
        
        if response and response.get('type') == 'success':
            result = response.get('result', [])
            
            if isinstance(result, dict):
                trades = result.get('tradeList', [])
            elif isinstance(result, list):
                trades = result
            else:
                trades = []
            
            return trades
        return []
        
    except Exception as e:
        logger.error(f"❌ Get trades error: {e}")
        return []


def place_straddle_order(symbol: str, lots: int) -> Optional[Dict]:
    """Place straddle order (delegated to builder)"""
    from trading.builder import build_straddle
    import asyncio
    
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Already in async context
            return None
        else:
            return loop.run_until_complete(build_straddle(symbol, lots))
    except Exception as e:
        logger.error(f"❌ Place straddle error: {e}")
        return None
