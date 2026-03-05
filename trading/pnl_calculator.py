"""
PnL Calculator - Real-time Position P&L Calculation
"""
from typing import Dict, Optional, List
from datetime import datetime
from utils.logger import logger


def calculate_pnl(straddle: Dict, live_prices: Dict[int, float]) -> Optional[Dict]:
    """
    Calculate P&L for a single straddle position
    
    Args:
        straddle: Straddle dict from database
        live_prices: {token: ltp} dict
    
    Returns:
        Dict with P&L breakdown or None
    """
    try:
        ce_token = straddle.get('ce_token')
        pe_token = straddle.get('pe_token')
        
        ce_entry_price = float(straddle.get('ce_entry_price', 0))
        pe_entry_price = float(straddle.get('pe_entry_price', 0))
        
        ce_quantity = int(straddle.get('ce_quantity', 0))
        pe_quantity = int(straddle.get('pe_quantity', 0))
        
        status = straddle.get('status', 'UNKNOWN')
        
        # Get live prices
        ce_ltp = live_prices.get(int(ce_token), ce_entry_price) if ce_token else ce_entry_price
        pe_ltp = live_prices.get(int(pe_token), pe_entry_price) if pe_token else pe_entry_price
        
        # Calculate P&L
        # For SELL positions: P&L = (Entry - Current) × Quantity
        ce_pnl = (ce_entry_price - ce_ltp) * ce_quantity if ce_entry_price > 0 else 0
        pe_pnl = (pe_entry_price - pe_ltp) * pe_quantity if pe_entry_price > 0 else 0
        
        total_pnl = ce_pnl + pe_pnl
        
        # If closed, P&L is realized, otherwise unrealized
        if status in ['CLOSED', 'CLOSED_SL', 'CLOSED_TIME']:
            realized_pnl = total_pnl
            unrealized_pnl = 0.0
        else:
            realized_pnl = 0.0
            unrealized_pnl = total_pnl
        
        return {
            'straddle_id': straddle.get('straddle_id') or straddle.get('trade_uid'),
            'ce_pnl': ce_pnl,
            'pe_pnl': pe_pnl,
            'total_pnl': total_pnl,
            'realized_pnl': realized_pnl,
            'unrealized_pnl': unrealized_pnl,
            'ce_entry': ce_entry_price,
            'pe_entry': pe_entry_price,
            'ce_ltp': ce_ltp,
            'pe_ltp': pe_ltp,
            'ce_quantity': ce_quantity,
            'pe_quantity': pe_quantity,
            'status': status
        }
        
    except Exception as e:
        logger.error(f"❌ PnL calculation error: {e}")
        return None


def calculate_aggregate_pnl(straddles: List[Dict], live_prices: Dict[int, float]) -> Dict:
    """
    Calculate aggregate P&L for multiple straddles
    
    Args:
        straddles: List of straddle dicts
        live_prices: {token: ltp} dict
    
    Returns:
        Dict with aggregate P&L
    """
    total_pnl = 0.0
    realized_pnl = 0.0
    unrealized_pnl = 0.0
    
    straddle_pnls = []
    
    for straddle in straddles:
        pnl = calculate_pnl(straddle, live_prices)
        
        if pnl:
            total_pnl += pnl['total_pnl']
            realized_pnl += pnl['realized_pnl']
            unrealized_pnl += pnl['unrealized_pnl']
            straddle_pnls.append(pnl)
    
    return {
        'total_pnl': total_pnl,
        'realized_pnl': realized_pnl,
        'unrealized_pnl': unrealized_pnl,
        'straddle_count': len(straddles),
        'straddles': straddle_pnls
    }


def calculate_pnl_per_straddle(straddle: Dict, live_prices: Dict[int, float]) -> float:
    """
    Calculate P&L per straddle (1 CE + 1 PE)
    
    Used for stop-loss calculation
    
    Args:
        straddle: Straddle dict
        live_prices: {token: ltp} dict
    
    Returns:
        P&L per straddle (float)
    """
    try:
        pnl = calculate_pnl(straddle, live_prices)
        
        if not pnl:
            return 0.0
        
        total_pnl = pnl['total_pnl']
        
        ce_quantity = pnl['ce_quantity']
        pe_quantity = pnl['pe_quantity']
        
        # Number of straddles = min(CE qty, PE qty)
        # Because 1 straddle = 1 CE + 1 PE
        num_straddles = min(ce_quantity, pe_quantity)
        
        if num_straddles == 0:
            return 0.0
        
        # P&L per straddle
        pnl_per_straddle = total_pnl / num_straddles
        
        return pnl_per_straddle
        
    except Exception as e:
        logger.error(f"❌ PnL per straddle error: {e}")
        return 0.0


def calculate_dte(expiry_str: str) -> int:
    """
    Calculate days to expiry
    
    Args:
        expiry_str: Expiry date string (e.g., "27Jan2026")
    
    Returns:
        Days to expiry (int)
    """
    try:
        from datetime import datetime
        
        # Parse expiry string
        expiry_date = datetime.strptime(expiry_str, "%d%b%Y")
        today = datetime.now()
        
        # Calculate days difference
        delta = expiry_date - today
        dte = delta.days
        
        return dte
        
    except Exception as e:
        logger.error(f"❌ DTE calculation error: {e}")
        return 0


def get_pnl_summary(trade_uid: str, straddle: Dict, live_prices: Dict[int, float]) -> Dict:
    """
    Get detailed P&L summary for a trade
    
    Args:
        trade_uid: Trade UID
        straddle: Straddle dict
        live_prices: {token: ltp} dict
    
    Returns:
        Dict with detailed P&L summary
    """
    try:
        pnl = calculate_pnl(straddle, live_prices)
        
        if not pnl:
            return {}
        
        pnl_per_straddle = calculate_pnl_per_straddle(straddle, live_prices)
        
        ce_quantity = pnl['ce_quantity']
        pe_quantity = pnl['pe_quantity']
        num_straddles = min(ce_quantity, pe_quantity)
        
        return {
            'trade_uid': trade_uid,
            'total_pnl': pnl['total_pnl'],
            'realized_pnl': pnl['realized_pnl'],
            'unrealized_pnl': pnl['unrealized_pnl'],
            'pnl_per_straddle': pnl_per_straddle,
            'num_straddles': num_straddles,
            'ce_pnl': pnl['ce_pnl'],
            'pe_pnl': pnl['pe_pnl'],
            'ce_entry': pnl['ce_entry'],
            'pe_entry': pnl['pe_entry'],
            'ce_ltp': pnl['ce_ltp'],
            'pe_ltp': pnl['pe_ltp'],
            'ce_quantity': ce_quantity,
            'pe_quantity': pe_quantity,
            'status': pnl['status']
        }
        
    except Exception as e:
        logger.error(f"❌ PnL summary error: {e}")
        return {}


def format_pnl(pnl: float) -> str:
    """
    Format P&L for display
    
    Args:
        pnl: P&L value
    
    Returns:
        Formatted string with color indicator
    """
    if pnl >= 0:
        return f"🟢 ₹{pnl:,.2f}"
    else:
        return f"🔴 ₹{pnl:,.2f}"


def get_pnl_percentage(entry_premium: float, current_pnl: float) -> float:
    """
    Calculate P&L percentage
    
    Args:
        entry_premium: Total entry premium
        current_pnl: Current P&L
    
    Returns:
        P&L percentage (float)
    """
    try:
        if entry_premium == 0:
            return 0.0
        
        percentage = (current_pnl / entry_premium) * 100
        return percentage
        
    except Exception as e:
        logger.error(f"❌ PnL percentage error: {e}")
        return 0.0
