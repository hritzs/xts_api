"""
Pydantic Models for API Requests and Responses
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


# ══════════════════════════════════════════════════════════════════════════════
# REQUEST MODELS
# ══════════════════════════════════════════════════════════════════════════════

class StraddleRequest(BaseModel):
    """Simple straddle order request"""
    symbol: str = Field(..., description="Index symbol", example="NIFTY")
    lots: int = Field(1, gt=0, description="Number of lots", example=5)
    delta_neutral: bool = Field(True, description="Enable delta-neutral positioning")


class ConfigBuildRequest(BaseModel):
    """
    Configuration-based automated build request
    
    All parameters for automated trading with filters and monitoring
    """
    symbol: str = Field(..., description="Index symbol", example="NIFTY")
    size: int = Field(..., gt=0, description="Number of lots", example=5)
    
    # Entry filters
    idv: float = Field(..., gt=0, description="IDV threshold", example=15.0)
    idv_divisor: float = Field(..., gt=0, description="IDV divisor", example=1.5)
    straddle_filter: float = Field(..., gt=0, description="Min straddle price", example=300.0)
    
    # Times (HH:MM format)
    entry_time: str = Field(..., description="Entry time HH:MM:SS", example="09:20:00")
    exit_time: str = Field(..., description="Exit time HH:MM:SS", example="15:15:00")
    
    # Monitoring intervals (seconds)
    hedge_monitor_interval: float = Field(60.0, gt=0, description="Hedge check interval (s)")
    sl_monitor_interval: float = Field(60.0, gt=0, description="SL check interval (s)")
    roll_monitor_interval: float = Field(60.0, gt=0, description="Roll check interval (s)")
    roll_flag_check_interval: float = Field(60.0, gt=0, description="Roll flag interval (s)")
    
    # Hedge parameters
    hedge_div: float = Field(2.0, gt=0, description="Hedge divisor")
    straddle_div: float = Field(2.0, gt=0, description="Straddle divisor")
    roll_straddle_div: float = Field(8.0, gt=0, description="Roll straddle divisor")
    hedge_frac: float = Field(0.7, ge=0, le=1, description="Hedge fraction")
    
    # Order placement buffers
    buy_buffer: int = Field(default=2, description="Number of ticks to add to the ask price for buy orders.")
    sell_buffer: int = Field(default=2, description="Number of ticks to subtract from the bid price for sell orders.")
    
    # Stop-loss
    sl_bps: float = Field(14.0, gt=0, description="Stop-loss BPS")

    # New optional fields for advanced monitoring
    straddle_price_drop_trigger: Optional[float] = Field(0.0, description="Square off if straddle premium drops by this amount from HWM. 0 to disable.", example=2.5)
    straddle_price_monitor_interval: Optional[float] = Field(5.0, gt=0, description="Interval for straddle price drop check (s)")
    hedge_start_time: Optional[str] = Field(None, description="Specific start time for hedge monitor (HH:MM or HH:MM:SS)", example="09:30")
    sl_start_time: Optional[str] = Field(None, description="Specific start time for SL monitor (HH:MM or HH:MM:SS)", example="09:21")
    roll_start_time: Optional[str] = Field(None, description="Specific start time for roll monitor (HH:MM or HH:MM:SS)", example="10:00")


class HedgeRequest(BaseModel):
    """Hedge request"""
    trade_uid: str = Field(..., description="Trade UID", example="ny230126133755")
    hedge_type: str = Field("DELTA", description="Hedge type", example="DELTA")
    strike: int = Field(..., description="Strike price", example=25600)
    option_type: str = Field(..., description="Option type", example="CE")
    quantity: int = Field(..., gt=0, description="Quantity", example=65)


class SquareOffRequest(BaseModel):
    """Square-off request"""
    trade_uid: str = Field(..., description="Trade UID", example="ny230126133755")


class RollRequest(BaseModel):
    """Roll position request"""
    trade_uid: str = Field(..., description="Trade UID", example="ny230126133755")
    new_expiry: str = Field(..., description="New expiry", example="30Jan2026")
    new_strike: Optional[int] = Field(None, description="New strike (optional)")


# ══════════════════════════════════════════════════════════════════════════════
# RESPONSE MODELS
# ══════════════════════════════════════════════════════════════════════════════

class HealthResponse(BaseModel):
    """Health check response"""
    status: str
    timestamp: str
    db_status: str
    socket_connected: bool
    cached_prices: int
    subscribed_tokens: int
    active_straddles: int


class APIResponse(BaseModel):
    """Generic API response"""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    message: Optional[str] = None
    timestamp: Optional[str] = None


class StraddleResponse(BaseModel):
    """Straddle placement response"""
    success: bool
    trade_uid: str
    data: Dict[str, Any]
    message: str
    timestamp: str


class ConfigBuildResponse(BaseModel):
    """Configuration build response"""
    success: bool
    trade_uid: Optional[str] = None
    message: str
    status: str
    timestamp: str


class OrderBookResponse(BaseModel):
    """Order book response"""
    success: bool
    count: int
    orders: List[Dict[str, Any]]


class PositionsResponse(BaseModel):
    """Positions response"""
    success: bool
    count: int
    positions: List[Dict[str, Any]]


class StraddlesResponse(BaseModel):
    """Straddles response"""
    success: bool
    count: int
    straddles: List[Dict[str, Any]]


class PnLResponse(BaseModel):
    """PnL response"""
    success: bool
    data: Dict[str, Any]
    timestamp: str


class OptionChainResponse(BaseModel):
    """Option chain response"""
    success: bool
    data: Dict[str, Any]


class PricesResponse(BaseModel):
    """Prices response"""
    success: bool
    count: int
    prices: Dict[str, float]
    timestamp: str


# ══════════════════════════════════════════════════════════════════════════════
# WEBSOCKET MODELS
# ══════════════════════════════════════════════════════════════════════════════

class WebSocketMessage(BaseModel):
    """WebSocket message format"""
    type: str
    data: Dict[str, Any]
    trade_uid: Optional[str] = None
    timestamp: str


class PriceUpdate(BaseModel):
    """Price update message"""
    token: int
    ltp: float
    timestamp: str


class PnLUpdate(BaseModel):
    """PnL update message"""
    trade_uid: str
    total_pnl: float
    unrealized_pnl: float
    realized_pnl: float
    timestamp: str


# ══════════════════════════════════════════════════════════════════════════════
# ERROR MODELS
# ══════════════════════════════════════════════════════════════════════════════

class ErrorResponse(BaseModel):
    """Error response"""
    success: bool = False
    error: str
    error_code: Optional[str] = None
    timestamp: str
