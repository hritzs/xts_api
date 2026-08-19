"""
Pydantic Models for API Requests and Responses
"""

from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


# ══════════════════════════════════════════════════════════════════════════════
# REQUEST MODELS
# ══════════════════════════════════════════════════════════════════════════════

class StraddleRequest(BaseModel):
    """Simple straddle order request"""
    symbol: str = Field(..., description="Index symbol", examples=["NIFTY"])
    lots: int = Field(1, gt=0, description="Number of lots", examples=[5])
    delta_neutral: bool = Field(True, description="Enable delta-neutral positioning")
    hedge_monitor_interval: float = Field(60.0, gt=0, description="Hedge check interval (s)")
    sl_monitor_interval: float = Field(60.0, gt=0, description="SL check interval (s)")
    roll_monitor_interval: float = Field(60.0, gt=0, description="Roll check interval (s)")
    order_lots_per_call: int = Field(1, gt=0, description="Lots to be passed in one call to the broker")

    entry_at_straddle: Optional[float] = Field(
        default=None,
        description="Enter only when live straddle reaches this premium."
    )

    exit_at_straddle: Optional[float] = Field(
        default=None,
        description="Exit when live straddle reaches this premium."
    )



class CustomStraddleRequest(StraddleRequest):
    """
    Custom straddle/strangle order request with specific strikes.
    Inherits all fields from StraddleRequest.
    """
    ce_strike_price: int = Field(..., description="Strike price for the Call option leg.")
    pe_strike_price: int = Field(..., description="Strike price for the Put option leg.")
    product_type: str = Field("MIS", description="Product type (MIS/NRML)", examples=["MIS"])


class ConfigBuildRequest(BaseModel):
    """
    Configuration-based automated build request

    Score-based flow:
    - backend fetches IDV / previous straddle references internally
    - if fetch fails, UI can pass manual values
    - final decision is based on score tables
    """
    symbol: str = Field(..., description="Index symbol", examples=["NIFTY"])
    size: int = Field(..., gt=0, description="Number of lots", examples=[5])

    # Custom strikes
    ce_strike_price: Optional[int] = Field(
        default=None,
        description="Custom CE strike price. If provided alone, backend may auto-fill PE with ATM depending on route logic."
    )
    pe_strike_price: Optional[int] = Field(
        default=None,
        description="Custom PE strike price. If provided alone, backend may auto-fill CE with ATM depending on route logic."
    )

    # Times
    entry_time: str = Field(..., description="Entry time HH:MM:SS", examples=["09:20:00"])
    exit_time: str = Field(..., description="Exit time HH:MM:SS", examples=["15:15:00"])

    # Monitoring intervals (seconds)
    hedge_monitor_interval: float = Field(60.0, gt=0, description="Hedge check interval (s)")
    sl_monitor_interval: float = Field(60.0, gt=0, description="SL check interval (s)")
    roll_monitor_interval: float = Field(60.0, gt=0, description="Roll check interval (s)")
    roll_flag_check_interval: float = Field(60.0, gt=0, description="Roll flag interval (s)")

    # Hedge parameters
    hedge_div: float = Field(57.0, gt=0, description="Hedge divisor")
    straddle_div: float = Field(4.0, gt=0, description="Straddle divisor")
    roll_straddle_div: float = Field(0.001, gt=0, description="Roll straddle divisor")
    hedge_frac: float = Field(1.0, ge=0, le=1, description="Hedge fraction")

    # Order placement buffers
    buy_buffer: int = Field(2, ge=0, description="Number of ticks to add to ask price for buy orders.")
    sell_buffer: int = Field(2, ge=0, description="Number of ticks to subtract from bid price for sell orders.")

    # Stop-loss
    sl_bps: float = Field(14.0, gt=0, description="Stop-loss in BPS")
    straddle_stop_loss_pct: float = Field(
        1.0,
        gt=0,
        description="Stop building / react if straddle price drops by this percentage of reference."
    )

    # Optional advanced monitoring start times
    hedge_start_time: Optional[str] = Field(
        default=None,
        description="Specific start time for hedge monitor (HH:MM or HH:MM:SS)",
        examples=["09:30:00"]
    )
    sl_start_time: Optional[str] = Field(
        default=None,
        description="Specific start time for SL monitor (HH:MM or HH:MM:SS)",
        examples=["09:21:00"]
    )
    roll_start_time: Optional[str] = Field(
        default=None,
        description="Specific start time for roll monitor (HH:MM or HH:MM:SS)",
        examples=["10:00:00"]
    )

    # Manual overrides from UI if shared files are unavailable
    manual_latest_idv: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual latest IDV override from UI if auto-fetch fails"
    )
    manual_historical_idv: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual historical IDV override from UI if auto-fetch fails"
    )
    manual_prev_day_straddle: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual previous day straddle override from UI if auto-fetch fails"
    )

    manual_spot_price: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual spot price override for LUT calculations."
    )


    use_live_spot_for_og: bool = Field(
        default=False,
        description="Use live synthetic spot instead of the captured 9:18 spot for OG calculations."
    )

    tp_points: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual Take-Profit threshold in points. Overrides the SL multiplier."
    )

    tp_bps: Optional[float] = Field(
        default=None,
        gt=0,
        description="Take-Profit in basis points (BPS) of the entry spot price."
    )

    order_lots_per_call: int = Field(1, gt=0, description="Lots to be passed in one call to the broker")

    straddle_price_drop_trigger: Optional[float] = Field(default=None, description="Price drop trigger in points for partial square-off.")
    entry_at_straddle: Optional[float] = Field(
        default=None,
        description="Enter only when live straddle reaches this premium."
    )

    exit_at_straddle: Optional[float] = Field(default=None, description="Square off when live straddle reaches this value.")
    straddle_price_drop_pct_sqf: Optional[float] = Field(default=None, description="Percentage to square-off when price drop is triggered.")


class ConfigScorePreviewRequest(BaseModel):
    """
    Request model for live score preview.
    Kept aligned with ConfigBuildRequest so frontend can preview
    the same data before starting automated build.
    """
    symbol: str = Field(..., description="Index symbol", examples=["NIFTY"])
    size: int = Field(..., gt=0, description="Number of lots", examples=[5])

    ce_strike_price: Optional[int] = Field(default=None, description="Optional CE strike override")
    pe_strike_price: Optional[int] = Field(default=None, description="Optional PE strike override")

    entry_time: str = Field(..., description="Entry time HH:MM:SS", examples=["09:20:00"])
    exit_time: str = Field(..., description="Exit time HH:MM:SS", examples=["15:15:00"])

    hedge_div: float = Field(57.0, gt=0, description="Hedge divisor")
    straddle_div: float = Field(4.0, gt=0, description="Straddle divisor")
    roll_straddle_div: float = Field(0.001, gt=0, description="Roll straddle divisor")
    hedge_frac: float = Field(1.0, ge=0, le=1, description="Hedge fraction")
    sl_bps: float = Field(14.0, gt=0, description="Stop-loss in BPS")
    straddle_stop_loss_pct: float = Field(1.0, gt=0, description="Straddle stop percentage")
    buy_buffer: int = Field(2, ge=0, description="Buy buffer")
    sell_buffer: int = Field(2, ge=0, description="Sell buffer")
    hedge_monitor_interval: float = Field(60.0, gt=0, description="Hedge check interval (s)")
    sl_monitor_interval: float = Field(60.0, gt=0, description="SL check interval (s)")
    roll_monitor_interval: float = Field(60.0, gt=0, description="Roll check interval (s)")
    roll_flag_check_interval: float = Field(60.0, gt=0, description="Roll flag interval (s)")
    hedge_start_time: Optional[str] = Field(default=None, description="Specific hedge start time")
    sl_start_time: Optional[str] = Field(default=None, description="Specific SL start time")
    roll_start_time: Optional[str] = Field(default=None, description="Specific roll start time")

    manual_latest_idv: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual latest IDV override from UI if auto-fetch fails"
    )
    manual_historical_idv: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual historical IDV override from UI if auto-fetch fails"
    )
    manual_prev_day_straddle: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual previous day straddle override from UI if auto-fetch fails"
    )

    manual_spot_price: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual spot price override for LUT calculations."
    )


    use_live_spot_for_og: bool = Field(
        default=False,
        description="Use live synthetic spot instead of the captured 9:18 spot for OG calculations."
    )

    tp_points: Optional[float] = Field(
        default=None,
        gt=0,
        description="Manual Take-Profit threshold in points. Overrides the SL multiplier."
    )

    tp_bps: Optional[float] = Field(
        default=None,
        gt=0,
        description="Take-Profit in basis points (BPS) of the entry spot price."
    )

    order_lots_per_call: int = Field(1, gt=0, description="Lots to be passed in one call to the broker")

    straddle_price_drop_trigger: Optional[float] = Field(default=None, description="Price drop trigger in points for partial square-off.")

    entry_at_straddle: Optional[float] = Field(
        default=None,
        description="Enter only when live straddle reaches this premium."
    )

    exit_at_straddle: Optional[float] = Field(default=None, description="Square off when live straddle reaches this value.")
    straddle_price_drop_pct_sqf: Optional[float] = Field(default=None, description="Percentage to square-off when price drop is triggered.")


class HedgeRequest(BaseModel):
    """Hedge request"""
    trade_uid: str = Field(..., description="Trade UID", examples=["ny230126133755"])
    hedge_type: str = Field("DELTA", description="Hedge type", examples=["DELTA"])
    strike: int = Field(..., description="Strike price", examples=[25600])
    option_type: str = Field(..., description="Option type", examples=["CE"])
    quantity: int = Field(..., gt=0, description="Quantity", examples=[65])


class SquareOffRequest(BaseModel):
    """Square-off request"""
    trade_uid: str = Field(..., description="Trade UID", examples=["ny230126133755"])


class PartialSquareOffRequest(BaseModel):
    """Partial square-off request"""
    percentage: float = Field(..., gt=0, le=100, description="Percentage of the original position to square off.")


class UpdateTradeConfigRequest(BaseModel):
    """Request to update configuration for a live trade."""
    tp_bps: Optional[float] = Field(default=None, description="New Take-Profit in BPS.")
    sl_bps: Optional[float] = Field(default=None, description="New Stop-Loss in BPS.")
    exit_at_straddle: Optional[float] = Field(
        default=None,
        description="Square off when live straddle reaches this value."
    )


class RollRequest(BaseModel):
    """Roll position request"""
    trade_uid: str = Field(..., description="Trade UID", examples=["ny230126133755"])
    new_expiry: str = Field(..., description="New expiry", examples=["30Jan2026"])
    new_strike: Optional[int] = Field(default=None, description="New strike (optional)")


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


class ConfigScorePreviewResponse(BaseModel):
    """Score preview response for automation screen"""
    success: bool
    score: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    timestamp: Optional[str] = None


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
