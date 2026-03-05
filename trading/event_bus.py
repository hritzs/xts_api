"""
Event Bus - Priority-based async event coordination
Priority: HEDGE (1) > STOP_LOSS (2) > SQUARE_OFF (3) > ROLL (4) > MONITORING (5)
"""
import asyncio
from typing import Dict, Callable, Any, List
from datetime import datetime
from dataclasses import dataclass, field
from enum import IntEnum
from utils.logger import logger


class EventPriority(IntEnum):
    """
    Event priority (lower number = higher priority)
    
    Priority Order:
    1. HEDGE - Most critical (prevent large losses due to delta/gamma)
    2. STOP_LOSS - Exit losing positions
    3. SQUARE_OFF - Planned exit (time/profit target)
    4. ROLL - Position adjustment (can wait)
    5. MONITORING - Background checks (lowest priority)
    """
    HEDGE = 1          # Highest - Protect position immediately
    STOP_LOSS = 2      # Exit losing trade
    SQUARE_OFF = 3     # Planned exit
    ROLL = 4           # Position rollover
    MONITORING = 5     # Background monitoring (lowest priority)


@dataclass(order=True)
class Event:
    """
    Event with priority
    
    Attributes:
        priority: Event priority (1=highest)
        event_type: Type of event
        trade_uid: Trade UID
        data: Event data
        timestamp: Event timestamp
    """
    priority: int = field(compare=True)
    event_type: str = field(compare=False)
    trade_uid: str = field(compare=False)
    data: Dict[str, Any] = field(compare=False, default_factory=dict)
    timestamp: datetime = field(compare=False, default_factory=datetime.now)


class EventBus:
    """
    🚌 EVENT BUS - Priority-based async coordination
    
    Features:
    - Priority queue (HEDGE > SL > SQUARE_OFF > ROLL > MONITORING)
    - Per-trade event isolation
    - Async event handlers
    - Event history tracking
    
    What is MONITORING?
    ====================
    Monitoring is the background process that continuously checks conditions
    for each trade and emits higher-priority events when conditions are met:
    
    Examples:
    - Check if hedge is needed → Emit HEDGE event
    - Check if SL is hit → Emit STOP_LOSS event
    - Check if time to square off → Emit SQUARE_OFF event
    - Check if time to roll → Emit ROLL event
    - Update PnL calculations
    - Update Greeks
    - Refresh market data
    
    Monitoring runs at LOW priority so it doesn't interfere with actual
    execution tasks (hedge/SL/square-off/roll).
    """
    
    def __init__(self):
        self.event_queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self.handlers: Dict[str, List[Callable]] = {}
        self.running = False
        self.event_history: List[Event] = []
        self.max_history = 1000
        logger.info("✅ EventBus initialized")
        logger.info("📋 Priority: HEDGE(1) > SL(2) > SQF(3) > ROLL(4) > MON(5)")
    
    def register_handler(self, event_type: str, handler: Callable):
        """Register event handler"""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)
        logger.info(f"✅ Handler registered: {event_type}")
    
    async def emit(
        self,
        event_type: str,
        trade_uid: str,
        priority: EventPriority,
        data: Dict[str, Any] = None
    ):
        """
        Emit event with priority
        
        Args:
            event_type: Event type
                - hedge_needed (priority: HEDGE)
                - sl_triggered (priority: STOP_LOSS)
                - time_to_square_off (priority: SQUARE_OFF)
                - partial_square_off_needed (priority: SQUARE_OFF)
                - roll_needed (priority: ROLL)
                - monitor_tick (priority: MONITORING)
            trade_uid: Trade UID
            priority: Event priority
            data: Event data
        """
        event = Event(
            priority=priority.value,
            event_type=event_type,
            trade_uid=trade_uid,
            data=data or {},
            timestamp=datetime.now()
        )
        
        await self.event_queue.put(event)
        
        logger.debug(
            f"📤 Event: {event_type} | "
            f"P{priority.value}({priority.name}) | "
            f"Trade: {trade_uid}"
        )
    
    async def process_events(self):
        """Process events from priority queue"""
        self.running = True
        logger.info("🚌 EventBus processing started")
        
        while self.running:
            try:
                # Get highest priority event
                event = await asyncio.wait_for(
                    self.event_queue.get(),
                    timeout=1.0
                )
                
                priority_name = EventPriority(event.priority).name
                
                logger.info("="*100)
                logger.info(
                    f"🎯 Dispatching: {event.event_type} | "
                    f"P{event.priority}({priority_name}) | "
                    f"Trade: {event.trade_uid}"
                )
                logger.info("="*100)
                
                # Execute handlers
                if event.event_type in self.handlers:
                    for handler in self.handlers[event.event_type]:
                        try:
                            # Create a new task to run the handler concurrently.
                            # This allows the event bus to immediately process the next event.
                            asyncio.create_task(handler(event))
                        except Exception as e:
                            logger.error(f"❌ Handler dispatch error: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                
                # Track history
                self.event_history.append(event)
                if len(self.event_history) > self.max_history:
                    self.event_history.pop(0)
                
                self.event_queue.task_done()
                
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                logger.error(f"❌ Event processing error: {e}")
                import traceback
                logger.error(traceback.format_exc())
        
        logger.info("🛑 EventBus processing stopped")
    
    async def stop(self):
        """Stop event processing"""
        self.running = False
        logger.info("🛑 EventBus stop requested")
    
    def get_trade_events(self, trade_uid: str) -> List[Event]:
        """Get events for a specific trade"""
        return [e for e in self.event_history if e.trade_uid == trade_uid]


# Global event bus
global_event_bus: EventBus = None


def get_event_bus() -> EventBus:
    """Get global event bus"""
    return global_event_bus


def set_event_bus(event_bus: EventBus):
    """Set global event bus"""
    global global_event_bus
    global_event_bus = event_bus
    logger.info("✅ Global EventBus set")
