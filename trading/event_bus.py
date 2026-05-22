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
    1. HEDGE      - Most critical (prevent large losses due to delta/gamma)
    2. STOP_LOSS  - Exit losing positions
    3. SQUARE_OFF - Planned exit (time/profit target)
    4. ROLL       - Position adjustment (can wait)
    5. MONITORING - Background checks (lowest priority)
    """
    HEDGE      = 1
    STOP_LOSS  = 2
    SL         = 2   # alias
    SQUARE_OFF = 3
    ROLL       = 4
    MONITORING = 5


@dataclass(order=True)
class Event:
    """
    Event with priority

    Attributes:
        priority:   Event priority (1=highest)
        event_type: Type of event
        trade_uid:  Trade UID
        data:       Event data
        timestamp:  Event timestamp
    """
    priority:   int            = field(compare=True)
    event_type: str            = field(compare=False)
    trade_uid:  str            = field(compare=False)
    data:       Dict[str, Any] = field(compare=False, default_factory=dict)
    timestamp:  datetime       = field(compare=False, default_factory=datetime.now)


class EventBus:
    """
    🚌 EVENT BUS - Priority-based async coordination

    Features:
    - Priority queue (HEDGE > SL > SQUARE_OFF > ROLL > MONITORING)
    - Per-trade event deduplication (in-flight guard)
    - Async event handlers
    - Event history tracking

    Architecture:
    =============
    snapshot_service (port 8003)
        → computes PnL / Greeks / pts_out
        → pushes to UI via WebSocket
        → populates state.trade_snapshots (read by monitors)

    worker process monitors
        → read state.trade_snapshots every second
        → gated by start_time + interval
        → emit to LOCAL event_bus when condition met
        → event_bus dispatches to hedger / roller / square_off

    The event_bus lives entirely inside the worker process.
    snapshot_service never emits to it directly.
    """

    def __init__(self):
        self.event_queue:  asyncio.PriorityQueue  = asyncio.PriorityQueue()
        self.handlers:     Dict[str, List[Callable]] = {}
        self.running:      bool                   = False
        self.event_history: List[Event]           = []
        self.max_history:  int                    = 1000
        self._in_flight:   set                    = set()   # dedup guard

        logger.info("✅ EventBus initialized")
        logger.info("📋 Priority: HEDGE(1) > SL(2) > SQF(3) > ROLL(4) > MON(5)")

    def register_handler(self, event_type: str, handler: Callable):
        """Register an async handler for an event type."""
        if event_type not in self.handlers:
            self.handlers[event_type] = []
        self.handlers[event_type].append(handler)
        logger.info(f"✅ Handler registered: {event_type}")

    async def emit(
        self,
        event_type: str,
        trade_uid:  str,
        priority:   EventPriority,
        data:       Dict[str, Any] = None
    ):
        """
        Emit an event with priority.

        Deduplication: if the same event_type:trade_uid is already in-flight
        (i.e. a handler is currently executing for it), the new emit is
        silently dropped. The in-flight flag is cleared automatically when
        the handler task completes.

        event_type values:
            hedge_needed            → priority HEDGE
            sl_triggered            → priority STOP_LOSS
            time_to_square_off      → priority SQUARE_OFF
            partial_square_off_needed → priority SQUARE_OFF
            roll_needed             → priority ROLL
            monitor_tick            → priority MONITORING
        """
        flight_key = f"{event_type}:{trade_uid}"

        if flight_key in self._in_flight:
            logger.debug(f"⏭️ Skipping duplicate in-flight event: {flight_key}")
            return

        self._in_flight.add(flight_key)

        event = Event(
            priority   = priority.value,
            event_type = event_type,
            trade_uid  = trade_uid,
            data       = data or {},
            timestamp  = datetime.now()
        )

        await self.event_queue.put(event)

        logger.debug(
            f"📤 Event queued: {event_type} | "
            f"P{priority.value}({priority.name}) | "
            f"Trade: {trade_uid}"
        )

    async def process_events(self):
        """
        Main event loop — dequeues highest-priority event and dispatches
        to registered handlers. Each handler runs as an independent asyncio
        task so the bus can immediately process the next event.
        The in-flight flag for a trade:event_type pair is cleared only after
        its handler task finishes (success or exception).
        """
        self.running = True
        logger.info("🚌 EventBus processing started")

        while self.running:
            try:
                event = await asyncio.wait_for(
                    self.event_queue.get(),
                    timeout=1.0
                )

                priority_name = EventPriority(event.priority).name
                flight_key    = f"{event.event_type}:{event.trade_uid}"

                logger.info("=" * 100)
                logger.info(
                    f"🎯 Dispatching: {event.event_type} | "
                    f"P{event.priority}({priority_name}) | "
                    f"Trade: {event.trade_uid}"
                )
                logger.info("=" * 100)

                handlers = self.handlers.get(event.event_type, [])

                if handlers:
                    for handler in handlers:
                        try:
                            async def _run(h=handler, e=event, fk=flight_key):
                                try:
                                    await h(e)
                                except Exception as ex:
                                    logger.error(
                                        f"❌ Handler error [{fk}]: {ex}",
                                        exc_info=True
                                    )
                                finally:
                                    self._in_flight.discard(fk)

                            asyncio.create_task(_run())

                        except Exception as e:
                            self._in_flight.discard(flight_key)
                            logger.error(f"❌ Handler dispatch error: {e}")
                            import traceback
                            logger.error(traceback.format_exc())
                else:
                    # No handler registered — clear in-flight immediately
                    self._in_flight.discard(flight_key)
                    logger.warning(f"⚠️ No handler registered for event: {event.event_type}")

                # Track history
                self.event_history.append(event)
                if len(self.event_history) > self.max_history:
                    self.event_history.pop(0)

                self.event_queue.task_done()

            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                logger.info("🛑 EventBus process_events cancelled.")
                break
            except Exception as e:
                logger.error(f"❌ Event processing error: {e}")
                import traceback
                logger.error(traceback.format_exc())

        logger.info("🛑 EventBus processing stopped")

    async def stop(self):
        """Signal the event processing loop to stop."""
        self.running = False
        logger.info("🛑 EventBus stop requested")

    def get_trade_events(self, trade_uid: str) -> List[Event]:
        """Return all history events for a specific trade."""
        return [e for e in self.event_history if e.trade_uid == trade_uid]

    def clear_in_flight(self, trade_uid: str = None):
        """
        Manually clear in-flight flags.
        Pass trade_uid to clear only that trade's flags,
        or None to clear all (e.g. on emergency reset).
        """
        if trade_uid:
            to_remove = {k for k in self._in_flight if k.endswith(f":{trade_uid}")}
            self._in_flight -= to_remove
            if to_remove:
                logger.info(f"🧹 Cleared {len(to_remove)} in-flight flags for {trade_uid}")
        else:
            count = len(self._in_flight)
            self._in_flight.clear()
            logger.info(f"🧹 Cleared all {count} in-flight flags")


# ── Global event bus registry ─────────────────────────────────────────────────

global_event_bus: EventBus = None


def get_event_bus() -> EventBus:
    """Get the current global event bus (returns None if not yet set)."""
    return global_event_bus


def set_event_bus(event_bus: EventBus):
    """Set the global event bus (called once per process at startup)."""
    global global_event_bus
    global_event_bus = event_bus
    logger.info("✅ Global EventBus set")
