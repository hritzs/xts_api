"""
Socket.IO Callbacks - Fixed Queue Access
"""
import asyncio
from utils.logger import logger
from models.state import state
import queue

# Event loop reference
main_event_loop = None
market_data_queue = queue.Queue(maxsize=2000)

def set_main_event_loop(loop):
    """Set reference to main event loop"""
    global main_event_loop
    main_event_loop = loop
    logger.info("✅ Main event loop reference set")


def on_socket_connect():
    """Socket.IO connected"""
    state.socket_connected = True
    state.data_source = "WEBSOCKET"
    logger.info("✅ Market Data Socket CONNECTED. Data source is WEBSOCKET.")


def on_socket_disconnect():
    """Socket.IO disconnected"""
    state.socket_connected = False
    state.data_source = "REST_POLL"
    logger.warning("🔌 Market Data Socket DISCONNECTED. Data source changed to REST_POLL.")


def on_socket_error(error):
    """Socket.IO error"""
    logger.error(f"❌ Socket error: {error}")


def _push_to_queue(token: int, ltp: float, timestamp=None):
    """Shared helper — push a price tick to the async queue for broadcast."""
    if main_event_loop and hasattr(state, 'market_data_queue') and state.market_data_queue:
        try:
            asyncio.run_coroutine_threadsafe(
                state.market_data_queue.put({
                    'token': token,
                    'ltp': ltp,
                    'timestamp': timestamp
                }),
                main_event_loop
            )
        except Exception as e:
            logger.error(f"❌ Queue put error: {e}")


def on_message1512_json_full(data):
    """
    Full market data update (1512) — FO options, futures, equities
    Contains: LTP, Volume, OI, Bid/Ask, etc.
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        ltp = data.get('LastTradedPrice') or data.get('Touchline', {}).get('LastTradedPrice')

        if token and ltp:
            state.update_price(int(token), float(ltp))
            logger.debug(f"⚡️ [1512 FULL] Token={token}, LTP={ltp}")
            _push_to_queue(int(token), float(ltp), data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1512 Full processing error: {e}")


def on_message1512_json_partial(data):
    """
    Partial market data update (1512) — LTP only (faster updates)
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        ltp = data.get('LastTradedPrice') or data.get('Touchline', {}).get('LastTradedPrice')

        if token and ltp:
            state.update_price(int(token), float(ltp))
            logger.debug(f"⚡️ [1512 PARTIAL] Token={token}, LTP={ltp}")
            _push_to_queue(int(token), float(ltp), data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1512 Partial processing error: {e}")


def on_message1501_json_full(data):
    """
    Touchline update (1501) — Cash indices: NIFTY, BANKNIFTY, SENSEX, MIDCPNIFTY etc.
    This is the correct code for real-time spot price on ALL exchanges (NSE + BSE).
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        # Cash indices publish LastTradedPrice inside Touchline or at top level
        touchline = data.get('Touchline', {})
        ltp = (
            data.get('LastTradedPrice')
            or touchline.get('LastTradedPrice')
            or touchline.get('Close')
            or data.get('Close')
        )

        if token and ltp:
            ltp = float(ltp)
            if ltp > 0:
                state.update_price(int(token), ltp)
                logger.debug(f"⚡️ [1501 FULL] Cash Index Token={token}, LTP={ltp}")
                _push_to_queue(int(token), ltp, data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1501 Full processing error: {e}")


def on_message1501_json_partial(data):
    """
    Touchline partial update (1501) — Cash indices spot price tick
    """
    try:
        if not isinstance(data, dict):
            return

        token = data.get('ExchangeInstrumentID')
        touchline = data.get('Touchline', {})
        ltp = (
            data.get('LastTradedPrice')
            or touchline.get('LastTradedPrice')
            or touchline.get('Close')
            or data.get('Close')
        )

        if token and ltp:
            ltp = float(ltp)
            if ltp > 0:
                state.update_price(int(token), ltp)
                logger.debug(f"⚡️ [1501 PARTIAL] Cash Index Token={token}, LTP={ltp}")
                _push_to_queue(int(token), ltp, data.get('ExchangeTimeStamp'))

    except Exception as e:
        logger.error(f"❌ 1501 Partial processing error: {e}")
