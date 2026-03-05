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


def on_message1512_json_full(data):
    """
    Full market data update (1512)
    
    Contains: LTP, Volume, OI, Bid/Ask, etc.
    """
    try:
        if not isinstance(data, dict):
            return
        
        # Extract token and LTP
        token = data.get('ExchangeInstrumentID')
        ltp = data.get('LastTradedPrice') or data.get('Touchline', {}).get('LastTradedPrice')
        
        if token and ltp:
            # Update state immediately
            state.update_price(int(token), float(ltp))
            logger.debug(f"⚡️ Price update from WebSocket for {token}: {ltp}")
            
            # Queue for async processing
            if main_event_loop and hasattr(state, 'market_data_queue') and state.market_data_queue:
                try:
                    asyncio.run_coroutine_threadsafe(
                        state.market_data_queue.put({
                            'token': int(token),
                            'ltp': float(ltp),
                            'timestamp': data.get('ExchangeTimeStamp')
                        }),
                        main_event_loop
                    )
                except Exception as e:
                    logger.error(f"❌ Queue put error: {e}")
                    
    except Exception as e:
        logger.error(f"❌ Full data processing error: {e}")


def on_message1512_json_partial(data):
    """
    Partial market data update (1512)
    
    Contains: LTP only (faster updates)
    """
    try:
        if not isinstance(data, dict):
            return
        
        # Extract token and LTP
        token = data.get('ExchangeInstrumentID')
        ltp = data.get('LastTradedPrice') or data.get('Touchline', {}).get('LastTradedPrice')
        
        if token and ltp:
            # Update state immediately
            state.update_price(int(token), float(ltp))
            logger.debug(f"⚡️ Price update from WebSocket for {token}: {ltp}")
            
            # Queue for async processing
            if main_event_loop and hasattr(state, 'market_data_queue') and state.market_data_queue:
                try:
                    asyncio.run_coroutine_threadsafe(
                        state.market_data_queue.put({
                            'token': int(token),
                            'ltp': float(ltp),
                            'timestamp': data.get('ExchangeTimeStamp')
                        }),
                        main_event_loop
                    )
                except Exception as e:
                    logger.error(f"❌ Queue put error: {e}")
                    
    except Exception as e:
        logger.error(f"❌ Partial data processing error: {e}")
