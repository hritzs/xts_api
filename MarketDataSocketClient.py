"""
XTS Market Data Socket.IO Client - FIXED FOR COMPATIBILITY
"""

import configparser
import os
import logging
import json
from datetime import datetime
import socketio
from typing import Optional, Callable, Any, List, Dict

class MDSocket_io:
    """XTS Market Data Socket.IO Client - Compatible with all socketio versions"""
    
    def __init__(self, token: str, userID: str):
        """
        Initialize XTS Market Data Socket
        """
        self.token = token
        self.userID = userID
        
        # ✅ FIXED: Simple initialization without version parameter
        self.sid = socketio.Client(
            logger=False,
            engineio_logger=False,
            reconnection=True,
            reconnection_attempts=5,
            reconnection_delay=2,
            reconnection_delay_max=10
        )
        
        # Event emitter reference
        self.eventlistener = self.sid
        
        # ✅ EXTERNAL CALLBACKS - Override these from dashboard
        self.on_connect = None
        self.on_disconnect = None
        self.on_error = None
        self.on_message1512_json_full = None
        self.on_message1512_json_partial = None
        # ✅ ADDED: External callbacks for cash index ticks
        self.on_message1510_json_full = None
        self.on_message1510_json_partial = None
        # ✅ ADDED: External callbacks for BSE cash index ticks (1502)
        self.on_message1502_json_full = None
        self.on_message1502_json_partial = None
        # ✅ ADDED: External callbacks for BSE cash index ticks (1502)
        self.on_message1502_json_full = None
        self.on_message1502_json_partial = None
        # ✅ ADDED: External callbacks for Touchline (1501)
        self.on_message1501_json_full = None
        self.on_message1501_json_partial = None
        
        # Load config
        self._load_config()
        
        # Build connection URL
        self._build_connection_url()
        
        # ✅ Register event handlers
        self._register_event_handlers()
        
        logging.info(f"🔌 XTS Socket initialized")
    
    def _load_config(self):
        """Load config.ini"""
        try:
            currDirMain = os.getcwd()
            configParser = configparser.ConfigParser()
            configFilePath = os.path.join(currDirMain, 'config.ini')
            configParser.read(configFilePath)
            
            self.port = configParser.get('root_url', 'root')
            self.broadcastMode = configParser.get('root_url', 'broadcastMode')
            
        except Exception as e:
            logging.error(f"Config error: {e}")
            # Fallback defaults
            self.port = "https://developers.symphonyfintech.in"
            self.broadcastMode = "Full"
    
    def _build_connection_url(self):
        """Build XTS connection URL"""
        publishFormat = 'JSON'
        self.connection_url = (
            f"{self.port}/?token={self.token}"
            f"&userID={self.userID}"
            f"&publishFormat={publishFormat}"
            f"&broadcastMode={self.broadcastMode}"
        )
    
    def _register_event_handlers(self):
        """Register all event handlers"""
        # Connection events
        self.sid.on('connect', self._on_connect)
        self.sid.on('disconnect', self._on_disconnect)
        self.sid.on('connect_error', self._on_connect_error)
        
        # Market data events (1512 = LTP)
        self.sid.on('1512-json-full', self._on_message1512_json_full)
        self.sid.on('1512-json-partial', self._on_message1512_json_partial)
        
        # Other market data events
        self.sid.on('1501-json-full', self._on_message1501_json_full)
        self.sid.on('1501-json-partial', self._on_message1501_json_partial)
        self.sid.on('1502-json-full', self._on_message1502_json_full)
        self.sid.on('1502-json-partial', self._on_message1502_json_partial)
        self.sid.on('1505-json-full', self._on_message1505_json_full)
        self.sid.on('1505-json-partial', self._on_message1505_json_partial)
        self.sid.on('1510-json-full', self._on_message1510_json_full)
        self.sid.on('1510-json-partial', self._on_message1510_json_partial)
    
    def connect(self, headers: dict = None, transports: str = 'websocket', 
                namespaces: list = None, socketio_path: str = '/apimarketdata/socket.io',
                verify: bool = True):
        """Connect to XTS Market Data Socket"""
        try:
            logging.info("🚀 Connecting to XTS Market Data Socket...")
            logging.info(f"📋 URL: {self.connection_url[:80]}...")
            logging.info(f"📋 Path: {socketio_path}")
            
            # ✅ FIXED: Simple connect call
            self.sid.connect(
                self.connection_url,
                headers=headers or {},
                transports=transports,
                namespaces=namespaces,
                socketio_path=socketio_path,
                wait_timeout=20
            )
            
            logging.info("✅ Attempting connection...")
            
            # ✅ IMPORTANT: Wait for connection
            self.sid.wait()
            
        except KeyboardInterrupt:
            logging.info("🛑 Connection interrupted by user")
            self.disconnect()
        except Exception as e:
            logging.error(f"❌ Socket connect error: {e}")
            raise
    
    def _on_connect(self):
        """Internal connect handler"""
        logging.info("✅ XTS Market Data Socket CONNECTED")
        if self.on_connect:
            self.on_connect()
    
    def _on_disconnect(self):
        """Internal disconnect handler"""
        logging.warning("🔌 XTS Market Data Socket DISCONNECTED")
        if self.on_disconnect:
            self.on_disconnect()
    
    def _on_connect_error(self, error):
        """Internal connect error handler"""
        logging.error(f"❌ XTS Socket Connection Error: {error}")
        if self.on_error:
            self.on_error(error)
    
    # ✅ 1512 LTP HANDLERS
    def _on_message1512_json_full(self, data):
        """Handle 1512 Full LTP Message"""
        try:
            # Parse if string
            if isinstance(data, str):
                data = json.loads(data)
            
            logging.debug(f"🔔 1512 FULL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            
            if self.on_message1512_json_full:
                self.on_message1512_json_full(data)
        except Exception as e:
            logging.error(f"1512 full handler error: {e}")
    
    def _on_message1512_json_partial(self, data):
        """Handle 1512 Partial LTP Message"""
        try:
            # Parse if string
            if isinstance(data, str):
                data = json.loads(data)
            
            logging.debug(f"🔔 1512 PARTIAL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            
            if self.on_message1512_json_partial:
                self.on_message1512_json_partial(data)
        except Exception as e:
            logging.error(f"1512 partial handler error: {e}")
    
    # Other handlers (basic implementation)
    def _on_message1501_json_full(self, data):
        """Handle 1501 Full Touchline Message (for cash indices)"""
        try:
            if isinstance(data, str): data = json.loads(data)
            logging.debug(f"🔔 1501 FULL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            if self.on_message1501_json_full:
                self.on_message1501_json_full(data)
        except Exception as e:
            logging.error(f"1501 full handler error: {e}")
    
    def _on_message1501_json_partial(self, data):
        """Handle 1501 Partial Touchline Message (for cash indices)"""
        try:
            if isinstance(data, str): data = json.loads(data)
            logging.debug(f"🔔 1501 PARTIAL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            if self.on_message1501_json_partial:
                self.on_message1501_json_partial(data)
        except Exception as e:
            logging.error(f"1501 partial handler error: {e}")
    
    def _on_message1502_json_full(self, data):
        """Handle 1502 Full Index/Quote Message"""
        try:
            if isinstance(data, str): data = json.loads(data)
            logging.debug(f"🔔 1502 FULL: Token={data.get('ExchangeInstrumentID')}, IndexValue={data.get('IndexValue')}")
            if self.on_message1502_json_full:
                self.on_message1502_json_full(data)
        except Exception as e:
            logging.error(f"1502 full handler error: {e}")
    
    def _on_message1502_json_partial(self, data):
        """Handle 1502 Partial Index/Quote Message"""
        try:
            if isinstance(data, str): data = json.loads(data)
            logging.debug(f"🔔 1502 PARTIAL: Token={data.get('ExchangeInstrumentID')}, IndexValue={data.get('IndexValue')}")
            if self.on_message1502_json_partial:
                self.on_message1502_json_partial(data)
        except Exception as e:
            logging.error(f"1502 partial handler error: {e}")

    def _on_message1505_json_full(self, data):
        logging.debug("1505 FULL received")
    
    def _on_message1505_json_partial(self, data):
        logging.debug("1505 PARTIAL received")
    
    def _on_message1510_json_full(self, data):
        """Handle 1510 Full Index LTP Message"""
        try:
            if isinstance(data, str):
                data = json.loads(data)
            
            logging.debug(f"🔔 1510 FULL: Token={data.get('ExchangeInstrumentID')}, IndexValue={data.get('IndexValue')}")
            
            if self.on_message1510_json_full:
                self.on_message1510_json_full(data)
        except Exception as e:
            logging.error(f"1510 full handler error: {e}")
    
    def _on_message1510_json_partial(self, data):
        """Handle 1510 Partial Index LTP Message"""
        try:
            if isinstance(data, str):
                data = json.loads(data)
            
            logging.debug(f"🔔 1510 PARTIAL: Token={data.get('ExchangeInstrumentID')}, IndexValue={data.get('IndexValue')}")
            
            if self.on_message1510_json_partial:
                self.on_message1510_json_partial(data)
        except Exception as e:
            logging.error(f"1510 partial handler error: {e}")
    
    def send_subscription(self, instruments: List[Dict], message_type: int = 1512) -> Dict:
        """
        Send subscription request via Socket.IO
        NOTE: This uses Socket.IO emit, not REST API
        """
        try:
            if not self.sid.connected:
                logging.warning("⚠️ Socket not connected, cannot subscribe via socket")
                return {'type': 'error', 'description': 'Socket not connected'}
            
            # ✅ XTS expects this format for socket subscription
            subscription_payload = {
                'instruments': instruments,
                'xtsMessageCode': message_type
            }
            
            # ✅ Emit 'join' event (XTS Socket.IO convention)
            self.sid.emit('join', subscription_payload)
            
            logging.info(f"📡 Socket subscription sent via 'join' event: {len(instruments)} instruments (code={message_type})")
            logging.debug(f"   - Payload: {subscription_payload}")
            return {'type': 'success', 'count': len(instruments)}
            
        except Exception as e:
            logging.error(f"❌ Socket subscription error: {e}")
            return {'type': 'error', 'description': str(e)}
    
    def send_unsubscription(self, instruments: List[Dict], message_type: int = 1512) -> Dict:
        """Unsubscribe from instruments via Socket.IO"""
        try:
            if not self.sid.connected:
                return {'type': 'error', 'description': 'Socket not connected'}
            
            unsubscription_payload = {
                'instruments': instruments,
                'xtsMessageCode': message_type
            }
            
            # ✅ Emit 'leave' event (XTS Socket.IO convention)
            self.sid.emit('leave', unsubscription_payload)
            
            logging.info(f"📡 Socket unsubscription sent: {len(instruments)} instruments")
            return {'type': 'success', 'count': len(instruments)}
            
        except Exception as e:
            logging.error(f"❌ Socket unsubscription error: {e}")
            return {'type': 'error', 'description': str(e)}
    
    def disconnect(self):
        """Graceful disconnect"""
        try:
            if self.sid.connected:
                self.sid.disconnect()
                logging.info("🔌 Socket disconnected gracefully")
        except Exception as e:
            logging.warning(f"Disconnect warning: {e}")
    
    def get_emitter(self):
        """Get event emitter for advanced usage"""
        return self.eventlistener


# ==============================================================================
# TEST FUNCTION
# ==============================================================================

def test_connection():
    """Test Socket.IO connection"""
    import time
    
    # Your test credentials
    TOKEN = "your_token_here"
    USERID = "your_userid_here"
    
    # Create socket
    md_socket = MDSocket_io(TOKEN, USERID)
    
    # Counters
    message_count = {'count': 0}
    
    def on_connect():
        print("✅ TEST: Socket Connected!")
        
        # Subscribe to test instrument
        instruments = [
            {'exchangeSegment': 2, 'exchangeInstrumentID': 58725}  # Example token
        ]
        result = md_socket.send_subscription(instruments, 1512)
        print(f"📡 Subscription result: {result}")
    
    def on_1512_full(data):
        message_count['count'] += 1
        token = data.get('ExchangeInstrumentID')
        ltp = data.get('LastTradedPrice', 0)
        print(f"🔔 #{message_count['count']} | 1512 FULL: Token={token}, LTP=₹{ltp}")
    
    def on_1512_partial(data):
        message_count['count'] += 1
        token = data.get('ExchangeInstrumentID')
        ltp = data.get('LastTradedPrice', 0)
        print(f"🔔 #{message_count['count']} | 1512 PARTIAL: Token={token}, LTP=₹{ltp}")
    
    def on_disconnect():
        print("🔌 TEST: Socket Disconnected")
    
    def on_error(error):
        print(f"❌ TEST: Socket Error: {error}")
    
    # Set handlers
    md_socket.on_connect = on_connect
    md_socket.on_message1512_json_full = on_1512_full
    md_socket.on_message1512_json_partial = on_1512_partial
    md_socket.on_disconnect = on_disconnect
    md_socket.on_error = on_error
    
    # Connect & test
    try:
        print("🚀 Starting test...")
        md_socket.connect()
    except KeyboardInterrupt:
        print("\n🛑 Test stopped by user")
    except Exception as e:
        print(f"❌ Test failed: {e}")
    finally:
        md_socket.disconnect()
        print(f"📊 Total messages received: {message_count['count']}")


if __name__ == "__main__":
    # Run test
    print("=" * 60)
    print("XTS Market Data Socket.IO Test")
    print("=" * 60)
    test_connection()
