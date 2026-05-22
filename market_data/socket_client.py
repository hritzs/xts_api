"""
XTS Market Data Socket.IO Client - Production Ready
Compatible with python-socketio v5.x
"""

import configparser
import os
import logging
import json
import socketio
from typing import Optional, Callable, Any, List, Dict


class MDSocket_io:
    """XTS Market Data Socket.IO Client"""

    def __init__(self, token: str, userID: str):
        self.token = token
        self.userID = userID

        self.sid = socketio.Client(
            logger=False,
            engineio_logger=False,
            reconnection=True,
            reconnection_attempts=5,
            reconnection_delay=2,
            reconnection_delay_max=10
        )

        self.eventlistener = self.sid

        # External callbacks — set these from your wiring code
        self.on_connect = None
        self.on_disconnect = None
        self.on_error = None

        # FO options / futures / equities
        self.on_message1512_json_full = None
        self.on_message1512_json_partial = None

        # ✅ Touchline — cash indices (NSE + BSE both use 1501)
        self.on_message1501_json_full = None
        self.on_message1501_json_partial = None

        # Market depth (not used for LTP, kept for completeness)
        self.on_message1502_json_full = None
        self.on_message1502_json_partial = None

        # Open interest
        self.on_message1510_json_full = None
        self.on_message1510_json_partial = None

        self._load_config()
        self._build_connection_url()
        self._register_event_handlers()

        logging.info("🔌 XTS Socket initialized")

    def _load_config(self):
        try:
            configParser = configparser.ConfigParser()
            configParser.read(os.path.join(os.getcwd(), 'config.ini'))
            self.port = configParser.get('root_url', 'root')
            self.broadcastMode = configParser.get('root_url', 'broadcastMode')
        except Exception as e:
            logging.error(f"Config error: {e}")
            self.port = "https://developers.symphonyfintech.in"
            self.broadcastMode = "Full"

    def _build_connection_url(self):
        publishFormat = 'JSON'
        self.connection_url = (
            f"{self.port}/?token={self.token}"
            f"&userID={self.userID}"
            f"&publishFormat={publishFormat}"
            f"&broadcastMode={self.broadcastMode}"
        )

    def _register_event_handlers(self):
        self.sid.on('connect', self._on_connect)
        self.sid.on('disconnect', self._on_disconnect)
        self.sid.on('connect_error', self._on_connect_error)

        # 1512 — FO LTP (options, futures)
        self.sid.on('1512-json-full',    self._on_message1512_json_full)
        self.sid.on('1512-json-partial', self._on_message1512_json_partial)

        # 1501 — Touchline / cash index spot price ✅
        self.sid.on('1501-json-full',    self._on_message1501_json_full)
        self.sid.on('1501-json-partial', self._on_message1501_json_partial)

        # 1502 — Market Depth
        self.sid.on('1502-json-full',    self._on_message1502_json_full)
        self.sid.on('1502-json-partial', self._on_message1502_json_partial)

        # 1505 — Candle
        self.sid.on('1505-json-full',    self._on_message1505_json_full)
        self.sid.on('1505-json-partial', self._on_message1505_json_partial)

        # 1510 — Open Interest
        self.sid.on('1510-json-full',    self._on_message1510_json_full)
        self.sid.on('1510-json-partial', self._on_message1510_json_partial)

    def connect(self, headers: dict = None, transports: str = 'websocket',
                namespaces: list = None,
                socketio_path: str = '/apimarketdata/socket.io',
                verify: bool = True):
        try:
            logging.info("🚀 Connecting to XTS Market Data Socket...")
            logging.info(f"📋 URL: {self.connection_url[:80]}...")
            self.sid.connect(
                self.connection_url,
                headers=headers or {},
                transports=transports,
                namespaces=namespaces,
                socketio_path=socketio_path,
                wait_timeout=20
            )
            self.sid.wait()
        except KeyboardInterrupt:
            logging.info("🛑 Connection interrupted by user")
            self.disconnect()
        except Exception as e:
            logging.error(f"❌ Socket connect error: {e}")
            raise

    # ── Connection handlers ──────────────────────────────────────────────────

    def _on_connect(self):
        logging.info("✅ XTS Market Data Socket CONNECTED")
        if self.on_connect:
            self.on_connect()

    def _on_disconnect(self):
        logging.warning("🔌 XTS Market Data Socket DISCONNECTED")
        if self.on_disconnect:
            self.on_disconnect()

    def _on_connect_error(self, error):
        logging.error(f"❌ XTS Socket Connection Error: {error}")
        if self.on_error:
            self.on_error(error)

    # ── 1512 handlers ────────────────────────────────────────────────────────

    def _on_message1512_json_full(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1512 FULL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            if self.on_message1512_json_full:
                self.on_message1512_json_full(data)
        except Exception as e:
            logging.error(f"1512 full handler error: {e}")

    def _on_message1512_json_partial(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1512 PARTIAL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            if self.on_message1512_json_partial:
                self.on_message1512_json_partial(data)
        except Exception as e:
            logging.error(f"1512 partial handler error: {e}")

    # ── 1501 handlers — Touchline / cash index spot ✅ ─────────────────────

    def _on_message1501_json_full(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1501 FULL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            if self.on_message1501_json_full:
                self.on_message1501_json_full(data)
        except Exception as e:
            logging.error(f"1501 full handler error: {e}")

    def _on_message1501_json_partial(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1501 PARTIAL: Token={data.get('ExchangeInstrumentID')}, LTP={data.get('LastTradedPrice')}")
            if self.on_message1501_json_partial:
                self.on_message1501_json_partial(data)
        except Exception as e:
            logging.error(f"1501 partial handler error: {e}")

    # ── 1502 handlers — Market Depth ─────────────────────────────────────────

    def _on_message1502_json_full(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1502 FULL: Token={data.get('ExchangeInstrumentID')}")
            if self.on_message1502_json_full:
                self.on_message1502_json_full(data)
        except Exception as e:
            logging.error(f"1502 full handler error: {e}")

    def _on_message1502_json_partial(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1502 PARTIAL: Token={data.get('ExchangeInstrumentID')}")
            if self.on_message1502_json_partial:
                self.on_message1502_json_partial(data)
        except Exception as e:
            logging.error(f"1502 partial handler error: {e}")

    # ── 1505/1510 handlers ───────────────────────────────────────────────────

    def _on_message1505_json_full(self, data):
        logging.debug("1505 FULL received")

    def _on_message1505_json_partial(self, data):
        logging.debug("1505 PARTIAL received")

    def _on_message1510_json_full(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1510 FULL: Token={data.get('ExchangeInstrumentID')}")
            if self.on_message1510_json_full:
                self.on_message1510_json_full(data)
        except Exception as e:
            logging.error(f"1510 full handler error: {e}")

    def _on_message1510_json_partial(self, data):
        try:
            if isinstance(data, str):
                data = json.loads(data)
            logging.debug(f"🔔 1510 PARTIAL: Token={data.get('ExchangeInstrumentID')}")
            if self.on_message1510_json_partial:
                self.on_message1510_json_partial(data)
        except Exception as e:
            logging.error(f"1510 partial handler error: {e}")

    # ── Subscription ─────────────────────────────────────────────────────────

    def send_subscription(self, instruments: List[Dict], message_type: int = 1512) -> Dict:
        try:
            if not self.sid.connected:
                logging.warning("⚠️ Socket not connected, cannot subscribe")
                return {'type': 'error', 'description': 'Socket not connected'}

            payload = {
                'instruments': instruments,
                'xtsMessageCode': message_type
            }
            self.sid.emit('join', payload)
            logging.info(f"📡 Subscribed: {len(instruments)} instruments (code={message_type})")
            return {'type': 'success', 'count': len(instruments)}

        except Exception as e:
            logging.error(f"❌ Socket subscription error: {e}")
            return {'type': 'error', 'description': str(e)}

    def send_unsubscription(self, instruments: List[Dict], message_type: int = 1512) -> Dict:
        try:
            if not self.sid.connected:
                return {'type': 'error', 'description': 'Socket not connected'}

            payload = {
                'instruments': instruments,
                'xtsMessageCode': message_type
            }
            self.sid.emit('leave', payload)
            logging.info(f"📡 Unsubscribed: {len(instruments)} instruments")
            return {'type': 'success', 'count': len(instruments)}

        except Exception as e:
            logging.error(f"❌ Socket unsubscription error: {e}")
            return {'type': 'error', 'description': str(e)}

    def disconnect(self):
        try:
            if self.sid.connected:
                self.sid.disconnect()
                logging.info("🔌 Socket disconnected gracefully")
        except Exception as e:
            logging.warning(f"Disconnect warning: {e}")

    def get_emitter(self):
        return self.eventlistener
