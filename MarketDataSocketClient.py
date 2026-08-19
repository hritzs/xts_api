"""
XTS Market Data Socket.IO Client - FIXED FOR COMPATIBILITY
"""

import configparser
import os
import logging
import json
import socketio
from typing import List, Dict, Optional, Callable, Any


class MDSocket_io:
    """XTS Market Data Socket.IO Client - Compatible with python-socketio client"""

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

        self.on_connect = None
        self.on_disconnect = None
        self.on_error = None

        self.on_message1512_json_full = None
        self.on_message1512_json_partial = None

        self.on_message1510_json_full = None
        self.on_message1510_json_partial = None

        self.on_message1502_json_full = None
        self.on_message1502_json_partial = None

        self.on_message1501_json_full = None
        self.on_message1501_json_partial = None

        self._load_config()
        self._build_connection_url()
        self._register_event_handlers()

        logging.info("🔌 XTS Socket initialized")

    def _load_config(self):
        """Load config.ini"""
        try:
            curr_dir = os.getcwd()
            config_parser = configparser.ConfigParser()
            config_file_path = os.path.join(curr_dir, "config.ini")
            config_parser.read(config_file_path)

            self.port = config_parser.get("root_url", "root")
            self.broadcastMode = config_parser.get("root_url", "broadcastMode")

        except Exception as e:
            logging.error(f"Config error: {e}")
            self.port = "https://developers.symphonyfintech.in"
            self.broadcastMode = "Full"

    def _build_connection_url(self):
        """Build XTS connection URL"""
        publish_format = "JSON"
        self.connection_url = (
            f"{self.port}/?token={self.token}"
            f"&userID={self.userID}"
            f"&publishFormat={publish_format}"
            f"&broadcastMode={self.broadcastMode}"
        )

    def _register_event_handlers(self):
        """Register all socket event handlers once"""
        self.sid.on("connect", self._on_connect)
        self.sid.on("disconnect", self._on_disconnect)
        self.sid.on("connect_error", self._on_connect_error)

        self.sid.on("1512-json-full", self._on_message1512_json_full)
        self.sid.on("1512-json-partial", self._on_message1512_json_partial)

        self.sid.on("1501-json-full", self._on_message1501_json_full)
        self.sid.on("1501-json-partial", self._on_message1501_json_partial)

        self.sid.on("1502-json-full", self._on_message1502_json_full)
        self.sid.on("1502-json-partial", self._on_message1502_json_partial)

        self.sid.on("1505-json-full", self._on_message1505_json_full)
        self.sid.on("1505-json-partial", self._on_message1505_json_partial)

        self.sid.on("1510-json-full", self._on_message1510_json_full)
        self.sid.on("1510-json-partial", self._on_message1510_json_partial)

        logging.info("✅ Socket.IO raw event handlers registered")

    def connect(
        self,
        headers: Optional[dict] = None,
        transports: str = "websocket",
        namespaces: Optional[list] = None,
        socketio_path: str = "/apimarketdata/socket.io",
        verify: bool = True,
    ):
        """Connect to XTS Market Data Socket"""
        try:
            logging.info("🚀 Connecting to XTS Market Data Socket...")
            logging.info(f"📋 URL: {self.connection_url[:120]}...")
            logging.info(f"📋 Path: {socketio_path}")

            self.sid.connect(
                self.connection_url,
                headers=headers or {},
                transports=transports,
                namespaces=namespaces,
                socketio_path=socketio_path,
                wait_timeout=20
            )

            logging.info("✅ Socket connect call sent")
            self.sid.wait()

        except KeyboardInterrupt:
            logging.info("🛑 Connection interrupted by user")
            self.disconnect()
        except Exception as e:
            logging.error(f"❌ Socket connect error: {e}", exc_info=True)
            raise

    def _safe_parse(self, data):
        if isinstance(data, str):
            try:
                return json.loads(data)
            except Exception as e:
                logging.error(f"JSON parse error: {e} | raw={str(data)[:500]}")
                return {}
        return data if isinstance(data, dict) else {}

    def _on_connect(self):
        logging.info("✅ [SocketClient] XTS Market Data Socket CONNECTED")
        if self.on_connect:
            try:
                self.on_connect()
            except Exception as e:
                logging.error(f"❌ [SocketClient] on_connect callback error: {e}", exc_info=True)

    def _on_disconnect(self):
        logging.warning("🔌 [SocketClient] XTS Market Data Socket DISCONNECTED")
        if self.on_disconnect:
            try:
                self.on_disconnect()
            except Exception as e:
                logging.error(f"❌ [SocketClient] on_disconnect callback error: {e}", exc_info=True)

    def _on_connect_error(self, error):
        logging.error(f"❌ [SocketClient] XTS Socket Connection Error: {error}")
        if self.on_error:
            try:
                self.on_error(error)
            except Exception as e:
                logging.error(f"❌ [SocketClient] on_error callback error: {e}", exc_info=True)

    def _on_message1512_json_full(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1512 FULL] {str(data)[:500]}")
        try:
            if self.on_message1512_json_full:
                self.on_message1512_json_full(data)
        except Exception as e:
            logging.error(f"1512 full handler error: {e}", exc_info=True)

    def _on_message1512_json_partial(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1512 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1512_json_partial:
                self.on_message1512_json_partial(data)
        except Exception as e:
            logging.error(f"1512 partial handler error: {e}", exc_info=True)

    def _on_message1501_json_full(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1501 FULL] {str(data)[:500]}")
        try:
            if self.on_message1501_json_full:
                self.on_message1501_json_full(data)
        except Exception as e:
            logging.error(f"1501 full handler error: {e}", exc_info=True)

    def _on_message1501_json_partial(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1501 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1501_json_partial:
                self.on_message1501_json_partial(data)
        except Exception as e:
            logging.error(f"1501 partial handler error: {e}", exc_info=True)

    def _on_message1502_json_full(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1502 FULL] {str(data)[:500]}")
        try:
            if self.on_message1502_json_full:
                self.on_message1502_json_full(data)
        except Exception as e:
            logging.error(f"1502 full handler error: {e}", exc_info=True)

    def _on_message1502_json_partial(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1502 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1502_json_partial:
                self.on_message1502_json_partial(data)
        except Exception as e:
            logging.error(f"1502 partial handler error: {e}", exc_info=True)

    def _on_message1505_json_full(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1505 FULL] {str(data)[:300]}")

    def _on_message1505_json_partial(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1505 PARTIAL] {str(data)[:300]}")

    def _on_message1510_json_full(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1510 FULL] {str(data)[:500]}")
        try:
            if self.on_message1510_json_full:
                self.on_message1510_json_full(data)
        except Exception as e:
            logging.error(f"1510 full handler error: {e}", exc_info=True)

    def _on_message1510_json_partial(self, data):
        data = self._safe_parse(data)
        logging.info(f"[RAW SOCKET 1510 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1510_json_partial:
                self.on_message1510_json_partial(data)
        except Exception as e:
            logging.error(f"1510 partial handler error: {e}", exc_info=True)

    def send_subscription(self, instruments: List[Dict], message_type: int = 1512) -> Dict:
        """Send subscription request via Socket.IO"""
        try:
            if not self.sid.connected:
                logging.warning("⚠️ Socket not connected, cannot subscribe via socket")
                return {"type": "error", "description": "Socket not connected"}

            payload = {
                "instruments": instruments,
                "xtsMessageCode": message_type
            }

            self.sid.emit("join", payload)

            logging.info(
                f"📡 Socket subscription sent via join | "
                f"count={len(instruments)} code={message_type}"
            )
            logging.debug(f"📡 Subscription payload: {payload}")

            return {"type": "success", "count": len(instruments), "code": message_type}

        except Exception as e:
            logging.error(f"❌ Socket subscription error: {e}", exc_info=True)
            return {"type": "error", "description": str(e)}

    def send_unsubscription(self, instruments: List[Dict], message_type: int = 1512) -> Dict:
        """Unsubscribe from instruments via Socket.IO"""
        try:
            if not self.sid.connected:
                return {"type": "error", "description": "Socket not connected"}

            payload = {
                "instruments": instruments,
                "xtsMessageCode": message_type
            }

            self.sid.emit("leave", payload)

            logging.info(
                f"📡 Socket unsubscription sent via leave | "
                f"count={len(instruments)} code={message_type}"
            )
            logging.debug(f"📡 Unsubscription payload: {payload}")

            return {"type": "success", "count": len(instruments), "code": message_type}

        except Exception as e:
            logging.error(f"❌ Socket unsubscription error: {e}", exc_info=True)
            return {"type": "error", "description": str(e)}

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