"""
XTS Market Data Socket.IO Client - FIXED FOR COMPATIBILITY
"""

import configparser
import os
import logging
import json
import random
import threading
import time
import socketio
from typing import List, Dict, Optional, Callable, Any, Tuple


class MDSocket_io:
    """XTS Market Data Socket.IO Client - Compatible with python-socketio client

    Reconnects are owned by run_forever(): python-socketio's own reconnection is
    disabled so a dropped socket always comes back through one path that can
    back off, refresh the login token and replay subscriptions.
    """

    BACKOFF_INITIAL = 1.0
    BACKOFF_MAX = 60.0
    STABLE_AFTER = 60.0            # connection lasting this long resets backoff
    TOKEN_REFRESH_AFTER_FAILURES = 3
    STALL_TIMEOUT = 30.0           # connected but no ticks for this long -> reconnect
    WATCHDOG_INTERVAL = 5.0
    RESUBSCRIBE_BATCH = 50

    def __init__(
        self,
        token: str,
        userID: str,
        rest_client: Any = None,
        token_refresher: Optional[Callable[[], Tuple[str, str]]] = None,
        stall_check: Optional[Callable[[], bool]] = None,
    ):
        self.token = token
        self.userID = userID

        # rest_client: XTSConnect used to replay REST subscriptions on reconnect.
        # token_refresher: re-logs in and returns (token, userID).
        # stall_check: returns True when a tick silence should count as a stall
        #              (e.g. during market hours); None disables the watchdog.
        self.rest_client = rest_client
        self.token_refresher = token_refresher
        self.stall_check = stall_check

        # {message_code: {(segment, instrument_id): instrument}}
        self._subscriptions: Dict[int, Dict[Tuple[int, int], Dict]] = {}
        self._sub_lock = threading.Lock()

        self._stop_event = threading.Event()
        self._connected_at: Optional[float] = None
        self.last_message_at: Optional[float] = None
        self._auth_error = False

        self.sid = self._new_client()

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

        logging.info("🔌 XTS Socket initialized")

    @property
    def eventlistener(self):
        return self.sid

    def _new_client(self) -> socketio.Client:
        """Fresh Socket.IO client per connection attempt, reconnection handled by run_forever"""
        client = socketio.Client(
            logger=False,
            engineio_logger=False,
            reconnection=False,
        )
        self._register_event_handlers(client)
        return client

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

    def _register_event_handlers(self, client: socketio.Client):
        """Register all socket event handlers on a client"""
        client.on("connect", self._on_connect)
        client.on("disconnect", self._on_disconnect)
        client.on("connect_error", self._on_connect_error)

        client.on("1512-json-full", self._on_message1512_json_full)
        client.on("1512-json-partial", self._on_message1512_json_partial)

        client.on("1501-json-full", self._on_message1501_json_full)
        client.on("1501-json-partial", self._on_message1501_json_partial)

        client.on("1502-json-full", self._on_message1502_json_full)
        client.on("1502-json-partial", self._on_message1502_json_partial)

        client.on("1505-json-full", self._on_message1505_json_full)
        client.on("1505-json-partial", self._on_message1505_json_partial)

        client.on("1510-json-full", self._on_message1510_json_full)
        client.on("1510-json-partial", self._on_message1510_json_partial)

        logging.debug("✅ Socket.IO raw event handlers registered")

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

    # ── Supervisor ──────────────────────────────────────────────────────

    def run_forever(self, **connect_kwargs):
        """
        Keep the socket connected until stop() is called.

        Each pass makes one connect + wait. When it returns (drop, stall or
        failed connect) we back off exponentially with jitter and try again,
        refreshing the login token after an auth error or repeated failures.
        """
        self._stop_event.clear()
        if self.stall_check is not None:
            threading.Thread(target=self._watchdog_loop, name="md-socket-watchdog", daemon=True).start()

        backoff = self.BACKOFF_INITIAL
        failures = 0

        while not self._stop_event.is_set():
            started = time.monotonic()
            self._auth_error = False
            self.sid = self._new_client()

            try:
                self.connect(**connect_kwargs)
            except Exception as e:
                logging.error(f"❌ [SocketClient] Connect attempt failed: {e}")
                if any(k in str(e).lower() for k in ("401", "403", "unauthorized", "token")):
                    self._auth_error = True

            if self._stop_event.is_set():
                break

            if self._connected_at is not None and time.monotonic() - started >= self.STABLE_AFTER:
                backoff = self.BACKOFF_INITIAL
                failures = 0
            else:
                failures += 1
            self._connected_at = None

            if self._auth_error or failures >= self.TOKEN_REFRESH_AFTER_FAILURES:
                if self._refresh_token():
                    failures = 0

            delay = backoff * random.uniform(0.5, 1.0)
            logging.warning(
                f"🔁 [SocketClient] Reconnecting in {delay:.1f}s (consecutive failures={failures})"
            )
            self._stop_event.wait(delay)
            backoff = min(backoff * 2, self.BACKOFF_MAX)

        logging.info("🛑 [SocketClient] Supervisor stopped")

    def stop(self):
        """Stop run_forever and close the socket"""
        self._stop_event.set()
        self.disconnect()

    def _refresh_token(self) -> bool:
        if not self.token_refresher:
            return False
        try:
            logging.info("🔑 [SocketClient] Refreshing market data token...")
            self.token, self.userID = self.token_refresher()
            self._build_connection_url()
            logging.info("✅ [SocketClient] Market data token refreshed")
            return True
        except Exception as e:
            logging.error(f"❌ [SocketClient] Token refresh failed: {e}")
            return False

    def _watchdog_loop(self):
        """Force a reconnect when the socket is up but ticks have stopped"""
        while not self._stop_event.wait(self.WATCHDOG_INTERVAL):
            try:
                if not self.sid.connected or self._connected_at is None:
                    continue
                if not self.subscription_count() or not self.stall_check():
                    continue

                now = time.monotonic()
                last = max(self._connected_at, self.last_message_at or 0.0)
                if now - last >= self.STALL_TIMEOUT:
                    logging.warning(
                        f"⏱️ [SocketClient] No ticks for {now - last:.0f}s while connected, forcing reconnect"
                    )
                    self.sid.disconnect()
            except Exception as e:
                logging.error(f"❌ [SocketClient] Watchdog error: {e}")

    # ── Subscription registry ───────────────────────────────────────────

    def track_subscription(self, instruments: List[Dict], message_type: int):
        """Remember instruments so they are replayed after every reconnect"""
        with self._sub_lock:
            bucket = self._subscriptions.setdefault(int(message_type), {})
            for instr in instruments or []:
                try:
                    key = (int(instr["exchangeSegment"]), int(instr["exchangeInstrumentID"]))
                except (KeyError, TypeError, ValueError):
                    continue
                bucket[key] = {"exchangeSegment": key[0], "exchangeInstrumentID": key[1]}

    def untrack_subscription(self, instruments: List[Dict], message_type: int):
        with self._sub_lock:
            bucket = self._subscriptions.get(int(message_type), {})
            for instr in instruments or []:
                try:
                    bucket.pop((int(instr["exchangeSegment"]), int(instr["exchangeInstrumentID"])), None)
                except (KeyError, TypeError, ValueError):
                    continue

    def subscription_count(self) -> int:
        with self._sub_lock:
            return sum(len(b) for b in self._subscriptions.values())

    def _resubscribe_all(self):
        """Replay every tracked subscription over REST and the socket"""
        with self._sub_lock:
            snapshot = {code: list(b.values()) for code, b in self._subscriptions.items() if b}
        if not snapshot:
            return

        total = sum(len(v) for v in snapshot.values())
        logging.info(f"🔄 [SocketClient] Resubscribing {total} instruments across {len(snapshot)} message codes")

        for code, instruments in snapshot.items():
            for i in range(0, len(instruments), self.RESUBSCRIBE_BATCH):
                batch = instruments[i:i + self.RESUBSCRIBE_BATCH]
                if self.rest_client is not None:
                    try:
                        resp = self.rest_client.send_subscription(batch, code)
                        # XTS answers "already subscribed" for instruments it kept; that is fine.
                        if isinstance(resp, dict) and resp.get("type") != "success":
                            logging.info(
                                f"ℹ️ [SocketClient] REST resubscribe code={code} returned: "
                                f"{resp.get('description', resp)}"
                            )
                    except Exception as e:
                        logging.error(f"❌ [SocketClient] REST resubscribe error code={code}: {e}")
                try:
                    # No sid.connected check: it only flips True after the connect handler
                    # has fired, and emit() already raises if the namespace is gone.
                    self.sid.emit("join", {"instruments": batch, "xtsMessageCode": code})
                except Exception as e:
                    logging.error(f"❌ [SocketClient] Socket resubscribe error code={code}: {e}")

        logging.info("✅ [SocketClient] Resubscribe complete")

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
        self._connected_at = time.monotonic()
        # Off the socket's event thread: REST calls must not block incoming ticks.
        self.sid.start_background_task(self._resubscribe_all)
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
        text = str(error).lower()
        if any(k in text for k in ("token", "auth", "401", "403", "invalid user")):
            self._auth_error = True
        if self.on_error:
            try:
                self.on_error(error)
            except Exception as e:
                logging.error(f"❌ [SocketClient] on_error callback error: {e}", exc_info=True)

    def _on_message1512_json_full(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1512 FULL] {str(data)[:500]}")
        try:
            if self.on_message1512_json_full:
                self.on_message1512_json_full(data)
        except Exception as e:
            logging.error(f"1512 full handler error: {e}", exc_info=True)

    def _on_message1512_json_partial(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1512 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1512_json_partial:
                self.on_message1512_json_partial(data)
        except Exception as e:
            logging.error(f"1512 partial handler error: {e}", exc_info=True)

    def _on_message1501_json_full(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1501 FULL] {str(data)[:500]}")
        try:
            if self.on_message1501_json_full:
                self.on_message1501_json_full(data)
        except Exception as e:
            logging.error(f"1501 full handler error: {e}", exc_info=True)

    def _on_message1501_json_partial(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1501 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1501_json_partial:
                self.on_message1501_json_partial(data)
        except Exception as e:
            logging.error(f"1501 partial handler error: {e}", exc_info=True)

    def _on_message1502_json_full(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1502 FULL] {str(data)[:500]}")
        try:
            if self.on_message1502_json_full:
                self.on_message1502_json_full(data)
        except Exception as e:
            logging.error(f"1502 full handler error: {e}", exc_info=True)

    def _on_message1502_json_partial(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1502 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1502_json_partial:
                self.on_message1502_json_partial(data)
        except Exception as e:
            logging.error(f"1502 partial handler error: {e}", exc_info=True)

    def _on_message1505_json_full(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1505 FULL] {str(data)[:300]}")

    def _on_message1505_json_partial(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1505 PARTIAL] {str(data)[:300]}")

    def _on_message1510_json_full(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1510 FULL] {str(data)[:500]}")
        try:
            if self.on_message1510_json_full:
                self.on_message1510_json_full(data)
        except Exception as e:
            logging.error(f"1510 full handler error: {e}", exc_info=True)

    def _on_message1510_json_partial(self, data):
        data = self._safe_parse(data)
        self.last_message_at = time.monotonic()
        logging.info(f"[RAW SOCKET 1510 PARTIAL] {str(data)[:500]}")
        try:
            if self.on_message1510_json_partial:
                self.on_message1510_json_partial(data)
        except Exception as e:
            logging.error(f"1510 partial handler error: {e}", exc_info=True)

    def send_subscription(self, instruments: List[Dict], message_type: int = 1512) -> Dict:
        """Send subscription request via Socket.IO (tracked for replay on reconnect)"""
        self.track_subscription(instruments, message_type)
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
        self.untrack_subscription(instruments, message_type)
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
        return self.sid