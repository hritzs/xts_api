"""
ZeroMQ pub/sub wrapper.
Publisher  — marketdata publishes NIFTY_TICK
           — reconciler publishes FILLS_UPDATED
Subscriber — run_dev subscribes to both
"""
import zmq
import zmq.asyncio
from utils.logger import logger
import config

# Ports
TICK_PUB_PORT  = getattr(config, 'ZMQ_TICK_PUB_PORT',  5563)
FILLS_PUB_PORT = getattr(config, 'ZMQ_FILLS_PUB_PORT', 5564)


class TickPublisher:
    """Used by marketdata_service. Call publish(symbol) on every chain update."""
    def __init__(self):
        self._ctx    = zmq.asyncio.Context.instance()
        self._socket = self._ctx.socket(zmq.PUB)
        self._socket.bind(f"tcp://*:{TICK_PUB_PORT}")
        logger.info(f"✅ TickPublisher bound to tcp://*:{TICK_PUB_PORT}")

    async def publish(self, symbol: str):
        await self._socket.send_string(symbol)

    def publish_sync(self, symbol: str):
        """Thread-safe sync publish from socket callback thread."""
        ctx    = zmq.Context.instance()
        sock   = ctx.socket(zmq.PUSH)
        sock.connect(f"tcp://localhost:{TICK_PUB_PORT}")
        sock.send_string(symbol)
        sock.close()

    def close(self):
        self._socket.close()


class FillsPublisher:
    """Used by order_reconciler. Publish after every reconcile cycle."""
    def __init__(self):
        self._ctx    = zmq.asyncio.Context.instance()
        self._socket = self._ctx.socket(zmq.PUB)
        self._socket.bind(f"tcp://*:{FILLS_PUB_PORT}")
        logger.info(f"✅ FillsPublisher bound to tcp://*:{FILLS_PUB_PORT}")

    async def publish(self, message: str = "FILLS_UPDATED"):
        await self._socket.send_string(message)

    def close(self):
        self._socket.close()


class TickSubscriber:
    """Used by run_dev. Awaits NIFTY_TICK signals."""
    def __init__(self):
        self._ctx    = zmq.asyncio.Context.instance()
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.connect(f"tcp://localhost:{TICK_PUB_PORT}")
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        logger.info(f"✅ TickSubscriber connected to tcp://localhost:{TICK_PUB_PORT}")

    async def recv(self) -> str:
        return await self._socket.recv_string()

    def close(self):
        self._socket.close()


class FillsSubscriber:
    """Used by run_dev. Awaits FILLS_UPDATED signals."""
    def __init__(self):
        self._ctx    = zmq.asyncio.Context.instance()
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.connect(f"tcp://localhost:{FILLS_PUB_PORT}")
        self._socket.setsockopt_string(zmq.SUBSCRIBE, "")
        logger.info(f"✅ FillsSubscriber connected to tcp://localhost:{FILLS_PUB_PORT}")

    async def recv(self) -> str:
        return await self._socket.recv_string()

    def close(self):
        self._socket.close()