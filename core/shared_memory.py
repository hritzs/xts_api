"""
Shared Memory manager — inter-PROCESS data bridge.
Used between: marketdata_service ↔ run_dev ↔ order_reconciler
"""
import json
import mmap
import struct
import time
import numpy as np
from multiprocessing import shared_memory
from typing import Optional, Dict, Any
from utils.logger import logger

MAX_INSTRUMENTS = 50000
CHAIN_SHM_SIZE  = 4 * 1024 * 1024  # 4MB per symbol for chain JSON


class PriceSHM:
    """Fast numpy-backed price array — sub-millisecond reads."""

    def __init__(self, create: bool = False):
        self.create = create
        self._token_map: Dict[int, int] = {}
        self._next_idx = 0

        try:
            self.shm_prices = shared_memory.SharedMemory(
                name='prices_shm', create=create,
                size=MAX_INSTRUMENTS * 8
            )
        except FileExistsError:
            self.shm_prices = shared_memory.SharedMemory(
                name='prices_shm', create=False
            )
        try:
            self.shm_tokens = shared_memory.SharedMemory(
                name='tokens_shm', create=create,
                size=MAX_INSTRUMENTS * 8
            )
        except FileExistsError:
            self.shm_tokens = shared_memory.SharedMemory(
                name='tokens_shm', create=False
            )

        self.prices = np.ndarray(
            (MAX_INSTRUMENTS,), dtype=np.float64,
            buffer=self.shm_prices.buf
        )
        self.tokens = np.ndarray(
            (MAX_INSTRUMENTS,), dtype=np.int64,
            buffer=self.shm_tokens.buf
        )

        if create:
            self.prices[:] = 0.0
            self.tokens[:] = 0
        else:
            self._rebuild_map()

    def _rebuild_map(self):
        for i, tok in enumerate(self.tokens):
            if tok != 0:
                self._token_map[int(tok)] = i
        self._next_idx = len(self._token_map)

    def update(self, token: int, price: float):
        if token not in self._token_map:
            if self._next_idx >= MAX_INSTRUMENTS:
                return
            idx = self._next_idx
            self.tokens[idx] = token
            self._token_map[token] = idx
            self._next_idx += 1
        self.prices[self._token_map[token]] = price

    def get(self, token: int) -> float:
        idx = self._token_map.get(token)
        if idx is None:
            self._rebuild_map()
            idx = self._token_map.get(token)
        return float(self.prices[idx]) if idx is not None else 0.0

    def close(self, unlink: bool = False):
        self.shm_prices.close()
        self.shm_tokens.close()
        if unlink and self.create:
            try:
                self.shm_prices.unlink()
                self.shm_tokens.unlink()
            except Exception:
                pass


class ChainSHM:
    """
    Shared memory for option chain JSON per symbol.
    marketdata writes → run_dev + reconciler read.
    Uses a fixed-size SHM slot per symbol.
    """
    _HEADER = 8  # 4 bytes length + 4 bytes timestamp_ms

    def __init__(self, symbol: str, create: bool = False):
        self.symbol = symbol
        self.name   = f'chain_{symbol.lower()}_shm'
        try:
            self.shm = shared_memory.SharedMemory(
                name=self.name, create=create,
                size=CHAIN_SHM_SIZE
            )
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(
                name=self.name, create=False
            )

    def write(self, chain: dict):
        logger.info("[CHAIN SHM WRITE] symbol=%s", getattr(self, "symbol", "?"))
        data = json.dumps(chain).encode('utf-8')
        length = len(data)
        if length + self._HEADER > CHAIN_SHM_SIZE:
            logger.warning(f"ChainSHM: chain for {self.symbol} too large ({length} bytes)")
            return
        ts = int(time.time() * 1000) & 0xFFFFFFFF
        struct.pack_into('<II', self.shm.buf, 0, length, ts)
        self.shm.buf[self._HEADER:self._HEADER + length] = data

    def read(self) -> Optional[dict]:
        try:
            if self.shm is None:
                try:
                    self.shm = shared_memory.SharedMemory(name=self.name, create=False, size=CHAIN_SHM_SIZE)
                except Exception:
                    return None
            length, _ = struct.unpack_from('<II', self.shm.buf, 0)
            if length == 0 or length > CHAIN_SHM_SIZE - self._HEADER:
                return None
            raw = bytes(self.shm.buf[self._HEADER:self._HEADER + length])
            return json.loads(raw.decode('utf-8'))
        except Exception:
            return None

    def close(self, unlink: bool = False):
        self.shm.close()
        if unlink:
            try:
                self.shm.unlink()
            except Exception:
                pass


class OrderSHM:
    """
    Shared memory for ORDER_DICT + VERIFIED flags.
    order_reconciler writes → run_dev reads.
    Simple JSON blob, refreshed every 2s.
    """
    _NAME   = 'order_shm' 
    _SIZE   = 8 * 1024 * 1024  # 8MB — enough for 40k+ orders
    _HEADER = 8

    def __init__(self, create: bool = False):
        try:
            self.shm = shared_memory.SharedMemory(
                name=self._NAME, create=create,
                size=self._SIZE
            )
        except FileExistsError:
            self.shm = shared_memory.SharedMemory(
                name=self._NAME, create=False
            )

    def write(self, order_dict: dict, verified: dict):
        payload = json.dumps({
            'orders':   order_dict,
            'verified': verified,
            'ts':       time.time()
        }).encode('utf-8')
        length = len(payload)
        if length + self._HEADER > self._SIZE:
            logger.warning("OrderSHM: payload too large, truncating")
            return
        struct.pack_into('<II', self.shm.buf, 0, length, int(time.time()) & 0xFFFFFFFF)
        self.shm.buf[self._HEADER:self._HEADER + length] = payload

    def read(self) -> dict:
        try:
            length, _ = struct.unpack_from('<II', self.shm.buf, 0)
            if length == 0:
                return {'orders': {}, 'verified': {}}
            raw = bytes(self.shm.buf[self._HEADER:self._HEADER + length])
            return json.loads(raw.decode('utf-8'))
        except Exception:
            return {'orders': {}, 'verified': {}}

    def close(self, unlink: bool = False):
        self.shm.close()
        if unlink:
            try:
                self.shm.unlink()
            except Exception:
                pass