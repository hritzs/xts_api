"""
tick_engine.py — Master tick loop for run_dev.
Triggered by ZeroMQ NIFTY_TICK signal.
Reads SHM chains → batch Greeks → updates TRADE_STATES.
"""
import asyncio
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Dict, List
import os

from utils.logger import logger
from core.state     import TRADE_STATES
from core.shared_memory import PriceSHM, ChainSHM
from core.zmq_bus   import TickSubscriber


_price_shm: PriceSHM = None
_chain_shms: Dict[str, ChainSHM] = {}
_tick_subscriber: TickSubscriber = None
_process_pool: ProcessPoolExecutor = None

# asyncio.Event — monitors wait on this, fired every tick
TICK_EVENT = asyncio.Event()
LATEST_CHAINS: Dict[str, dict] = {}   # symbol → chain dict, updated each tick


def _init_tick_engine():
    global _price_shm, _tick_subscriber, _process_pool
    _price_shm       = PriceSHM(create=False)
    _tick_subscriber = TickSubscriber()
    _process_pool    = ProcessPoolExecutor(max_workers=max(1, os.cpu_count() - 1))
    logger.info("✅ Tick engine initialized")


def get_price_shm() -> PriceSHM:
    return _price_shm


async def tick_engine_loop():
    """
    Runs forever in run_dev.
    Each ZeroMQ signal:
      1. Read SHM chain
      2. Update LTPs in TRADE_STATES
      3. Fire TICK_EVENT → all monitors wake simultaneously
    """
    _init_tick_engine()
    logger.info("⚡ Tick engine loop started — waiting for signals")

    while True:
        try:
            symbol = await asyncio.wait_for(_tick_subscriber.recv(), timeout=5.0)
            _on_tick(symbol)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            logger.info("⚡ Tick engine cancelled")
            break
        except Exception as e:
            logger.error(f"❌ Tick engine error: {e}")
            await asyncio.sleep(0.1)


def _on_tick(symbol: str):
    """Synchronous tick handler — called from async context."""
    global _chain_shms

    # Get or create ChainSHM reader
    if symbol not in _chain_shms:
        try:
            _chain_shms[symbol] = ChainSHM(symbol, create=False)
        except Exception as e:
            logger.warning(f"ChainSHM attach failed for {symbol}: {e}")
            return

    chain = _chain_shms[symbol].read()
    if not chain:
        return

    LATEST_CHAINS[symbol] = chain

    # Update TRADE_STATES LTPs for relevant trades
    fut_ltp = chain.get('fut_ltp', 0.0)
    for trade_uid, state in TRADE_STATES.items():
        if state.get('symbol', '').upper() == symbol.upper():
            state['spot'] = fut_ltp
            # Update CE/PE LTPs from price SHM
            ce_tok = state.get('ce_token')
            pe_tok = state.get('pe_token')
            if ce_tok and _price_shm:
                state['ce_ltp'] = _price_shm.get(int(ce_tok))
            if pe_tok and _price_shm:
                state['pe_ltp'] = _price_shm.get(int(pe_tok))

    # Fire event — all per-trade monitors wake simultaneously
    TICK_EVENT.set()
    TICK_EVENT.clear()