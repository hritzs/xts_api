"""
resilient_task — supervised asyncio task wrapper.
Any exception → logged → task restarted after restart_delay.
Other tasks completely unaffected.
"""
import asyncio
import traceback
from utils.logger import logger


async def resilient_task(name: str, coro_fn, *args, restart_delay: float = 1.0, **kwargs):
    """
    Run coro_fn(*args, **kwargs) as a supervised task.
    On crash: log, wait restart_delay, restart automatically.
    """
    while True:
        try:
            logger.info(f"▶ [{name}] Starting")
            await coro_fn(*args, **kwargs)
            logger.warning(f"⚠ [{name}] Exited without error — restarting in {restart_delay}s")
        except asyncio.CancelledError:
            logger.info(f"⏹ [{name}] Cancelled — stopping")
            break
        except Exception as e:
            logger.error(f"❌ [{name}] Crashed: {e}\n{traceback.format_exc()}\n↻ Restarting in {restart_delay}s")
        await asyncio.sleep(restart_delay)