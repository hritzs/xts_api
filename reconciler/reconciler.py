"""
Heavy CPU reconciliation loop.
Runs every 2s, parses all orders, verifies per trade, writes SHM.
"""
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List
from utils.logger import logger
from database.db_manager import Database
from reconciler.order_book import fetch_order_book_raw
from reconciler.verifier   import verify_all_trades
from core.shared_memory    import OrderSHM
from core.zmq_bus          import FillsPublisher


class ReconcilerLoop:
    def __init__(self):
        self._pending: Dict[str, dict] = {}
        self._results: Dict[str, dict] = {}
        self._executor  = ThreadPoolExecutor(max_workers=4)
        self._order_shm = OrderSHM(create=True)
        self._publisher = FillsPublisher()
        self._db        = Database()
        self._order_dict: Dict[str, dict] = {}
        self._verified:   Dict[str, bool]  = {}

    def add_batch(self, batch_name: str, order_ids: List[str], trade_uid: str):
        self._pending[batch_name] = {
            'order_ids':    order_ids,
            'trade_uid':    trade_uid,
            'submitted_at': time.time(),
            'attempts':     0,
        }
        logger.info(f"📬 Queued: {batch_name} ({len(order_ids)} orders, trade={trade_uid})")

    async def run(self):
        logger.info("🔄 Reconciler loop started (2s interval)")
        await asyncio.sleep(2.0)
        while True:
            try:
                await asyncio.sleep(2.0)
                await self._cycle()
            except asyncio.CancelledError:
                logger.info("🔄 Reconciler loop cancelled")
                break
            except Exception as e:
                logger.error(f"❌ Reconciler error: {e}")
                await asyncio.sleep(5.0)

    async def _cycle(self):
        # 1. Fetch raw order book
        order_book = await fetch_order_book_raw(self._executor)
        if order_book is None:
            logger.warning("Reconciler: order book fetch failed — skipping cycle.")
            return
        if not order_book:
            # Empty order book is valid (no orders placed yet) — continue cycle
            pass

        # 2. Parse + build ORDER_DICT
        self._order_dict = self._parse_order_book(order_book)

        # 3. Increment attempts on all pending
        for b in self._pending.values():
            b['attempts'] += 1

        # 4. Verify all pending trades in parallel
        if self._pending:
            results = await verify_all_trades(
                self._pending, order_book, self._db
            )
            completed = []
            for batch_name, result in results.items():
                if result:
                    self._results[batch_name] = result
                    trade_uid = result['trade_uid']
                    self._verified[trade_uid] = len(result['failed']) == 0
                    completed.append(batch_name)

            for b in completed:
                self._pending.pop(b, None)

        # 5. Write to SHM so run_dev can read
        self._order_shm.write(self._order_dict, self._verified)

        # 6. Signal run_dev
        await self._publisher.publish("FILLS_UPDATED")

        # 7. Prune old results
        if len(self._results) > 500:
            oldest = sorted(self._results.keys())[:len(self._results) - 500]
            for k in oldest:
                self._results.pop(k, None)

    def _parse_order_book(self, orders: List[dict]) -> Dict[str, dict]:
        result = {}
        for o in orders:
            oid = str(o.get('AppOrderID') or o.get('app_order_id') or '')
            if not oid:
                continue
            result[oid] = { 'status': str(o.get('OrderStatus') or o.get('order_status') or '').upper(), 'avg_price': float(o.get('OrderAverageTradedPrice') or o.get('order_avg_price') or 0), 'qty': int(o.get('CumulativeQuantity') or o.get('cumulative_quantity') or 0), 'order': o, }
        return result

    def close(self):
        self._order_shm.close(unlink=True)
        self._publisher.close()
        self._executor.shutdown(wait=False)