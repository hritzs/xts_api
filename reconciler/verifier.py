"""
Per-trade parallel verifier.
Reads raw order book → matches order IDs → returns verified fills.
"""
import asyncio
from typing import List, Dict
from utils.logger import logger
from utils.helpers import get_ist_now


async def verify_all_trades(
    pending:    Dict[str, dict],   # batch_name → {order_ids, trade_uid, attempts}
    order_book: List[dict],
    db,
) -> Dict[str, dict]:
    """
    Verify all pending batches in parallel.
    Returns {batch_name: result_dict}
    """
    book_map: Dict[str, dict] = {
        str(o.get('AppOrderID') or o.get('app_order_id') or ''): o
        for o in order_book
        if o.get('AppOrderID') or o.get('app_order_id')
    }

    tasks = {
        batch_name: _verify_single(batch_name, batch, book_map, db)
        for batch_name, batch in pending.items()
    }

    results = await asyncio.gather(*tasks.values(), return_exceptions=True)
    return {
        name: res
        for name, res in zip(tasks.keys(), results)
        if isinstance(res, dict)
    }


async def _verify_single(
    batch_name: str,
    batch:      dict,
    book_map:   Dict[str, dict],
    db,
) -> dict:
    order_ids  = batch['order_ids']
    trade_uid  = batch['trade_uid']
    attempts   = batch['attempts']

    verified       = []
    failed         = []
    still_pending  = []

    for oid in order_ids:
        order = book_map.get(str(oid))
        if not order:
            still_pending.append(oid)
            continue

        status = str(
            order.get('OrderStatus') or order.get('order_status') or ''
        ).upper()

        if status in ['FILLED', 'COMPLETE', 'TRADED', 'EXECUTED']:
            verified.append({'order_id': oid, 'status': status, 'order': order})
            if db:
                try:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, db.update_order_status, oid, status, order
                    )
                except Exception as e:
                    logger.warning(f"DB update failed for {oid}: {e}")

        elif status in ['CANCELLED', 'REJECTED', 'CANCELED']:
            failed.append({ 'order_id': oid, 'status':   status, 'reason':   order.get('CancelRejectReason', 'Unknown'), })
        else:
            still_pending.append(oid)

    all_resolved = len(still_pending) == 0
    timed_out    = attempts >= 5

    if not (all_resolved or timed_out):
        return None   # not done yet

    if timed_out and still_pending:
        logger.warning(f"⚠ [{batch_name}] timed out — {len(still_pending)} unresolved")
        failed += [ {'order_id': oid, 'status': 'TIMEOUT', 'reason': 'Not found after 5 attempts'} for oid in still_pending ]

    logger.info(f"✅ [{batch_name}] {len(verified)}/{len(order_ids)} filled, {len(failed)} failed | trade={trade_uid}")
    return { 'batch_name': batch_name, 'trade_uid':  trade_uid, 'timestamp':  get_ist_now().isoformat(), 'verified':   verified, 'failed':     failed, 'total':      len(order_ids), 'status':     'complete', }