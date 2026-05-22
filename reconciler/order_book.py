"""
Order book poller — fetches XTS order book every 1s.
Runs inside order_reconciler.py process.
"""
import asyncio
import json
import sqlite3
import os
from typing import List, Dict, Optional
from utils.logger import logger
import config

_cached_xt = None   # ← cached XTSConnect, created once

def _load_token_from_db() -> Optional[Dict]:
    db_path = os.path.abspath("shared_tokens.db")
    try:
        conn   = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM tokens WHERE key = 'xts_interactive_token'")
        result = cursor.fetchone()
        conn.close()
        if result:
            return json.loads(result[0])
    except Exception as e:
        logger.error(f"❌ OrderBook: token load failed: {e}")
    return None


def _get_or_create_xt():
    """Return cached XTSConnect — create only once, reuse forever."""
    global _cached_xt
    if _cached_xt is not None:
        return _cached_xt
    token_data = _load_token_from_db()
    if not token_data:
        return None
    try:
        from Connect import XTSConnect
        import cred
        xt = XTSConnect(cred.API_KEY_I, cred.API_SECRET_I, "WEBAPI")
        xt._set_common_variables(
            token_data['token'],
            token_data.get('userID'),
            token_data.get('isInvestorClient')
        )
        xt.isInvestorClient = False
        _cached_xt = xt
        logger.info("✅ OrderBook: XTSConnect instance cached.")
        return _cached_xt
    except Exception as e:
        logger.error(f"❌ OrderBook: failed to create XTSConnect: {e}")
        return None


async def fetch_order_book_raw(executor) -> List[dict]:
    """Fetch full XTS order book using cached XTSConnect instance."""
    global _cached_xt

    xt = _get_or_create_xt()
    if not xt:
        logger.warning("OrderBook: no XTS instance yet — token may not be written to DB. Retrying next cycle.")
        return []

    def _fetch():
        global _cached_xt
        try:
            import cred
            client_id = getattr(cred, 'clientID', None)
            response  = xt.get_order_book(clientID=client_id)
            if response and response.get('type') == 'success':
                result = response.get('result', {})
                if isinstance(result, dict):
                    return result.get('orderList', []) or result.get('OrderList', [])   
                elif isinstance(result, list):
                    return result
            return []
        except Exception as e:
            logger.error(f"❌ OrderBook fetch error: {e}")
            _cached_xt = None   # reset so next call recreates on token expiry
            return None 

    try:
        loop   = asyncio.get_event_loop()
        orders = await asyncio.wait_for(
            loop.run_in_executor(executor, _fetch),
            timeout=5.0   # ← reduced from 10s
        )
        return orders or []
    except asyncio.TimeoutError:
        logger.warning("OrderBook: fetch timed out")
        return []
    except Exception as e:
        logger.error(f"❌ OrderBook fetch failed: {e}")
        return []
