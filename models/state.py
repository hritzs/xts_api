"""
Global State Management

This module provides a singleton `state` object to manage shared application state
across different parts of the main process and to provide a consistent interface
for state access in worker processes.
"""

import os
import time
from copy import deepcopy
from datetime import datetime, timezone
from typing import Dict, Optional, Any, List
from utils.logger import logger


class SharedDataManager:
    pass


class DashboardState:
    """
    Global state object for the application.
    This is a singleton-style shared runtime cache/state holder.

    Design intent:
    - prices = canonical runtime LTP cache
    - market_depth = canonical runtime depth cache
    - option_chains = mutable WORKING chain cache for builders/providers only
    - published_option_chains = immutable published snapshot cache for API/UI/WS/consumers
    """

    def __init__(self):
        self.db = None
        self.xt_i = None

        # Connectivity / source metadata
        self.socket_connected: bool = False
        self.data_source: str = "UNKNOWN"

        # Subscriptions / token metadata
        self.subscribed_tokens: set[int] = set()
        self.token_segment_map: Dict[int, int] = {}

        # Verification / task runtime caches
        self.verification_results: Dict[str, Any] = {}
        self.verification_tasks: Dict[str, Any] = {}
        self.cancellation_flags: Dict[str, Any] = {}
        self.trade_snapshots: Dict[str, Any] = {}
        self.trade_data_cache: Dict[str, Any] = {}
        self.closing_trades: set[str] = set()

        # Explicit order-trade caches
        self.trade_fill_cache: Dict[str, List[Dict[str, Any]]] = {}
        self.order_to_trade_map: Dict[str, str] = {}

        # Multiprocessing / shared manager integration
        self.shared_data: Optional["SharedDataManager"] = None
        self.trade_processes: Dict[str, Any] = {}

        # Canonical runtime prices
        self.prices: Dict[int, float] = {}
        self.price_timestamps: Dict[int, float] = {}

        # Canonical runtime market depth
        self.market_depth: Dict[int, Dict[str, Any]] = {}
        self.depth_timestamps: Dict[int, float] = {}

        # Mutable working cache used only during chain build/update
        self.option_chains: Dict[str, Dict[str, Any]] = {}

        # Immutable published cache used by API + WS + consumers
        self.published_option_chains: Dict[str, Dict[str, Any]] = {}

        # Monotonic sequence number per symbol for stale-checking
        self.chain_publish_seq: Dict[str, int] = {}

        # --- NEW: Condition Task Engine State ---
        # Stores BuildContext for trades waiting for an entry condition
        self.pending_builds: Dict[str, Any] = {}
        # Central registry for all scheduled, periodic checks
        self.condition_tasks: Dict[str, Any] = {}

        # --- NEW: For entry-at-straddle monitoring ---
        self.pending_entry_builds: Dict[str, Any] = {}

        # Optional runtime objects used by older codepaths
        self.broadcast_queue = None

    # ------------------------------------------------------------------
    # Price cache
    # ------------------------------------------------------------------
    def get_price(self, token: int) -> Optional[float]:
        if token is None:
            return None

        try:
            token = int(token)
        except (ValueError, TypeError):
            return None

        if self.shared_data and hasattr(self.shared_data, "get_price"):
            try:
                return self.shared_data.get_price(token)
            except Exception as e:
                logger.warning(f"SharedDataManager.get_price failed for token {token}: {e}")

        if isinstance(self.prices, dict):
            return self.prices.get(token)

        logger.warning(
            f"Could not get price for token {token}: "
            "SharedDataManager not available and self.prices is not a dict."
        )
        return None

    def get_price_timestamp(self, token: int) -> Optional[float]:
        if token is None:
            return None

        try:
            token = int(token)
        except (ValueError, TypeError):
            return None

        if isinstance(self.price_timestamps, dict):
            return self.price_timestamps.get(token)

        return None

    def update_price(self, token: int, price: float, ts: Optional[float] = None):
        if token is None:
            return

        try:
            token = int(token)
            price = float(price)
            ts = float(ts) if ts is not None else time.time()
        except (ValueError, TypeError):
            logger.warning(f"Invalid price update ignored for token={token}, price={price}, ts={ts}")
            return

        if self.shared_data and hasattr(self.shared_data, "update_price"):
            try:
                try:
                    self.shared_data.update_price(token, price, ts)
                except TypeError:
                    self.shared_data.update_price(token, price)
                    if hasattr(self.shared_data, "price_timestamps"):
                        self.shared_data.price_timestamps[token] = ts
                return
            except Exception as e:
                pass  # Expected behavior in worker process

        if not isinstance(self.prices, dict):
            self.prices = {}
        if not isinstance(self.price_timestamps, dict):
            self.price_timestamps = {}

        self.prices[token] = price
        self.price_timestamps[token] = ts

    def bulk_update_prices(self, price_map: Dict[int, float], ts: Optional[float] = None):
        if not price_map:
            return

        update_ts = float(ts) if ts is not None else time.time()
        for token, price in price_map.items():
            self.update_price(token, price, update_ts)

    def clear_prices(self):
        self.prices = {}
        self.price_timestamps = {}

    def is_price_stale(self, token: int, max_age: float = 1.0) -> bool:
        ts = self.get_price_timestamp(token)
        if not ts:
            return True
        return (time.time() - ts) > float(max_age)

    # ------------------------------------------------------------------
    # Market depth cache
    # ------------------------------------------------------------------
    def get_market_depth(self, token: int) -> Optional[Dict[str, Any]]:
        if token is None:
            return None

        try:
            token = int(token)
        except (ValueError, TypeError):
            return None

        if self.shared_data and hasattr(self.shared_data, "get_market_depth"):
            try:
                snap = self.shared_data.get_market_depth(token)
                return deepcopy(snap) if snap else None
            except Exception as e:
                logger.warning(f"SharedDataManager.get_market_depth failed for token {token}: {e}")

        if isinstance(self.market_depth, dict):
            snap = self.market_depth.get(token)
            return deepcopy(snap) if snap else None

        logger.warning(
            f"Could not get market depth for token {token}: "
            "SharedDataManager not available and self.market_depth is not a dict."
        )
        return None

    def get_depth_timestamp(self, token: int) -> Optional[float]:
        if token is None:
            return None

        try:
            token = int(token)
        except (ValueError, TypeError):
            return None

        if isinstance(self.depth_timestamps, dict):
            return self.depth_timestamps.get(token)

        return None

    def set_market_depth(self, token: int, depth: Dict[str, Any], ts: Optional[float] = None):
        if token is None or not isinstance(depth, dict):
            return

        try:
            token = int(token)
            update_ts = float(ts) if ts is not None else float(
                depth.get("_ts") or depth.get("ts") or time.time()
            )
        except (ValueError, TypeError):
            logger.warning(f"Invalid market depth update ignored for token={token}, depth={depth}")
            return

        depth_copy = deepcopy(depth)
        depth_copy["_ts"] = update_ts
        depth_copy["ts"] = update_ts

        if self.shared_data and hasattr(self.shared_data, "update_market_depth"):
            try:
                self.shared_data.update_market_depth(token, depth_copy, update_ts)
                return
            except TypeError:
                try:
                    self.shared_data.update_market_depth(token, depth_copy)
                    return
                except Exception as e:
                    logger.warning(f"SharedDataManager.update_market_depth failed for token {token}: {e}")
            except Exception as e:
                logger.warning(f"SharedDataManager.update_market_depth failed for token {token}: {e}")

        if not isinstance(self.market_depth, dict):
            self.market_depth = {}
        if not isinstance(self.depth_timestamps, dict):
            self.depth_timestamps = {}

        self.market_depth[token] = depth_copy
        self.depth_timestamps[token] = update_ts

    def update_market_depth(self, token: int, depth: Dict[str, Any], ts: Optional[float] = None):
        self.set_market_depth(token, depth, ts)

    def bulk_update_market_depth(self, depth_map: Dict[int, Dict[str, Any]], ts: Optional[float] = None):
        if not depth_map:
            return

        update_ts = float(ts) if ts is not None else time.time()
        for token, depth in depth_map.items():
            self.set_market_depth(token, depth, update_ts)

    def clear_market_depth(self):
        self.market_depth = {}
        self.depth_timestamps = {}

    def is_market_depth_stale(self, token: int, max_age: float = 0.5) -> bool:
        ts = self.get_depth_timestamp(token)
        if not ts:
            return True
        return (time.time() - ts) > float(max_age)

    # ------------------------------------------------------------------
    # WORKING option chain cache (builder/provider only)
    # ------------------------------------------------------------------
    def get_working_option_chain(self, symbol: str) -> Optional[Dict]:
        if not symbol:
            return None

        symbol = symbol.upper()

        if self.shared_data and hasattr(self.shared_data, "get_option_chain"):
            try:
                return self.shared_data.get_option_chain(symbol)
            except Exception as e:
                logger.warning(f"SharedDataManager.get_option_chain failed for {symbol}: {e}")

        if not isinstance(self.option_chains, dict):
            self.option_chains = {}

        return self.option_chains.get(symbol)

    def update_working_option_chain(self, symbol: str, chain_data: Dict):
        if not symbol or not isinstance(chain_data, dict):
            return

        symbol = symbol.upper()

        if self.shared_data and hasattr(self.shared_data, "update_option_chain"):
            try:
                self.shared_data.update_option_chain(symbol, chain_data)
                return
            except Exception as e:
                logger.warning(f"SharedDataManager.update_option_chain failed for {symbol}: {e}")

        if not isinstance(self.option_chains, dict):
            self.option_chains = {}

        self.option_chains[symbol] = chain_data

    def clear_working_option_chain(self, symbol: str):
        if not symbol:
            return
        symbol = symbol.upper()
        if isinstance(self.option_chains, dict):
            self.option_chains.pop(symbol, None)

    def clear_all_working_option_chains(self):
        self.option_chains = {}

    def is_working_option_chain_stale(self, symbol: str, max_age: int = 5) -> bool:
        if not symbol:
            return True

        symbol = symbol.upper()

        if not isinstance(self.option_chains, dict):
            return True

        chain_data = self.option_chains.get(symbol)
        if not chain_data:
            return True

        last_update_timestamp = (
            chain_data.get("timestamp")
            or chain_data.get("published_at_epoch")
            or chain_data.get("built_at_epoch")
        )
        if not last_update_timestamp:
            logger.warning(f"Working chain for {symbol} is missing a timestamp. Assuming stale.")
            return True

        try:
            age = time.time() - float(last_update_timestamp)
        except (ValueError, TypeError):
            logger.warning(f"Working chain for {symbol} has invalid timestamp. Assuming stale.")
            return True

        is_stale = age > max_age
        if is_stale:
            logger.info(f"Working chain for {symbol} is stale (age: {age:.2f}s > {max_age}s).")
        return is_stale

    # ------------------------------------------------------------------
    # PUBLISHED option chain cache (single source of truth)
    # ------------------------------------------------------------------
    def get_published_option_chain(self, symbol: str) -> Optional[Dict]:
        if not symbol:
            return None

        symbol = symbol.upper()

        # 1. Try shared manager proxy if available
        if self.shared_data and hasattr(self.shared_data, "option_chains_proxy") and self.shared_data.option_chains_proxy:
            try:
                proxy_chain = self.shared_data.option_chains_proxy.get(symbol)
                if proxy_chain:
                    return deepcopy(proxy_chain)
            except Exception:
                pass

        # 2. Always try fresh ChainSHM first to prevent stale cache
        try:
            from core.shared_memory import ChainSHM
            shm_reader = ChainSHM(symbol, create=False)
            shm_chain = shm_reader.read()
            if shm_chain and isinstance(shm_chain, dict) and "chain" in shm_chain:
                self.published_option_chains[symbol] = deepcopy(shm_chain)
                return shm_chain
        except Exception:
            pass

        # 3. Fallback to local published cache
        snap = self.published_option_chains.get(symbol)
        if snap:
            return deepcopy(snap)

        # 4. Cross-Process Fallback: Query via trading data client / market data service
        try:
            import importlib
            market_data_client = None
            try:
                mdc_mod = importlib.import_module("trading.market_data_client")
                market_data_client = getattr(mdc_mod, "market_data_client", None)
            except (ImportError, AttributeError):
                pass

            if market_data_client and hasattr(market_data_client, "get_option_chain"):
                chain = market_data_client.get_option_chain(symbol)
                if chain and isinstance(chain, dict) and "chain" in chain:
                    self.published_option_chains[symbol] = deepcopy(chain)
                    return chain
        except Exception:
            pass

        return None

    def publish_option_chain(self, symbol: str, snapshot: Dict) -> Dict:
        if not symbol or not isinstance(snapshot, dict):
            raise ValueError("publish_option_chain requires non-empty symbol and dict snapshot")

        symbol = symbol.upper()
        seq = self.chain_publish_seq.get(symbol, 0) + 1
        self.chain_publish_seq[symbol] = seq

        published = deepcopy(snapshot)
        published["symbol"] = symbol
        published["publish_seq"] = seq

        if not published.get("published_at"):
            published["published_at"] = datetime.now(timezone.utc).isoformat()
        if not published.get("published_at_epoch"):
            published["published_at_epoch"] = time.time()

        self.published_option_chains[symbol] = published

        if self.shared_data and hasattr(self.shared_data, "publish_option_chain"):
            try:
                self.shared_data.publish_option_chain(symbol, published)
            except Exception as e:
                logger.warning(f"SharedDataManager.publish_option_chain failed for {symbol}: {e}")

        # [CHAIN SHM COMMIT] Physically commit chain snapshot to cross-process shared memory
        try:
            from core.shared_memory import ChainSHM
            shm_writer = ChainSHM(symbol, create=True)
            shm_writer.write(published)
            logger.info(f"[CHAIN SHM WRITE SUCCESS] Symbol={symbol} | Seq={seq} | Rows={len(published.get('chain', []))}")
        except Exception as shm_err:
            logger.warning(f"[CHAIN SHM WRITE FAILED] {symbol}: {shm_err}")

        return deepcopy(published)
    def clear_published_option_chain(self, symbol: str):
        if not symbol:
            return
        symbol = symbol.upper()
        self.published_option_chains.pop(symbol, None)
        self.chain_publish_seq.pop(symbol, None)

        if self.shared_data and hasattr(self.shared_data, "clear_published_option_chain"):
            try:
                self.shared_data.clear_published_option_chain(symbol)
            except Exception as e:
                logger.warning(f"SharedDataManager.clear_published_option_chain failed for {symbol}: {e}")

    def clear_all_published_option_chains(self):
        self.published_option_chains = {}
        self.chain_publish_seq = {}

    # ------------------------------------------------------------------
    # Subscriptions / mappings
    # ------------------------------------------------------------------
    def add_subscription(self, token: int):
        try:
            self.subscribed_tokens.add(int(token))
        except (ValueError, TypeError):
            logger.warning(f"Invalid subscription token ignored: {token}")

    def remove_subscription(self, token: int):
        try:
            self.subscribed_tokens.discard(int(token))
        except (ValueError, TypeError):
            logger.warning(f"Invalid subscription token ignored: {token}")

    def set_token_segment(self, token: int, segment: int):
        try:
            self.token_segment_map[int(token)] = int(segment)
        except (ValueError, TypeError):
            logger.warning(f"Invalid token-segment mapping ignored: token={token}, segment={segment}")

    def get_token_segment(self, token: int) -> Optional[int]:
        try:
            return self.token_segment_map.get(int(token))
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # Order / trade helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_order_id(order_or_fill: Dict[str, Any]) -> str:
        if not isinstance(order_or_fill, dict):
            return ""
        return str(
            order_or_fill.get("AppOrderID")
            or order_or_fill.get("app_order_id")
            or order_or_fill.get("apporderid")
            or order_or_fill.get("order_id")
            or ""
        ).strip()

    def seed_trade_fills(self, trade_uid: str, fills: List[Dict[str, Any]]):
        if not trade_uid:
            return

        merged: Dict[str, Dict[str, Any]] = {}
        no_id_items: List[Dict[str, Any]] = []

        for fill in fills or []:
            oid = self._extract_order_id(fill)
            if oid:
                merged[oid] = fill
            else:
                no_id_items.append(fill)

        self.trade_fill_cache[str(trade_uid)] = list(merged.values()) + no_id_items

    def get_trade_fills(self, trade_uid: str) -> List[Dict[str, Any]]:
        if not trade_uid:
            return []
        return list(self.trade_fill_cache.get(str(trade_uid), []))

    def append_trade_fill(self, trade_uid: str, fill: Dict[str, Any]):
        if not trade_uid or not isinstance(fill, dict):
            return

        key = str(trade_uid)
        curr = self.trade_fill_cache.get(key, [])
        oid = self._extract_order_id(fill)

        if not oid:
            curr.append(fill)
            self.trade_fill_cache[key] = curr
            return

        by_oid: Dict[str, Dict[str, Any]] = {}
        no_id_items: List[Dict[str, Any]] = []

        for existing in curr:
            existing_oid = self._extract_order_id(existing)
            if existing_oid:
                by_oid[existing_oid] = existing
            else:
                no_id_items.append(existing)

        by_oid[oid] = fill
        self.trade_fill_cache[key] = list(by_oid.values()) + no_id_items

    def clear_trade_fills(self, trade_uid: str):
        if not trade_uid:
            return
        self.trade_fill_cache.pop(str(trade_uid), None)

    def map_order_to_trade(self, order_id: str, trade_uid: str):
        if not order_id or not trade_uid:
            return
        self.order_to_trade_map[str(order_id)] = str(trade_uid)

    def get_trade_for_order(self, order_id: str) -> Optional[str]:
        if not order_id:
            return None
        return self.order_to_trade_map.get(str(order_id))

    def clear_order_to_trade(self, order_id: str):
        if not order_id:
            return
        self.order_to_trade_map.pop(str(order_id), None)

    # ------------------------------------------------------------------
    # Backward-compatible aliases for older codepaths
    # ------------------------------------------------------------------
    def getprice(self, token: int) -> Optional[float]:
        return self.get_price(token)

    def updateprice(self, token: int, price: float):
        self.update_price(token, price)

    def get_option_chain(self, symbol: str) -> Optional[Dict]:
        return self.get_working_option_chain(symbol)

    def update_option_chain(self, symbol: str, chain_data: Dict):
        self.update_working_option_chain(symbol, chain_data)

    def clear_option_chain(self, symbol: str):
        self.clear_working_option_chain(symbol)

    def clear_all_option_chains(self):
        self.clear_all_working_option_chains()

    def is_option_chain_stale(self, symbol: str, max_age: int = 5) -> bool:
        return self.is_working_option_chain_stale(symbol, max_age=max_age)

    def getoptionchain(self, symbol: str) -> Optional[Dict]:
        return self.get_working_option_chain(symbol)

    def updateoptionchain(self, symbol: str, chain_data: Dict):
        self.update_working_option_chain(symbol, chain_data)

    def isoptionchainstale(self, symbol: str, max_age: int = 5) -> bool:
        return self.is_working_option_chain_stale(symbol, max_age=max_age)

    def addsubscription(self, token: int):
        self.add_subscription(token)

    @property
    def socketconnected(self) -> bool:
        return self.socket_connected

    @socketconnected.setter
    def socketconnected(self, value: bool):
        self.socket_connected = bool(value)

    @property
    def optionchains(self) -> Dict[str, Dict[str, Any]]:
        return self.option_chains

    @optionchains.setter
    def optionchains(self, value):
        self.option_chains = value if isinstance(value, dict) else {}

    @property
    def temp_order_cache(self) -> Dict[str, Any]:
        logger.warning(
            "Deprecated temp_order_cache access detected. "
            "Use trade_fill_cache or order_to_trade_map explicitly."
        )
        return {
            "trade_fill_cache": self.trade_fill_cache,
            "order_to_trade_map": self.order_to_trade_map,
        }

    @temp_order_cache.setter
    def temp_order_cache(self, value):
        if value == {}:
            self.trade_fill_cache = {}
            self.order_to_trade_map = {}
        elif isinstance(value, dict):
            logger.warning(
                "Deprecated temp_order_cache bulk assignment detected. "
                "This codepath should be migrated to trade_fill_cache / order_to_trade_map."
            )

    def _get_shared_trade_cache(self):
        shared_cache = getattr(self, "trade_data_cache", None)
        if shared_cache is None and getattr(self, "shared_data", None) is not None:
            shared_cache = getattr(self.shared_data, "trade_data_cache_proxy", None)
        return shared_cache

    def set_pending_entry_context(self, trade_uid: str, build_context: Dict[str, Any]):
        self.pending_entry_builds[trade_uid] = build_context
        try:
            shared_cache = self._get_shared_trade_cache()
            if shared_cache is not None and hasattr(shared_cache, "setdefault"):
                pending_map = shared_cache.setdefault("__pending_entry_builds__", {})
                if pending_map is not None and hasattr(pending_map, "__setitem__"):
                    pending_map[trade_uid] = build_context
        except Exception:
            pass

    def get_pending_entry_context(self, trade_uid: str) -> Optional[Dict[str, Any]]:
        context = self.pending_entry_builds.get(trade_uid)
        if context is not None:
            return context
        try:
            shared_cache = self._get_shared_trade_cache()
            if shared_cache is not None and hasattr(shared_cache, "get"):
                pending_map = shared_cache.get("__pending_entry_builds__", {})
                if pending_map is not None and hasattr(pending_map, "get"):
                    return pending_map.get(trade_uid)
        except Exception:
            return None
        return None

    def pop_pending_entry_context(self, trade_uid: str) -> Optional[Dict[str, Any]]:
        context = self.pending_entry_builds.pop(trade_uid, None)
        try:
            shared_cache = self._get_shared_trade_cache()
            if shared_cache is not None and hasattr(shared_cache, "get"):
                pending_map = shared_cache.get("__pending_entry_builds__", {})
                if pending_map is not None and hasattr(pending_map, "pop"):
                    pending_map.pop(trade_uid, None)
        except Exception:
            pass
        return context


state = DashboardState()
