"""
Global State Management

This module provides a singleton `state` object to manage shared application state
across different parts of the main process and to provide a consistent interface
for state access in worker processes.
"""
import time
from typing import Dict, Optional, List, Any
from utils.logger import logger

# Forward declaration for type hinting
class SharedDataManager:
    pass

class DashboardState:
    """
    Global state object for the application.
    This is a singleton pattern.
    """
    def __init__(self):
        self.db = None
        self.xt_i = None
        self.socket_connected: bool = False
        self.data_source: str = "UNKNOWN"
        self.subscribed_tokens: set = set()
        self.token_segment_map: Dict[int, int] = {} # NEW: Map token to segment for fallback lookup
        self.verification_results: Dict = {}
        self.verification_tasks: Dict = {}
        self.cancellation_flags: Dict = {}
        self.trade_snapshots: Dict = {}
        self.temp_order_cache: Dict = {}
        self.trade_data_cache: Dict = {}
        self.closing_trades: set = set()

        # --- MULTIPROCESSING ---
        self.shared_data: Optional['SharedDataManager'] = None
        self.trade_processes: Dict = {}
        
        # These attributes will be proxies to the shared data manager.
        self.prices = None
        self.option_chains = None

    def get_price(self, token: int) -> Optional[float]:
        """
        Safely gets a price from the shared data manager.
        This is the single source of truth for price lookups.
        """
        if token is None:
            return None

        if self.shared_data:
            return self.shared_data.get_price(token)
        
        # Fallback for non-multiprocessing environments (e.g., tests) or worker processes
        if self.prices is None:
            self.prices = {}

        if isinstance(self.prices, dict):
            try:
                return self.prices.get(int(token))
            except (ValueError, TypeError):
                return None
            
        logger.warning(f"Could not get price for token {token}: SharedDataManager not available and self.prices is not a dict.")
        return None

    def update_price(self, token: int, price: float):
        """
        Safely updates a price in the shared data manager or local cache.
        """
        if token is None:
            return

        if self.shared_data:
            self.shared_data.update_price(token, price)
        else:
            if self.prices is None:
                self.prices = {}
            if isinstance(self.prices, dict):
                try:
                    self.prices[int(token)] = float(price)
                except (ValueError, TypeError):
                    pass

    def get_option_chain(self, symbol: str) -> Optional[Dict]:
        """
        Safely gets an option chain from the shared data manager.
        """
        if self.option_chains is not None:
            # The option_chains is a proxy dict, which supports .get()
            return self.option_chains.get(symbol.upper())
        
        # Fallback/Init for local cache
        self.option_chains = {}
        return self.option_chains.get(symbol.upper())

    def update_option_chain(self, symbol: str, chain_data: Dict):
        """
        Safely updates an option chain in the shared data manager.
        """
        if self.option_chains is not None:
            self.option_chains[symbol.upper()] = chain_data
        else:
            # Fallback/Init for local cache
            self.option_chains = {}
            self.option_chains[symbol.upper()] = chain_data

    def is_option_chain_stale(self, symbol: str, max_age: int = 15) -> bool:
        """
        Checks if the cached option chain for a symbol is older than max_age seconds.
        """
        if self.option_chains is None:
            return True # No chain data at all

        chain_data = self.option_chains.get(symbol.upper())
        if not chain_data:
            return True # No chain for this symbol

        last_update_timestamp = chain_data.get('timestamp')
        if not last_update_timestamp:
            logger.warning(f"Chain for {symbol.upper()} is missing a timestamp. Assuming stale.")
            return True # No timestamp, assume stale

        is_stale = (time.time() - last_update_timestamp) > max_age
        if is_stale:
            logger.info(f"Chain for {symbol.upper()} is stale (age: {time.time() - last_update_timestamp:.2f}s > {max_age}s).")
        return is_stale

    def add_subscription(self, token: int):
        self.subscribed_tokens.add(token)

    def map_order_to_trade(self, order_id: str, trade_uid: str):
        pass

# Create a single global instance
state = DashboardState()