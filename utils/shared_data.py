"""
Manages shared data structures for multiprocessing.
- Prices are stored in a high-performance numpy array in shared memory.
- Option Chains are stored in a multiprocessing.Manager dictionary.
"""
import numpy as np
from multiprocessing import shared_memory, Lock, Manager
from utils.logger import logger

# Define a fixed size for the price and token arrays.
# This needs to be large enough to hold all potential instruments.
MAX_INSTRUMENTS = 50000

class SharedDataManager:
    def __init__(self, create=False):
        self.is_creator = create
        self.lock = Lock()
        self.token_map = {}
        self.next_index = 0
        self.manager = None

        if create:
            logger.info(f"Creating new shared memory for {MAX_INSTRUMENTS} instruments.")
            # Shared memory for the numpy array of prices (float64)
            try:
                self.shm_prices = shared_memory.SharedMemory(name='prices_shm', create=True, size=MAX_INSTRUMENTS * 8)
            except FileExistsError:
                self.shm_prices = shared_memory.SharedMemory(name='prices_shm', create=False)
            
            # Shared memory for the numpy array of tokens (int64)
            try:
                self.shm_tokens = shared_memory.SharedMemory(name='tokens_shm', create=True, size=MAX_INSTRUMENTS * 8)
            except FileExistsError:
                self.shm_tokens = shared_memory.SharedMemory(name='tokens_shm', create=False)

            self.prices_array = np.ndarray((MAX_INSTRUMENTS,), dtype=np.float64, buffer=self.shm_prices.buf)
            self.tokens_array = np.ndarray((MAX_INSTRUMENTS,), dtype=np.int64, buffer=self.shm_tokens.buf)
            self.prices_array[:] = 0.0  # Initialize prices to 0
            self.tokens_array[:] = 0    # Initialize tokens to 0

            # Create a managed dictionary for option chains
            self.manager = Manager()
            self.option_chains_proxy = self.manager.dict()
            self.trade_data_cache_proxy = self.manager.dict()

        else:
            # Child process attaches to existing shared memory
            self.shm_prices = shared_memory.SharedMemory(name='prices_shm', create=False)
            self.shm_tokens = shared_memory.SharedMemory(name='tokens_shm', create=False)
            self.prices_array = np.ndarray((MAX_INSTRUMENTS,), dtype=np.float64, buffer=self.shm_prices.buf)
            self.tokens_array = np.ndarray((MAX_INSTRUMENTS,), dtype=np.int64, buffer=self.shm_tokens.buf)
            self._build_local_token_map()

    def _build_local_token_map(self):
        """Build a local token-to-index map for fast lookups in child processes."""
        for i, token in enumerate(self.tokens_array):
            if token != 0 and token not in self.token_map:
                self.token_map[token] = i
        self.next_index = len(self.token_map)

    def get_index_for_token(self, token: int) -> int:
        """Get the array index for a token, creating it if it doesn't exist (only for creator process)."""
        if token in self.token_map:
            return self.token_map[token]
        
        if not self.is_creator:
            self._build_local_token_map()
            return self.token_map.get(token)

        with self.lock:
            if token in self.token_map:
                return self.token_map[token]
            
            if self.next_index >= MAX_INSTRUMENTS:
                raise MemoryError("Shared price manager is full. Increase MAX_INSTRUMENTS.")
            
            index = self.next_index
            self.tokens_array[index] = token
            self.token_map[token] = index
            self.next_index += 1
            return index

    def update_price(self, token: int, price: float):
        if not self.is_creator:
            raise PermissionError("Only the main process can update prices.")
        
        index = self.get_index_for_token(token)
        if index is not None:
            self.prices_array[index] = price

    def get_price(self, token: int) -> float:
        index = self.token_map.get(token)
        if index is not None:
            return self.prices_array[index]
        
        if not self.is_creator:
            self._build_local_token_map()
            index = self.token_map.get(token)
            if index is not None:
                return self.prices_array[index]
        return 0.0

    def close(self, unlink=False):
        self.shm_prices.close()
        self.shm_tokens.close()
        if self.is_creator and unlink:
            self.shm_prices.unlink()
            self.shm_tokens.unlink()
            if self.manager:
                self.manager.shutdown()