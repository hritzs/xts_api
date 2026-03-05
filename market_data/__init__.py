"""
Market Data Client Package

This package provides client functions to interact with the Market Data Microservice.
It abstracts the HTTP calls and provides simple async functions for the main application to use.
"""

# Expose key functions from the chain_provider (which is now a client)
from .chain_provider import (
    get_option_chain,
    get_spot_details,
    get_ltp,
    get_bulk_ltp,
    get_market_depth,
    get_bulk_market_depth,
    SYMBOL_CONFIG
)

__all__ = [
    "get_option_chain",
    "get_spot_details",
    "get_ltp",
    "get_market_depth",
    "get_bulk_market_depth",
    "get_bulk_ltp",
    "SYMBOL_CONFIG",
]