import time

import pytest

import utils  # noqa: F401  (import first: models.state <-> utils.helpers cycle)
from models.state import state
from trading import depth_aware_fast_mtm as mtm

ASKS = [
    {"price": 10.0, "qty": 50},
    {"price": 10.5, "qty": 50},
    {"price": 11.0, "qty": 50},
    {"price": 11.5, "qty": 50},
    {"price": 12.0, "qty": 50},
]
BIDS = [
    {"price": 9.5, "qty": 100},
    {"price": 9.0, "qty": 100},
]


@pytest.fixture(autouse=True)
def clean_state():
    state.shared_data = None
    state.clear_market_depth()
    state.quotes = {}
    yield
    state.clear_market_depth()
    state.quotes = {}


def test_walk_within_first_level():
    r = mtm.walk_depth(ASKS, 30)
    assert r["price"] == 10.0
    assert r["levels_used"] == 1
    assert r["depth_filled_qty"] == 30


def test_walk_across_levels_vwap():
    r = mtm.walk_depth(ASKS, 125)
    assert r["price"] == pytest.approx((50 * 10 + 50 * 10.5 + 25 * 11) / 125)
    assert r["levels_used"] == 3


def test_walk_beyond_depth_prices_remainder_at_worst_level():
    r = mtm.walk_depth(ASKS, 300)
    visible = sum(l["price"] * l["qty"] for l in ASKS)
    assert r["price"] == pytest.approx((visible + 50 * 12.0) / 300)
    assert r["depth_filled_qty"] == 250
    assert r["levels_used"] == 5


def test_walk_skips_empty_levels_and_accepts_raw_xts_keys():
    levels = [{"Price": 0, "Size": 0}, {"Price": 10, "Size": 10}]
    assert mtm.walk_depth(levels, 5)["price"] == 10.0
    assert mtm.walk_depth([], 5) is None


def test_short_walks_asks_long_walks_bids():
    depth = {"asks": ASKS, "bids": BIDS}
    assert mtm.exit_price(1, 100, depth=depth)["price"] == pytest.approx(10.25)
    long_ = mtm.exit_price(1, -150, depth=depth)
    assert long_["price"] == pytest.approx((100 * 9.5 + 50 * 9.0) / 150)
    assert long_["source"] == "depth5"


def test_fallback_to_top_of_book():
    r = mtm.exit_price(1, 100, depth={"ask": 10.2, "bid": 9.8})
    assert (r["price"], r["source"]) == (10.2, "top")


def test_fallback_to_ltp_then_none():
    assert mtm.exit_price(1, 100, depth={}, ltp=10.1)["source"] == "ltp"
    r = mtm.exit_price(1, 100)
    assert (r["price"], r["source"]) == (0.0, "none")
    assert mtm.exit_price(1, 0, ltp=10)["source"] == "none"


def test_reads_fresh_depth_from_state():
    state.set_market_depth(42, {"asks": ASKS, "bids": BIDS}, ts=time.time())
    assert mtm.exit_price(42, 50)["source"] == "depth5"


def test_stale_state_depth_falls_back_to_ltp():
    state.set_market_depth(42, {"asks": ASKS}, ts=time.time() - 60)
    r = mtm.exit_price(42, 50, ltp=9.9)
    assert (r["price"], r["source"]) == (9.9, "ltp")


def test_reads_quote_cache_when_no_depth_cache():
    state.quotes = {7: {"bids": BIDS, "_ts": time.time()}}
    assert mtm.exit_price(7, -10)["price"] == 9.5
    state.quotes = {7: {"bids": BIDS, "quote_ts": "2026-09-28T10:00:00+05:30"}}
    assert mtm.exit_price(7, -10, ltp=9.4)["source"] == "ltp"


def test_exit_mtm_sign_convention():
    assert mtm.exit_mtm(12.0, 100, 10.0) == 200.0   # short, price fell
    assert mtm.exit_mtm(12.0, -100, 10.0) == -200.0  # long, price fell
    assert mtm.exit_mtm(12.0, 100, 0.0) == 0.0


def test_extract_depth_levels_from_xts_quote():
    from trading.chain_provider import _extract_depth_from_quote

    quote = {
        "Touchline": {"LastTradedPrice": 10.0},
        "Bids": [{"Price": 9.9 - i * 0.1, "Size": 50, "TotalOrders": 2} for i in range(5)],
        "Asks": [{"Price": 10.1 + i * 0.1, "Size": 50, "TotalOrders": 3} for i in range(5)]
        + [{"Price": 0, "Size": 0, "TotalOrders": 0}],
    }
    d = _extract_depth_from_quote(quote)
    assert len(d["bids"]) == 5 and len(d["asks"]) == 5
    assert d["asks"][0] == {"price": 10.1, "qty": 50, "orders": 3}
    assert d["bid"] == 9.9
