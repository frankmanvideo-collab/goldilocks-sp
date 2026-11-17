"""Offline tests: gap engine, radar selection, feed parsers, URL normalisation. Run: pytest -q"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import time

from app.config import load_settings
from app.db import normalise_url
from app.exchanges_rest import VenueInfo, split_mult
from app.feeds import BybitFeed, HyperliquidFeed, MexcFeed
from app.radar import select_hot_coins
from app.state import Hub, Target, VenueSpec

CFG = load_settings()


class StubWriter:
    def __init__(self) -> None:
        self.gaps: list[dict] = []
        self.funding: list[dict] = []

    def put_gap(self, row: dict) -> None:
        self.gaps.append(row)

    def put_funding(self, row: dict) -> None:
        self.funding.append(row)


def make_hub(cfg=CFG):
    w = StubWriter()
    hub = Hub(cfg, w)  # type: ignore[arg-type]
    t = Target("FOO", 10, 5, 5, 1, 0, {"hyperliquid": 5e6, "bybit": 5e6},
               {"hyperliquid": VenueSpec("FOO", "FOO"), "bybit": VenueSpec("FOO", "FOOUSDT")})
    hub.set_targets([t])
    return hub, w


def test_gap_lifecycle_and_storage_guard():
    hub, w = make_hub()
    hub.on_book("FOO", "hyperliquid", 100.0, 5000, 100.01, 5000)
    hub.on_book("FOO", "bybit", 100.00, 5000, 100.02, 5000)
    assert not hub.open_gaps and not w.gaps            # no gap: nothing stored, ever

    for _ in range(1000):                              # 1000 ticks, still nothing persisted
        hub.on_book("FOO", "bybit", 100.00, 5000, 100.02, 5000)
    assert not w.gaps

    # Bybit bid jumps 0.2%: buy HL ask 100.01, sell Bybit bid 100.30 -> 0.29% > 0.10% fee
    hub.on_book("FOO", "bybit", 100.30, 5000, 100.32, 5000)
    assert ("FOO", "hyperliquid", "bybit") in hub.open_gaps
    time.sleep(0.02)
    hub.on_book("FOO", "bybit", 100.40, 5000, 100.42, 5000)   # peak widens
    hub.on_book("FOO", "bybit", 100.00, 5000, 100.02, 5000)   # closes
    assert not hub.open_gaps
    assert len(w.gaps) == 1
    g = w.gaps[0]
    assert g["buy_exchange"] == "hyperliquid" and g["sell_exchange"] == "bybit"
    assert g["duration_ms"] >= 20
    assert g["closed_at"] > g["opened_at"]
    assert g["peak_spread_pct"] > g["entry_spread_pct"] > 0.10
    assert g["notional_usd"] == 1000.0                  # capped by PAPER_NOTIONAL_CAP_USD
    expect = 1000.0 * (g["entry_spread_pct"] / 100 - CFG.min_profit)
    assert abs(g["paper_profit_usd"] - expect) < 1e-3


def test_liquidity_floor_blocks_gap():
    hub, w = make_hub()
    hub.on_book("FOO", "hyperliquid", 100.0, 5000, 100.01, 299.0)  # only $299 on the ask
    hub.on_book("FOO", "bybit", 100.30, 5000, 100.32, 5000)
    assert not hub.open_gaps


def test_below_fee_threshold_is_not_a_gap():
    hub, _ = make_hub()
    hub.on_book("FOO", "hyperliquid", 100.0, 5000, 100.00, 5000)
    hub.on_book("FOO", "bybit", 100.09, 5000, 100.10, 5000)       # 0.09% < 0.10%
    assert not hub.open_gaps


def test_feed_drop_closes_gap_and_rotation_cleans_memory():
    hub, w = make_hub()
    hub.on_book("FOO", "hyperliquid", 100.0, 5000, 100.01, 5000)
    hub.on_book("FOO", "bybit", 100.30, 5000, 100.32, 5000)
    assert hub.open_gaps
    hub.invalidate_exchange("bybit")
    assert not hub.open_gaps and len(w.gaps) == 1
    hub.set_targets([])                                            # rotate everything out
    assert hub.latest_ticks == {} and hub.targets == {}


def test_quote_objects_are_overwritten_in_place():
    hub, _ = make_hub()
    q = hub.latest_ticks["FOO"]["bybit"]
    hub.on_book("FOO", "bybit", 1.0, 1000, 1.01, 1000)
    hub.on_book("FOO", "bybit", 2.0, 1000, 2.01, 1000)
    assert hub.latest_ticks["FOO"]["bybit"] is q and q.bid == 2.0


def _vi(vol, mid, bid=0.0, ask=0.0, fund=None, rng=None, ch=1.0, sym="X"):
    return VenueInfo(sym, 1.0, 1.0, vol, bid, ask, mid, fund, ch, rng)


def test_goldilocks_filter_and_ranking():
    snaps = {
        "hyperliquid": {"BTC": _vi(2e9, 60000), "DEAD": _vi(5e4, 1.0), "AAA": _vi(10e6, 1.000), "BBB": _vi(10e6, 2.0, fund=-80.0)},
        "bybit": {"BTC": _vi(5e9, 60010), "DEAD": _vi(5e4, 1.0), "AAA": _vi(12e6, 1.004, 1.0039, 1.0041, rng=9.0), "BBB": _vi(9e6, 2.0, fund=10.0, rng=3.0)},
        "mexc": {"AAA": _vi(2e6, 1.001), "SCAM": _vi(5e6, 1.0), "ONLYMEXC": _vi(9e6, 1.0)},
    }
    snaps["bybit"]["SCAM"] = _vi(5e6, 55.0)   # same ticker, different asset -> rejected
    top, funding, matched, eligible = select_hot_coins(snaps, CFG)
    coins = [t.coin for t in top]
    assert "BTC" not in coins                 # giant
    assert "DEAD" not in coins                # illiquid
    assert "SCAM" not in coins                # price mismatch
    assert "ONLYMEXC" not in coins            # single venue
    assert coins[0] == "AAA"                  # widest cross-venue divergence + volatility
    assert any(f[0] == "BBB" and f[5] == 90.0 for f in funding)   # 10 - (-80) = 90% APR > 50%


def test_split_mult():
    assert split_mult("1000PEPE") == ("PEPE", 1000.0)
    assert split_mult("1INCH") == ("1INCH", 1.0)
    assert split_mult("1000000MOG") == ("MOG", 1_000_000.0)


class FakeHub:
    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.exchange_up: dict = {}

    def on_book(self, *a) -> None:
        self.calls.append(a)


def _feed(cls, hub):
    cfg = dataclasses.replace(CFG)
    return cls(cfg, hub, None, "wss://x")  # type: ignore[arg-type]


def test_bybit_snapshot_then_delta():
    hub = FakeHub()
    f = _feed(BybitFeed, hub)
    spec = VenueSpec("PEPE", "1000PEPEUSDT", mult=1000.0)
    f.sym_map[spec.symbol] = spec
    f._raw[spec.symbol] = [0.0] * 4
    snap = {"topic": "tickers.1000PEPEUSDT", "type": "snapshot",
            "data": {"symbol": "1000PEPEUSDT", "bid1Price": "0.01000", "bid1Size": "50000", "ask1Price": "0.01001", "ask1Size": "40000"}}
    f.on_message(json.dumps(snap))
    delta = {"topic": "tickers.1000PEPEUSDT", "type": "delta", "data": {"symbol": "1000PEPEUSDT", "bid1Price": "0.01002"}}
    f.on_message(json.dumps(delta))
    f.on_message(json.dumps({"topic": "tickers.1000PEPEUSDT", "type": "delta", "data": {"symbol": "1000PEPEUSDT", "fundingRate": "0.0001"}}))
    assert len(hub.calls) == 2                                   # funding-only delta ignored
    coin, ex, bid, bid_usd, ask, ask_usd = hub.calls[1]
    assert (coin, ex) == ("PEPE", "bybit")
    assert abs(bid - 0.00001002) < 1e-12 and abs(ask - 0.00001001) < 1e-12   # normalised per 1 PEPE
    assert abs(bid_usd - 0.01002 * 50000) < 1e-6


def test_mexc_depth_uses_contract_size():
    hub = FakeHub()
    f = _feed(MexcFeed, hub)
    spec = VenueSpec("FOO", "FOO_USDT", contract_size=10.0)
    f.sym_map[spec.symbol] = spec
    msg = {"channel": "push.depth.full", "symbol": "FOO_USDT",
           "data": {"asks": [[1.02, 30, 2], [1.01, 20, 1]], "bids": [[1.0, 40, 3], [0.99, 10, 1]], "version": 1}}
    f.on_message(json.dumps(msg))
    coin, ex, bid, bid_usd, ask, ask_usd = hub.calls[0]
    assert (bid, ask) == (1.0, 1.01)
    assert abs(bid_usd - 1.0 * 40 * 10) < 1e-9 and abs(ask_usd - 1.01 * 20 * 10) < 1e-9


def test_hyperliquid_bbo_and_l2():
    hub = FakeHub()
    f = _feed(HyperliquidFeed, hub)
    spec = VenueSpec("PEPE", "kPEPE", mult=1000.0)
    f.sym_map["kPEPE"] = spec
    f.on_message(json.dumps({"channel": "bbo", "data": {"coin": "kPEPE", "time": 1, "bbo": [{"px": "10.0", "sz": "100", "n": 1}, {"px": "10.1", "sz": "50", "n": 1}]}}))
    f.on_message(json.dumps({"channel": "bbo", "data": {"coin": "kPEPE", "time": 2, "bbo": [None, {"px": "10.1", "sz": "50", "n": 1}]}}))
    assert hub.calls[0][2] == 0.01 and hub.calls[0][3] == 1000.0
    assert hub.calls[1][2] == 0.0                                 # one-sided book -> invalidated


def test_url_normalisation_strips_libpq_only_params():
    u = normalise_url("postgres://postgres.abc:p%40ss@aws-0-eu-central-1.pooler.supabase.com:6543/postgres?sslmode=require&pgbouncer=true")
    assert u.drivername == "postgresql+asyncpg" and u.password == "p@ss" and not u.query
    assert u.port == 6543
