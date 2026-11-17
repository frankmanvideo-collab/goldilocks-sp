"""Bybit linear perps via the `tickers.<symbol>` topic (bid1/ask1 + sizes, delta-merged, ~100ms).

`tickers` is used instead of `orderbook.1` (10ms) deliberately: ~10x fewer frames, which is what keeps
this inside a 0.1 vCPU budget while still being far faster than any human or REST-polling competitor.
"""
from __future__ import annotations

from typing import Any

import orjson

from ..state import VenueSpec
from .base import BaseFeed

CHUNK = 10  # Bybit caps args per subscribe request
_FIELDS = ("bid1Price", "bid1Size", "ask1Price", "ask1Size")


class BybitFeed(BaseFeed):
    name = "bybit"
    ping_interval_s = 20.0

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        # symbol -> [bid_px, bid_sz, ask_px, ask_sz]; lists are mutated in place by deltas
        self._raw: dict[str, list[float]] = {}

    async def _op(self, ws: Any, op: str, specs: list[VenueSpec]) -> None:
        for i in range(0, len(specs), CHUNK):
            args = [f"tickers.{s.symbol}" for s in specs[i : i + CHUNK]]
            await self.send(ws, {"op": op, "args": args})

    async def subscribe(self, ws: Any, specs: list[VenueSpec]) -> None:
        for s in specs:
            self._raw[s.symbol] = [0.0, 0.0, 0.0, 0.0]
        await self._op(ws, "subscribe", specs)

    async def unsubscribe(self, ws: Any, specs: list[VenueSpec]) -> None:
        await self._op(ws, "unsubscribe", specs)

    def forget(self, spec: VenueSpec) -> None:
        self._raw.pop(spec.symbol, None)

    def reset_session_state(self) -> None:
        self._raw.clear()

    def ping_frame(self) -> dict[str, Any]:
        return {"op": "ping"}

    def on_message(self, raw: str | bytes) -> None:
        msg = orjson.loads(raw)
        topic = msg.get("topic")
        if not topic or not topic.startswith("tickers."):
            return
        d = msg["data"]
        sym = d.get("symbol") or topic[8:]
        spec = self.sym_map.get(sym)
        st = self._raw.get(sym)
        if spec is None or st is None:
            return
        touched = False
        for i, key in enumerate(_FIELDS):
            v = d.get(key)
            if v is not None:  # deltas only carry changed fields
                st[i] = float(v) if v != "" else 0.0
                touched = True
        if not touched:
            return  # funding/volume-only delta -> nothing to evaluate
        m = spec.mult
        self.hub.on_book(spec.coin, self.name, st[0] / m, st[0] * st[1], st[2] / m, st[2] * st[3])
