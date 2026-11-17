"""Hyperliquid: top-of-book via the `bbo` channel (or `l2Book` with HL_FEED=l2Book)."""
from __future__ import annotations

from typing import Any

import orjson

from ..state import VenueSpec
from .base import BaseFeed


class HyperliquidFeed(BaseFeed):
    name = "hyperliquid"
    ping_interval_s = 25.0  # HL drops sockets idle for 60s

    def __init__(self, *a: Any, **kw: Any) -> None:
        super().__init__(*a, **kw)
        self.channel = "l2Book" if self.cfg.hl_feed.lower() == "l2book" else "bbo"

    def _frame(self, method: str, spec: VenueSpec) -> dict[str, Any]:
        return {"method": method, "subscription": {"type": self.channel, "coin": spec.symbol}}

    async def subscribe(self, ws: Any, specs: list[VenueSpec]) -> None:
        for s in specs:
            await self.send(ws, self._frame("subscribe", s))

    async def unsubscribe(self, ws: Any, specs: list[VenueSpec]) -> None:
        for s in specs:
            await self.send(ws, self._frame("unsubscribe", s))

    def ping_frame(self) -> dict[str, Any]:
        return {"method": "ping"}

    def on_message(self, raw: str | bytes) -> None:
        msg = orjson.loads(raw)
        ch = msg.get("channel")
        if ch == "bbo":
            d = msg["data"]
            spec = self.sym_map.get(d["coin"])
            if spec is None:
                return
            bid, ask = d["bbo"]
            self._push(spec, bid, ask)
        elif ch == "l2Book":
            d = msg["data"]
            spec = self.sym_map.get(d["coin"])
            if spec is None:
                return
            bids, asks = d["levels"]
            self._push(spec, bids[0] if bids else None, asks[0] if asks else None)

    def _push(self, spec: VenueSpec, bid: dict | None, ask: dict | None) -> None:
        if not bid or not ask:
            self.hub.on_book(spec.coin, self.name, 0.0, 0.0, 0.0, 0.0)
            return
        bpx, bsz = float(bid["px"]), float(bid["sz"])
        apx, asz = float(ask["px"]), float(ask["sz"])
        m = spec.mult
        # px*sz is already USD notional (kPEPE sizes are in kPEPE units).
        self.hub.on_book(spec.coin, self.name, bpx / m, bpx * bsz, apx / m, apx * asz)
