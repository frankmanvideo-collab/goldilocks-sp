"""MEXC futures via `sub.depth.full` (limit 5): a full top-5 snapshot per push, so no book maintenance needed."""
from __future__ import annotations

from typing import Any

import orjson

from ..state import VenueSpec
from .base import BaseFeed


class MexcFeed(BaseFeed):
    name = "mexc"
    ping_interval_s = 15.0  # server drops sockets with no ping inside 60s

    async def subscribe(self, ws: Any, specs: list[VenueSpec]) -> None:
        for s in specs:
            await self.send(ws, {"method": "sub.depth.full", "param": {"symbol": s.symbol, "limit": 5}})

    async def unsubscribe(self, ws: Any, specs: list[VenueSpec]) -> None:
        for s in specs:
            # The docs spell the method both ways ("unsub." / "usub."); an unknown one just yields rs.error.
            await self.send(ws, {"method": "unsub.depth.full", "param": {"symbol": s.symbol, "limit": 5}})
            await self.send(ws, {"method": "usub.depth.full", "param": {"symbol": s.symbol, "limit": 5}})

    def ping_frame(self) -> dict[str, Any]:
        return {"method": "ping"}

    def on_message(self, raw: str | bytes) -> None:
        msg = orjson.loads(raw)
        if msg.get("channel") != "push.depth.full":
            return
        spec = self.sym_map.get(msg.get("symbol", ""))
        if spec is None:
            return
        d = msg["data"]
        bids, asks = d.get("bids"), d.get("asks")
        if not bids or not asks:
            self.hub.on_book(spec.coin, self.name, 0.0, 0.0, 0.0, 0.0)
            return
        # Levels are [price, contracts, orders]; pick best defensively (<= 5 levels, trivial cost).
        bb = bids[0]
        for lvl in bids:
            if lvl[0] > bb[0]:
                bb = lvl
        ba = asks[0]
        for lvl in asks:
            if lvl[0] < ba[0]:
                ba = lvl
        cs, m = spec.contract_size, spec.mult
        bpx, apx = float(bb[0]), float(ba[0])
        self.hub.on_book(spec.coin, self.name, bpx / m, bpx * float(bb[1]) * cs, apx / m, apx * float(ba[1]) * cs)
