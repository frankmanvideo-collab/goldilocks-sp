"""Shared WebSocket plumbing: reconnect with backoff, heartbeat, watchdog and diff-based re-subscription."""
from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import aiohttp
import orjson

from ..config import Settings
from ..state import Hub, Target, VenueSpec


class BaseFeed:
    name = "base"
    ping_interval_s = 20.0
    read_timeout_s = 60.0  # no frame (not even a pong) for this long => reconnect

    def __init__(self, cfg: Settings, hub: Hub, session: aiohttp.ClientSession, url: str) -> None:
        self.cfg = cfg
        self.hub = hub
        self.session = session
        self.url = url
        self.log = logging.getLogger(f"feed.{self.name}")
        self.desired: dict[str, VenueSpec] = {}      # coin -> spec the radar wants
        self.subscribed: dict[str, VenueSpec] = {}   # coin -> spec currently live on the socket
        self.sym_map: dict[str, VenueSpec] = {}      # exchange symbol -> spec (for message routing)
        self._dirty = asyncio.Event()

    # ------------------------------------------------------------------ radar -> feed
    def set_targets(self, targets: dict[str, Target]) -> None:
        self.desired = {c: t.venues[self.name] for c, t in targets.items() if self.name in t.venues}
        self._dirty.set()

    # ------------------------------------------------------------------ subclass hooks
    async def subscribe(self, ws: aiohttp.ClientWebSocketResponse, specs: list[VenueSpec]) -> None:
        raise NotImplementedError

    async def unsubscribe(self, ws: aiohttp.ClientWebSocketResponse, specs: list[VenueSpec]) -> None:
        raise NotImplementedError

    def ping_frame(self) -> dict[str, Any]:
        raise NotImplementedError

    def on_message(self, raw: str | bytes) -> None:
        raise NotImplementedError

    def reset_session_state(self) -> None:
        """Hook: drop per-connection caches."""

    def forget(self, spec: VenueSpec) -> None:
        """Hook: drop per-symbol caches after unsubscribe."""

    # ------------------------------------------------------------------ helpers
    @staticmethod
    async def send(ws: aiohttp.ClientWebSocketResponse, payload: dict[str, Any]) -> None:
        await ws.send_str(orjson.dumps(payload).decode())

    # ------------------------------------------------------------------ lifecycle
    async def run(self) -> None:
        backoff = 1.0
        while True:
            try:
                await self._session_once()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                self.log.warning("socket error: %s: %s", type(exc).__name__, exc)
            finally:
                self.hub.invalidate_exchange(self.name)
                self.subscribed.clear()
                self.sym_map.clear()
                self.reset_session_state()
            delay = backoff + random.random()
            self.log.info("reconnecting in %.1fs", delay)
            await asyncio.sleep(delay)
            backoff = min(backoff * 2, 30.0)

    async def _session_once(self) -> None:
        async with self.session.ws_connect(
            self.url, heartbeat=None, autoping=True, max_msg_size=2**20, compress=0
        ) as ws:
            self.log.info("connected %s", self.url)
            self.hub.exchange_up[self.name] = True
            self._dirty.set()
            helpers = [
                asyncio.create_task(self._sync_loop(ws), name=f"{self.name}-sync"),
                asyncio.create_task(self._ping_loop(ws), name=f"{self.name}-ping"),
            ]
            try:
                while True:
                    msg = await ws.receive(timeout=self.read_timeout_s)
                    if msg.type in (aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY):
                        try:
                            self.on_message(msg.data)
                        except Exception:  # noqa: BLE001 - one bad frame must never kill the stream
                            self.log.exception("bad frame")
                    elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                        self.log.info("server closed socket")
                        return
                    elif msg.type == aiohttp.WSMsgType.ERROR:
                        raise ConnectionError(f"ws error: {ws.exception()}")
                    for t in helpers:  # surface helper crashes (e.g. failed send) as a reconnect
                        if t.done() and t.exception():
                            raise t.exception()  # type: ignore[misc]
            finally:
                for t in helpers:
                    t.cancel()
                await asyncio.gather(*helpers, return_exceptions=True)

    async def _ping_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            await asyncio.sleep(self.ping_interval_s)
            await self.send(ws, self.ping_frame())

    async def _sync_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        """Re-subscribe only the delta every time the radar rotates the Top-N set."""
        while True:
            await self._dirty.wait()
            self._dirty.clear()
            want = dict(self.desired)
            drop = [s for c, s in self.subscribed.items() if c not in want or want[c].symbol != s.symbol]
            add = [s for c, s in want.items() if c not in self.subscribed or self.subscribed[c].symbol != s.symbol]
            if drop:
                await self.unsubscribe(ws, drop)
                for s in drop:
                    self.subscribed.pop(s.coin, None)
                    self.sym_map.pop(s.symbol, None)
                    self.forget(s)
            if add:
                for s in add:
                    self.sym_map[s.symbol] = s
                await self.subscribe(ws, add)
                for s in add:
                    self.subscribed[s.coin] = s
            if drop or add:
                self.log.info("subs +%d -%d => %d live", len(add), len(drop), len(self.subscribed))
