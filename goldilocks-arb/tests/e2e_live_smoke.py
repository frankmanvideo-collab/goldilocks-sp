"""Live smoke test (needs internet + DATABASE_URL). Not part of pytest.

Uses real Hyperliquid REST+WS and real Bybit WS. Bybit/MEXC *REST* are synthesised from the HL
snapshot so the whole pipeline (radar -> WS sniper -> gap lifecycle -> DB writer) can be exercised
from networks where those REST hosts are geo-blocked.

    MIN_PROFIT_PCT is dropped to ~0 so gaps open and close within seconds.
    python tests/e2e_live_smoke.py
"""
from __future__ import annotations

import asyncio
import dataclasses
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.update(
    MIN_PROFIT_PCT="0.0005", RADAR_INTERVAL_S="10", MIN_LIQUIDITY_USD="50", VOL_MAX_USD="1e12",
    VOL_MIN_USD="20000000", TOP_N="6", FUNDING_APR_THRESHOLD_PCT="1",
)

import app.exchanges_rest as er  # noqa: E402
import app.radar as radar  # noqa: E402
from app.config import settings  # noqa: E402
from app.runtime import Runtime  # noqa: E402

real_hl = er.fetch_hyperliquid
BYBIT_COINS = {"ETH", "SOL", "XRP", "DOGE", "ADA", "AVAX", "LINK", "SUI", "BNB", "LTC", "BCH", "AAVE", "NEAR", "ARB", "OP", "APT", "TON", "WLD", "UNI", "DOT"}


async def fake_bybit(session, cfg):
    hl = await real_hl(session, cfg)
    return {
        k: dataclasses.replace(v, symbol=k + "USDT", mult=1.0, mid=v.mid * 1.0004,
                               funding_apr_pct=(v.funding_apr_pct or 0) + 30, range_pct=3.0)
        for k, v in hl.items() if k in BYBIT_COINS
    }


async def fake_mexc(session, cfg):
    raise RuntimeError("MEXC REST disabled in smoke test")


er.FETCHERS["bybit"] = fake_bybit
er.FETCHERS["mexc"] = fake_mexc
radar.FETCHERS = er.FETCHERS
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


async def main() -> None:
    rt = Runtime(settings)
    await rt.start()
    for _ in range(7):
        await asyncio.sleep(10)
        h = rt.health()
        print("HEALTH", h["tracked_coins"], "ram", h["ram_mb"], "ticks", h["ticks"], "open", h["open_gaps"],
              "closed", h["gaps_closed_session"], "written", h["rows_written"])
    for r in rt.hub.live_snapshot()["rows"][:3]:
        print(r["coin"], r["gross_pct"], {k: (v and (round(v["bid"], 4), round(v["ask"], 4))) for k, v in r["venues"].items()})
    print(await rt.today())
    await rt.stop()


if __name__ == "__main__":
    asyncio.run(main())
