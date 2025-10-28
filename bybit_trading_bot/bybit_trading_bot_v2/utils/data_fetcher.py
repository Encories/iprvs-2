from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.handlers.futures_ws import FuturesWS


def _ema(series: List[float], period: int) -> List[float]:
    if period <= 1 or len(series) == 0:
        return series[:]
    k = 2.0 / (period + 1)
    out: List[float] = []
    ema_prev = series[0]
    for v in series:
        ema_prev = v * k + ema_prev * (1 - k)
        out.append(ema_prev)
    return out


@dataclass
class Candle:
    ts: float
    o: float
    h: float
    l: float
    c: float
    v: float
    closed: bool


class DataFetcher:
    """Consumes futures klines for 5m and H1, computes EMA and RVOL."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self.ws = FuturesWS(testnet=False, config=self.config)  # live only per requirements
        self._buffers_5m: Dict[str, List[Candle]] = {}
        self._buffers_h1: Dict[str, List[Candle]] = {}
        self._warmed: set[str] = set()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        symbols = await self.get_symbols()
        # Subscribe klines
        self.ws.subscribe_kline_5m(symbols, on_kline=self._on_kline_5m)
        self.ws.subscribe_kline(symbols, interval="60", on_kline=self._on_kline_h1)

    async def stop(self) -> None:
        try:
            self.ws.stop()
        except Exception:
            pass

    async def get_symbols(self) -> List[str]:
        # For now, reuse symbols from existing DB active set in config if available
        try:
            from bybit_trading_bot.utils.db_manager import DBManager
            db = DBManager(self.config.database_path)
            return [rec.futures_symbol for rec in db.get_active_symbols()]
        except Exception:
            return ["BTCUSDT", "ETHUSDT"]

    def _on_kline_5m(self, symbol: str, k: Dict[str, float | bool | None]) -> None:
        try:
            c = Candle(
                ts=float(k.get("start") or 0.0),
                o=float(k.get("open") or 0.0),
                h=float(k.get("high") or 0.0),
                l=float(k.get("low") or 0.0),
                c=float(k.get("close") or 0.0),
                v=float(k.get("volume") or 0.0),
                closed=bool(k.get("confirm") or False),
            )
        except Exception:
            return
        buf = self._buffers_5m.setdefault(symbol, [])
        if buf and abs(buf[-1].ts - c.ts) < 1.0:
            buf[-1] = c
        else:
            buf.append(c)
            if len(buf) > 600:
                del buf[:-600]

    def _on_kline_h1(self, symbol: str, k: Dict[str, float | bool | None]) -> None:
        try:
            c = Candle(
                ts=float(k.get("start") or 0.0),
                o=float(k.get("open") or 0.0),
                h=float(k.get("high") or 0.0),
                l=float(k.get("low") or 0.0),
                c=float(k.get("close") or 0.0),
                v=float(k.get("volume") or 0.0),
                closed=bool(k.get("confirm") or False),
            )
        except Exception:
            return
        buf = self._buffers_h1.setdefault(symbol, [])
        if buf and abs(buf[-1].ts - c.ts) < 1.0:
            buf[-1] = c
        else:
            buf.append(c)
            if len(buf) > 1000:
                del buf[:-1000]

    async def get_ema(self, symbol: str, timeframe: str, period: int) -> Optional[float]:
        buf = self._buffers_h1.get(symbol) if timeframe == "H1" else self._buffers_5m.get(symbol)
        if not buf or len(buf) < period:
            return None
        closes = [x.c for x in buf]
        return _ema(closes, period)[-1]

    async def get_rvol(self, symbol: str, lookback: int = 20) -> Optional[float]:
        buf = self._buffers_5m.get(symbol)
        if not buf or len(buf) < lookback + 1:
            return None
        vols = [x.v for x in buf[-(lookback + 1):]]
        avg = sum(vols[:-1]) / max(1, len(vols) - 1)
        if avg <= 0:
            return None
        return vols[-1] / avg

    async def latest_close(self, symbol: str, timeframe: str = "5m") -> Optional[float]:
        buf = self._buffers_h1.get(symbol) if timeframe == "H1" else self._buffers_5m.get(symbol)
        if not buf:
            return None
        return buf[-1].c

    async def is_min_volatility_met(self, symbol: str) -> bool:
        # Placeholder for realized volatility check; configurable threshold
        try:
            min_vol = float(getattr(self.config, "min_market_volatility", 0.0) or 0.0)
            if min_vol <= 0:
                return True
            buf = self._buffers_5m.get(symbol)
            if not buf or len(buf) < 20:
                return False
            rngs = [(x.h - x.l) / x.c if x.c > 0 else 0.0 for x in buf[-20:]]
            rv = (sum(rngs) / len(rngs)) * 100.0
            return rv >= min_vol
        except Exception:
            return True


