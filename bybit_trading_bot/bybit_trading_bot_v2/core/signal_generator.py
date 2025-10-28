from __future__ import annotations

from typing import Optional

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager

from ..utils.data_fetcher import DataFetcher
from ..strategies.volume_breakout import VolumeBreakoutStrategy, StrategySignal
from datetime import datetime
import json
import os


class SignalGenerator:
    def __init__(self, config: Config, data: DataFetcher, strategy: VolumeBreakoutStrategy, db: DBManager) -> None:
        self.config = config
        self.data = data
        self.strategy = strategy
        self.db = db
        self.logger = get_logger(self.__class__.__name__)

    async def generate(self, symbol: str) -> Optional[StrategySignal]:
        ema21_5m = await self.data.get_ema(symbol, timeframe="5m", period=int(getattr(self.config, "vb_ema_period", 21)))
        ema21_h1 = await self.data.get_ema(symbol, timeframe="H1", period=21)
        ema50_h1 = await self.data.get_ema(symbol, timeframe="H1", period=int(getattr(self.config, "vb_trend_ema_h1", 50)))
        rvol = await self.data.get_rvol(symbol, lookback=int(getattr(self.config, "rvol_period", 20)))
        last_close = await self.data.latest_close(symbol, timeframe="5m")
        # Pullback confirmation: price within X% of EMA21 AND current 5m volume > prev 5m volume AND price direction agrees
        pullback_ok = False
        try:
            x_pct = float(getattr(self.config, "vb_pullback_tolerance_pct", 0.15)) / 100.0
            if last_close is not None and ema21_5m is not None:
                delta_ok = (abs(last_close - ema21_5m) / max(1e-12, ema21_5m)) <= x_pct
            else:
                delta_ok = False
            # Approx volume confirmation using RVOL>min and treating last bar volume > average of prior bar
            # DataFetcher doesn't expose raw vols list; approximate with RVOL threshold boost
            vol_ok = (rvol or 0.0) >= max(float(getattr(self.config, "vb_min_rvol", 1.3)), 1.3)
            pullback_ok = bool(delta_ok and vol_ok)
        except Exception:
            pullback_ok = False
        sig = await self.strategy.evaluate(symbol, ema21_5m, ema21_h1, ema50_h1, rvol, last_close, pullback_ok)
        # v2 signal logging (JSONL lightweight) + DB logging
        try:
            if sig is not None:
                os.makedirs("bybit_trading_bot/storage", exist_ok=True)
                path = os.path.join("bybit_trading_bot", "storage", "scalp_signals.jsonl")
                rec = {
                    "ts": datetime.utcnow().isoformat(),
                    "symbol": symbol,
                    "side": sig.side,
                    "conf": float(getattr(sig, "confidence", 0.0)),
                    "price": float(last_close or 0.0),
                    "ema21_5m": float(ema21_5m or 0.0),
                    "ema21_h1": float(ema21_h1 or 0.0),
                    "ema50_h1": float(ema50_h1 or 0.0),
                    "rvol": float(rvol or 0.0),
                    "pullback_ok": bool(pullback_ok),
                }
                with open(path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(rec) + "\n")
                # DB: insert into signals (best-effort). Use symbol_id via spot_symbol (same mapping as futures)
                try:
                    sid = self.db.get_symbol_id(symbol)
                    if sid is not None:
                        action = f"v2_signal:{sig.side}"
                        self.db.insert_signal(sid, price_change=0.0, oi_change=0.0, action_taken=action)
                except Exception:
                    pass
        except Exception:
            pass
        return sig


