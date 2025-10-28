from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from bybit_trading_bot.config.settings import Config


@dataclass
class StrategySignal:
    symbol: str
    side: str  # "Buy" or "Sell"
    confidence: float
    ref_price: float
    sl_price: float


class VolumeBreakoutStrategy:
    """EMA21 breakout in the direction of H1 trend, RVOL confirmation, entry on pullback."""

    def __init__(self, config: Config) -> None:
        self.config = config

    async def evaluate(
        self,
        symbol: str,
        ema21_5m: Optional[float],
        ema21_h1: Optional[float],
        ema50_h1: Optional[float],
        rvol_5m: Optional[float],
        last_close: Optional[float],
        pullback_ok: bool,
    ) -> Optional[StrategySignal]:
        if any(x is None for x in (ema21_5m, ema21_h1, ema50_h1, rvol_5m, last_close)):
            return None
        min_rvol = float(getattr(self.config, "vb_min_rvol", 1.3))
        conf_thr = float(getattr(self.config, "vb_confidence_threshold", 1.2))

        price = float(last_close)
        h1_up = float(ema21_h1) > float(ema50_h1)
        h1_down = float(ema21_h1) < float(ema50_h1)
        above = price > float(ema21_5m)
        below = price < float(ema21_5m)
        rvol_ok = float(rvol_5m) >= min_rvol

        # Entry rule: EMA21 breakout in H1 trend direction + RVOL; then pullback confirmation
        if h1_up and above and rvol_ok and pullback_ok:
            sl = float(ema21_5m) * (1 - float(getattr(self.config, "tight_sl_ratio", 0.004)))
            return StrategySignal(symbol=symbol, side="Buy", confidence=max(conf_thr, float(rvol_5m)), ref_price=price, sl_price=sl)
        if h1_down and below and rvol_ok and pullback_ok:
            sl = float(ema21_5m) * (1 + float(getattr(self.config, "tight_sl_ratio", 0.004)))
            return StrategySignal(symbol=symbol, side="Sell", confidence=max(conf_thr, float(rvol_5m)), ref_price=price, sl_price=sl)
        return None


