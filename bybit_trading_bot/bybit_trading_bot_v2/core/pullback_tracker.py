from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional
from enum import Enum


class PullbackState(Enum):
    WAITING_BREAKOUT = "waiting_breakout"
    BREAKOUT_CONFIRMED = "breakout_confirmed"
    WAITING_PULLBACK = "waiting_pullback"
    PULLBACK_READY = "pullback_ready"


@dataclass
class SymbolState:
    state: PullbackState = PullbackState.WAITING_BREAKOUT
    breakout_price: Optional[float] = None
    ema21_at_breakout: Optional[float] = None
    bars_since_breakout: int = 0
    max_bars_wait: int = 10


class PullbackTracker:
    def __init__(self, max_bars_wait: int = 10, pullback_tolerance_pct: float = 0.15) -> None:
        self.symbol_states: Dict[str, SymbolState] = {}
        self.max_bars_wait = int(max_bars_wait)
        self.pullback_tolerance = float(pullback_tolerance_pct) / 100.0

    def update_state(self, symbol: str, price: float, ema21: float, rvol: float, trend_direction: str, min_rvol: float = 1.3) -> bool:
        if symbol not in self.symbol_states:
            self.symbol_states[symbol] = SymbolState(max_bars_wait=self.max_bars_wait)
        state = self.symbol_states[symbol]

        if state.state == PullbackState.WAITING_BREAKOUT:
            if trend_direction == "bullish" and price > ema21 and rvol >= min_rvol:
                state.state = PullbackState.BREAKOUT_CONFIRMED
                state.breakout_price = price
                state.ema21_at_breakout = ema21
                state.bars_since_breakout = 0
            elif trend_direction == "bearish" and price < ema21 and rvol >= min_rvol:
                state.state = PullbackState.BREAKOUT_CONFIRMED
                state.breakout_price = price
                state.ema21_at_breakout = ema21
                state.bars_since_breakout = 0
        elif state.state == PullbackState.BREAKOUT_CONFIRMED:
            state.bars_since_breakout += 1
            is_near_ema = abs(price - ema21) / max(1e-12, ema21) <= self.pullback_tolerance
            if is_near_ema:
                state.state = PullbackState.WAITING_PULLBACK
            if state.bars_since_breakout > state.max_bars_wait:
                state.state = PullbackState.WAITING_BREAKOUT
        elif state.state == PullbackState.WAITING_PULLBACK:
            is_near_ema = abs(price - ema21) / max(1e-12, ema21) <= self.pullback_tolerance
            if is_near_ema and rvol >= min_rvol:
                if trend_direction == "bullish" and price > ema21:
                    state.state = PullbackState.PULLBACK_READY
                    return True
                elif trend_direction == "bearish" and price < ema21:
                    state.state = PullbackState.PULLBACK_READY
                    return True
            if abs(price - ema21) / max(1e-12, ema21) > self.pullback_tolerance * 3:
                state.state = PullbackState.WAITING_BREAKOUT
        elif state.state == PullbackState.PULLBACK_READY:
            state.state = PullbackState.WAITING_BREAKOUT
        return False

    def get_state_info(self, symbol: str) -> Dict:
        if symbol not in self.symbol_states:
            return {"state": "unknown"}
        state = self.symbol_states[symbol]
        return {
            "state": state.state.value,
            "breakout_price": state.breakout_price,
            "bars_since_breakout": state.bars_since_breakout,
        }


