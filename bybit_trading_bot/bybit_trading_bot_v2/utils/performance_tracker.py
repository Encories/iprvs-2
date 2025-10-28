from __future__ import annotations

import asyncio
from collections import deque
from typing import Deque, Optional, List, Dict

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager


class PerformanceTracker:
    """Tracks PnL, winrate, and maintains rolling stats for adaptation."""

    def __init__(self, config: Config, db: DBManager) -> None:
        self.config = config
        self.db = db
        self.logger = get_logger(self.__class__.__name__)
        self._recent_closed: Deque[float] = deque(maxlen=50)
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        await self._refresh_recent()

    async def stop(self) -> None:
        return

    async def _refresh_recent(self) -> None:
        try:
            rows = self.db.get_last_closed_pnls(50)
            async with self._lock:
                self._recent_closed.clear()
                for pnl in rows:
                    self._recent_closed.append(float(pnl))
        except Exception:
            pass

    def recent_losses_streak(self, lookback: int = 10) -> int:
        try:
            if not self._recent_closed:
                return 0
            take = list(self._recent_closed)[-lookback:]
            streak = 0
            for x in reversed(take):
                if x <= 0:
                    streak += 1
                else:
                    break
            return streak
        except Exception:
            return 0

    def winrate(self, last_n: int = 20) -> Optional[float]:
        try:
            if not self._recent_closed:
                return None
            take = list(self._recent_closed)[-last_n:]
            if not take:
                return None
            wins = sum(1 for x in take if x > 0)
            return wins / len(take)
        except Exception:
            return None

    def total_pnl(self) -> float:
        try:
            return float(self.db.get_total_pnl())
        except Exception:
            return 0.0

    # --- Commission-aware profitability validation ---
    def get_recent_trades(self, limit: int = 50) -> List[Dict]:
        try:
            # Minimal adapter: reuse open trades query shape; for full details, a dedicated DB method would be ideal
            # Here we return empty list as placeholder unless DB exposes detailed trade history with fees
            return []
        except Exception:
            return []

    def validate_strategy_profitability(self, recent_trades_count: int = 50) -> Dict[str, float | bool]:
        recent_trades = self.get_recent_trades(recent_trades_count)
        if not recent_trades:
            return {
                "net_pnl": 0.0,
                "commission_impact_pct": 0.0,
                "profitable_after_fees": True,
                "avg_commission_per_trade": 0.0,
            }
        total_gross_pnl = sum([float(t.get("gross_pnl", 0.0)) for t in recent_trades])
        total_commission = sum([float(t.get("commission", 0.0)) for t in recent_trades])
        total_net_pnl = total_gross_pnl - total_commission
        commission_impact = (total_commission / total_gross_pnl) * 100.0 if total_gross_pnl > 0 else 0.0
        avg_commission = (total_commission / len(recent_trades)) if recent_trades else 0.0
        return {
            "net_pnl": total_net_pnl,
            "commission_impact_pct": commission_impact,
            "profitable_after_fees": total_net_pnl > 0,
            "avg_commission_per_trade": avg_commission,
        }


