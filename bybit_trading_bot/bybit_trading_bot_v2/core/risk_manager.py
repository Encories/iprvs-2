from __future__ import annotations

from typing import Tuple

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.core.order_manager_futures import FuturesOrderManager
from ..utils.performance_tracker import PerformanceTracker
from ..strategies.volume_breakout import StrategySignal


class RiskManager:
    def __init__(self, config: Config, tracker: PerformanceTracker) -> None:
        self.config = config
        self.tracker = tracker
        self.logger = get_logger(self.__class__.__name__)
        self._om = FuturesOrderManager(config)

    def can_trade_today(self, db: DBManager) -> bool:
        try:
            limit = float(getattr(self.config, "daily_loss_limit", 3.0))
        except Exception:
            limit = 3.0
        if limit <= 0:
            return True
        today_pnl = db.get_today_pnl()
        return today_pnl >= -abs((self.config.account_equity_usdt * limit) / 100.0)

    def is_in_active_session(self) -> bool:
        # Placeholder: could parse EU/US session windows from config
        return True

    def _base_notional(self, confidence: float) -> float:
        base = float(getattr(self.config, "base_position_size", 50.0))
        if bool(getattr(self.config, "confidence_multiplier", True)):
            base *= max(1.0, min(confidence, 2.0))
        # Winrate adaptation
        if bool(getattr(self.config, "win_rate_adaptation", True)):
            wr = self.tracker.winrate(20)
            if wr is not None:
                if wr > 0.6:
                    base *= 1.25
                elif wr < 0.4:
                    base *= 0.6
        return base

    def size_and_targets(self, symbol: str, sig: StrategySignal) -> Tuple[float, float, float, float, Tuple[float, float]]:
        entry = float(sig.ref_price)
        sl = float(sig.sl_price)
        # Risk-based qty fallback
        qty_risk = self._om.calc_risk_based_qty(symbol, entry, sl, self.config.account_equity_usdt)
        # Base notional path
        notional = self._base_notional(sig.confidence)
        lev = float(getattr(self.config, "adaptive_leverage_max", 5.0))
        qty_notional = (notional * lev) / max(1e-12, entry)
        # Strict risk-first sizing: notional cannot exceed per-trade risk
        if qty_notional > 0.0 and qty_risk > 0.0:
            qty = min(qty_risk, qty_notional)
        elif qty_risk > 0.0:
            qty = qty_risk
        else:
            qty = max(0.0, qty_notional)
        # Cap to 5% balance notionally
        max_pct = float(getattr(self.config, "max_position_pct", 5.0))
        cap_qty = ((self.config.account_equity_usdt * max_pct / 100.0) * lev) / max(1e-12, entry)
        qty = min(qty, cap_qty)

        # Targets (switch to fixed percent from entry per spec: 0.6%/1.2%)
        tp1_pct = float(getattr(self.config, "quick_tp_pct", 0.6)) / 100.0
        tp2_pct = float(getattr(self.config, "runner_tp_pct", 1.2)) / 100.0
        direction = 1 if sig.side == "Buy" else -1
        tp1 = entry * (1 + direction * tp1_pct)
        tp2 = entry * (1 + direction * tp2_pct)
        parts = (float(getattr(self.config, "quick_tp_pct", 0.6)), float(getattr(self.config, "runner_tp_pct", 0.4)))
        # Normalize parts sum to 1
        total = parts[0] + parts[1]
        if total <= 0:
            parts = (0.5, 0.5)
        else:
            parts = (parts[0] / total, parts[1] / total)
        return qty, sl, tp1, tp2, parts

    def allow_trade_after_losses(self, max_consecutive_losses: int = 3) -> bool:
        # Simple loss streak guard based on recent PnL
        try:
            # We rely on tracker.recent closed pnl; if last k are all negative, block
            wr = self.tracker.winrate( max(5, max_consecutive_losses) )
            if wr is None:
                return True
            # If last-N winrate is extremely low, block
            return wr > 0.0
        except Exception:
            return True


