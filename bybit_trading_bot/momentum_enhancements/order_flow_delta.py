from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Tuple


@dataclass(frozen=True)
class DeltaResult:
    imbalance: Optional[float]
    cumulative_delta: Optional[float]
    aggression_ratio: Optional[float]


class OrderFlowDeltaAnalyzer:
    """Lightweight order-flow delta analysis with graceful degradation.

    Expects trades_data as list of tuples: (price, qty, ts, side)
    where side is 'Buy'/'Sell' or +1/-1. If side is missing, returns None metrics.
    """

    def __init__(self, config) -> None:
        self.delta_imbalance_min: float = float(_get_conf(config, "delta_imbalance_min", 0.6))
        self.cumulative_delta_enabled: bool = bool(_get_conf(config, "cumulative_delta_divergence", True))
        self.aggression_ratio_min: float = float(_get_conf(config, "bid_ask_aggression_ratio", 0.7))
        self.filter_enabled: bool = bool(_get_conf(config, "order_flow_momentum_filter", True))

    def calculate_delta_volume(self, trades_data: Optional[List[Tuple[float, float, float, Optional[str]]]]) -> DeltaResult:
        if not trades_data:
            return DeltaResult(None, None, None)
        buys = 0.0
        sells = 0.0
        cum_delta = 0.0
        have_side = False
        for p, q, _ts, side in trades_data:
            try:
                qty = float(q)
            except Exception:
                qty = 0.0
            sgn: Optional[int] = None
            if isinstance(side, str):
                s = side.strip().lower()
                if s in ("buy", "b", "+1"):
                    sgn = +1
                elif s in ("sell", "s", "-1"):
                    sgn = -1
            elif isinstance(side, (int, float)):
                sgn = 1 if side > 0 else (-1 if side < 0 else None)
            if sgn is None:
                continue
            have_side = True
            if sgn > 0:
                buys += qty
                cum_delta += qty
            else:
                sells += qty
                cum_delta -= qty
        if not have_side:
            return DeltaResult(None, None, None)
        total = buys + sells
        imbalance = None if total <= 0 else abs(buys - sells) / total
        aggression_ratio = None if total <= 0 else (buys / total)
        return DeltaResult(imbalance, (cum_delta if self.cumulative_delta_enabled else None), aggression_ratio)

    def detect_delta_divergence(self, price_data: List[Tuple[float, float]], delta_series: Optional[List[float]]) -> bool:
        if not self.cumulative_delta_enabled or not price_data or not delta_series:
            return False
        if len(price_data) < 3 or len(delta_series) < 3:
            return False
        try:
            p_last, p_prev = float(price_data[-1][1]), float(price_data[-2][1])
            d_last, d_prev = float(delta_series[-1]), float(delta_series[-2])
        except Exception:
            return False
        return (p_last > p_prev) and (d_last < d_prev)

    def analyze_order_aggression(self, orderbook_data: Optional[Tuple[float, float]]) -> Optional[float]:
        # Optional hook: pass (best_bid, best_ask) to estimate spread tightness; not used here
        return None

    def validate_institutional_flow(self, delta_imbalance: Optional[float], aggression_ratio: Optional[float]) -> bool:
        if delta_imbalance is None or aggression_ratio is None:
            return True  # cannot decide, do not block
        # Require both a minimum imbalance and a minimum buy aggression for long entries
        return (delta_imbalance >= self.delta_imbalance_min) and (aggression_ratio >= self.aggression_ratio_min)


def _get_conf(config, key: str, default):
    try:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key)
    except Exception:
        return default


