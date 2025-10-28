from __future__ import annotations

from typing import Optional, List, Tuple


class MomentumExhaustionDetector:
    """Detect momentum exhaustion via RSI, volume, and candle expansion.

    Designed to be lightweight and robust to missing data. Methods return booleans
    and can be combined by the caller to block entries near exhaustion.
    """

    def __init__(self, config) -> None:
        self.exhaustion_rsi_threshold: float = _get_conf(config, "exhaustion_rsi_threshold", 78.0)
        self.volume_exhaustion_ratio: float = _get_conf(config, "volume_exhaustion_ratio", 0.3)
        self.candle_exhaustion_multiplier: float = _get_conf(config, "candle_exhaustion_multiplier", 2.2)
        self.momentum_divergence_detection: bool = bool(_get_conf(config, "momentum_divergence_detection", True))

    def detect_rsi_exhaustion(self, rsi_value: Optional[float], price_direction: int) -> bool:
        """Return True if RSI suggests exhaustion in the current direction.

        price_direction: +1 for up, -1 for down, 0 unknown
        """
        if rsi_value is None:
            return False
        if price_direction >= 0:
            return rsi_value >= self.exhaustion_rsi_threshold
        # For shorts, mirror could be applied, but spot bot focuses on buys
        return False

    def detect_volume_exhaustion(self, current_volume: Optional[float], avg_volume: Optional[float]) -> bool:
        """Return True if relative volume is falling significantly during move.

        Interpreted as exhaustion if current_volume < avg_volume * volume_exhaustion_ratio.
        """
        if current_volume is None or avg_volume is None or avg_volume <= 0:
            return False
        return current_volume < (avg_volume * self.volume_exhaustion_ratio)

    def detect_candle_exhaustion(self, candle_size: Optional[float], avg_candle_size: Optional[float]) -> bool:
        """Return True if current candle expands far beyond recent average size."""
        if candle_size is None or avg_candle_size is None or avg_candle_size <= 0:
            return False
        return candle_size >= (self.candle_exhaustion_multiplier * avg_candle_size)

    def check_momentum_divergence(self, price_data: List[Tuple[float, float]], momentum_series: Optional[List[float]]) -> bool:
        """Very lightweight divergence heuristic.

        If enabled and we have momentum_series, detect a simple lower-high in momentum
        while price makes a higher-high on the last two swing points. Returns True if divergence found.
        """
        if not self.momentum_divergence_detection:
            return False
        if not price_data or not momentum_series or len(price_data) < 3 or len(momentum_series) < 3:
            return False
        try:
            p_last = float(price_data[-1][1])
            p_prev = float(price_data[-2][1])
            m_last = float(momentum_series[-1])
            m_prev = float(momentum_series[-2])
        except Exception:
            return False
        # Price up but momentum down => bearish divergence
        return (p_last > p_prev) and (m_last < m_prev)


def _get_conf(config, key: str, default):
    try:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key)
    except Exception:
        return default


