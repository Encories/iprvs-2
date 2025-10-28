from __future__ import annotations

from typing import List, Tuple, Optional


class GradientMomentumDetector:
    """Early momentum detector based on price gradient and acceleration.

    Lightweight implementation to fit <50ms per tick. Accepts a small sliding
    window of price series and returns gradient/acceleration and an early signal flag.
    """

    def __init__(self, config) -> None:
        # Pull from config dict-like or object
        self.gradient_threshold: float = _get_conf(config, "momentum_gradient_threshold", 0.15)
        self.lookback_periods: int = int(_get_conf(config, "price_acceleration_lookback", 3))
        self.confirmation_bars: int = int(_get_conf(config, "momentum_confirmation_bars", 2))
        self.early_multiplier: float = _get_conf(config, "early_momentum_multiplier", 1.8)

        # Simple state for confirmation
        self._recent_pass: int = 0

    def calculate_momentum_gradient(self, price_data: List[Tuple[float, float]]) -> Tuple[Optional[float], Optional[float]]:
        """Compute first derivative (gradient) and second derivative (acceleration).

        price_data: list of (timestamp, price), ascending by time.
        Returns: (gradient, acceleration) or (None, None) if insufficient data.
        """
        n = len(price_data)
        if n < max(3, self.lookback_periods + 2):
            return None, None
        # Use simple finite differences over last lookback window
        # Normalize gradient by price to make threshold scale-invariant
        try:
            p2 = float(price_data[-1][1])
            p1 = float(price_data[-1 - self.lookback_periods][1])
            p0 = float(price_data[-1 - 2 * self.lookback_periods][1])
        except Exception:
            return None, None

        if p1 <= 0 or p2 <= 0 or p0 <= 0:
            return None, None

        g1 = (p2 - p1) / p1  # recent gradient (approx % change)
        g0 = (p1 - p0) / p0  # previous gradient
        a = g1 - g0          # acceleration
        return g1, a

    def detect_acceleration(self, momentum_history: List[Tuple[float, float]]) -> bool:
        """True if recent acceleration is positive and increasing.

        momentum_history: list of (gradient, acceleration)
        """
        if not momentum_history:
            return False
        grad, acc = momentum_history[-1]
        if grad is None or acc is None:
            return False
        return (grad >= self.gradient_threshold) and (acc > 0)

    def validate_early_signal(self, gradient: Optional[float], volume_data: Optional[List[Tuple[float, float]]]) -> bool:
        """Confirm early momentum by simple volume confirmation.

        Rule: current gradient >= threshold AND current volume >= early_multiplier * avg(volume N)
        """
        if gradient is None or gradient < self.gradient_threshold:
            self._recent_pass = 0
            return False
        if not volume_data or len(volume_data) < max(3, self.lookback_periods):
            # allow price-only confirmation if no volume
            self._recent_pass += 1
            return self._recent_pass >= self.confirmation_bars

        vols = [float(v) for (_, v) in volume_data[-max(5, self.lookback_periods * 2):]]
        if len(vols) < 3:
            self._recent_pass += 1
            return self._recent_pass >= self.confirmation_bars
        current = vols[-1]
        avg = sum(vols[:-1]) / max(1, len(vols) - 1)
        if avg <= 0:
            self._recent_pass += 1
            return self._recent_pass >= self.confirmation_bars
        ok = current >= (self.early_multiplier * avg)
        if ok:
            self._recent_pass += 1
        else:
            self._recent_pass = 0
        return self._recent_pass >= self.confirmation_bars


def _get_conf(config, key: str, default):
    try:
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key)
    except Exception:
        return default


