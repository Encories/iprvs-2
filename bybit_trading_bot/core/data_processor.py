from __future__ import annotations

from typing import Iterable, List, Sequence, Tuple


def calculate_percentage_change_from_series(values: Sequence[float]) -> float:
    """Return percent change between first and last value in series.

    If fewer than 2 points, returns 0.0.
    """
    if not values or len(values) < 2:
        return 0.0
    first = float(values[0])
    last = float(values[-1])
    if first == 0:
        return 0.0
    return (last - first) / first * 100.0


esseries = List[Tuple[object, float]]


def calculate_percentage_change(time_value_series: esseries) -> float:
    """Return percent change for list of (timestamp, value) tuples.

    If fewer than 2 points or zero baseline, returns 0.0.
    """
    if not time_value_series or len(time_value_series) < 2:
        return 0.0
    first = float(time_value_series[0][1])
    last = float(time_value_series[-1][1])
    if first == 0.0:
        return 0.0
    return (last - first) / first * 100.0


def meets_thresholds(price_change: float, oi_change: float, price_threshold: float, oi_threshold: float) -> bool:
    return price_change >= price_threshold and oi_change >= oi_threshold 