from __future__ import annotations

from typing import List, Tuple

import numpy as np


def calculate_rsi(prices: List[float], period: int = 14) -> List[float]:
    if len(prices) < period + 1:
        return []
    arr = np.asarray(prices, dtype=float)
    diffs = np.diff(arr)
    ups = np.clip(diffs, 0, None)
    downs = -np.clip(diffs, None, 0)
    roll_up = np.convolve(ups, np.ones(period), 'valid') / period
    roll_down = np.convolve(downs, np.ones(period), 'valid') / period
    rs = np.divide(roll_up, np.where(roll_down == 0, np.nan, roll_down))
    rsi = 100 - (100 / (1 + rs))
    return rsi.tolist()


def calculate_macd(prices: List[float], fast: int = 12, slow: int = 26, signal: int = 9) -> Tuple[List[float], List[float]]:
    if len(prices) < slow + signal:
        return [], []
    arr = np.asarray(prices, dtype=float)
    def ema(x: np.ndarray, span: int) -> np.ndarray:
        alpha = 2 / (span + 1)
        out = np.empty_like(x)
        out[0] = x[0]
        for i in range(1, len(x)):
            out[i] = alpha * x[i] + (1 - alpha) * out[i - 1]
        return out
    ema_fast = ema(arr, fast)
    ema_slow = ema(arr, slow)
    macd_line = ema_fast - ema_slow
    signal_line = ema(macd_line, signal)
    return macd_line.tolist(), signal_line.tolist()


def calculate_relative_volume(volumes: List[float], period: int = 20) -> List[float]:
    if len(volumes) < period + 1:
        return []
    arr = np.asarray(volumes, dtype=float)
    rel = []
    for i in range(period, len(arr)):
        window = arr[i - period:i]
        avg = window.mean() if window.size > 0 else 0.0
        rel.append(0.0 if avg == 0 else float(arr[i] / avg))
    return rel


def calculate_ema(prices: List[float], period: int) -> List[float]:
    """Simple EMA implementation consistent with MACD's internal EMA.

    Returns a list of EMA values aligned with the input length.
    If not enough data, returns empty list.
    """
    if not prices or period <= 0:
        return []
    arr = np.asarray(prices, dtype=float)
    if arr.size < 2:
        return []
    alpha = 2.0 / (period + 1.0)
    out = np.empty_like(arr)
    out[0] = arr[0]
    for i in range(1, len(arr)):
        out[i] = alpha * arr[i] + (1.0 - alpha) * out[i - 1]
    return out.tolist()


def calculate_vwap(high: List[float], low: List[float], close: List[float], volume: List[float]) -> List[float]:
    """Session VWAP cumulative over provided series.

    Typical price = (H+L+C)/3, cumulative(sum(typical*vol))/cumulative(sum(vol)).
    If any array lengths mismatch or insufficient data, returns empty list.
    """
    if not (high and low and close and volume):
        return []
    n = min(len(high), len(low), len(close), len(volume))
    if n == 0:
        return []
    h = np.asarray(high[:n], dtype=float)
    l = np.asarray(low[:n], dtype=float)
    c = np.asarray(close[:n], dtype=float)
    v = np.asarray(volume[:n], dtype=float)
    tp = (h + l + c) / 3.0
    pv = tp * v
    cum_pv = np.cumsum(pv)
    cum_v = np.cumsum(v)
    # Avoid division by zero
    cum_v = np.where(cum_v == 0, np.nan, cum_v)
    vwap = cum_pv / cum_v
    return vwap.tolist()


def calculate_atr(high: List[float], low: List[float], close: List[float], period: int = 14) -> List[float]:
    if not (high and low and close) or len(high) != len(low) or len(low) != len(close):
        return []
    n = len(close)
    if n < period + 1:
        return []
    h = np.asarray(high, dtype=float)
    l = np.asarray(low, dtype=float)
    c = np.asarray(close, dtype=float)
    tr = np.zeros(n, dtype=float)
    tr[0] = h[0] - l[0]
    for i in range(1, n):
        hl = h[i] - l[i]
        hc = abs(h[i] - c[i - 1])
        lc = abs(l[i] - c[i - 1])
        tr[i] = max(hl, hc, lc)
    atr = np.empty(n, dtype=float)
    atr[:period] = np.nan
    atr[period] = tr[1:period + 1].mean()
    alpha = 1.0 / float(period)
    for i in range(period + 1, n):
        atr[i] = (atr[i - 1] * (period - 1) + tr[i]) * alpha
    return atr.tolist()


def calculate_adx(high: List[float], low: List[float], close: List[float], period: int = 14) -> List[float]:
    if not (high and low and close) or len(high) != len(low) or len(low) != len(close):
        return []
    n = len(close)
    if n < period + 1:
        return []
    h = np.asarray(high, dtype=float)
    l = np.asarray(low, dtype=float)
    c = np.asarray(close, dtype=float)
    dm_plus = np.zeros(n, dtype=float)
    dm_minus = np.zeros(n, dtype=float)
    tr = np.zeros(n, dtype=float)
    for i in range(1, n):
        up_move = h[i] - h[i - 1]
        down_move = l[i - 1] - l[i]
        dm_plus[i] = up_move if up_move > down_move and up_move > 0 else 0.0
        dm_minus[i] = down_move if down_move > up_move and down_move > 0 else 0.0
        hl = h[i] - l[i]
        hc = abs(h[i] - c[i - 1])
        lc = abs(l[i] - c[i - 1])
        tr[i] = max(hl, hc, lc)
    # Smooth TR, DM
    tr_s = np.zeros(n, dtype=float)
    dm_plus_s = np.zeros(n, dtype=float)
    dm_minus_s = np.zeros(n, dtype=float)
    tr_s[period] = tr[1:period + 1].sum()
    dm_plus_s[period] = dm_plus[1:period + 1].sum()
    dm_minus_s[period] = dm_minus[1:period + 1].sum()
    for i in range(period + 1, n):
        tr_s[i] = tr_s[i - 1] - tr_s[i - 1] / period + tr[i]
        dm_plus_s[i] = dm_plus_s[i - 1] - dm_plus_s[i - 1] / period + dm_plus[i]
        dm_minus_s[i] = dm_minus_s[i - 1] - dm_minus_s[i - 1] / period + dm_minus[i]
    di_plus = np.zeros(n, dtype=float)
    di_minus = np.zeros(n, dtype=float)
    for i in range(period, n):
        if tr_s[i] > 0:
            di_plus[i] = 100.0 * (dm_plus_s[i] / tr_s[i])
            di_minus[i] = 100.0 * (dm_minus_s[i] / tr_s[i])
    dx = np.zeros(n, dtype=float)
    for i in range(period, n):
        denom = di_plus[i] + di_minus[i]
        dx[i] = 100.0 * (abs(di_plus[i] - di_minus[i]) / denom) if denom > 0 else 0.0
    adx = np.zeros(n, dtype=float)
    adx[:period * 2] = np.nan
    # Initialize ADX with average of first period dx values after initial period
    init_start = period
    init_end = period * 2
    if init_end < n:
        adx[init_end] = np.nanmean(dx[init_start:init_end + 1])
        for i in range(init_end + 1, n):
            adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period
    return adx.tolist()
