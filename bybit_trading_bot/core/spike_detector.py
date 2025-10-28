from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Deque, Dict, Optional, Tuple

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.core.order_manager import OrderManager
from bybit_trading_bot.indicators.technical import calculate_rsi, calculate_macd, calculate_relative_volume


@dataclass
class SpikeSignal:
    symbol: str
    price: float
    volume: float
    timestamp: datetime
    strength: float


class SpikeDetector:
    def __init__(self, config: Config, order_manager: Optional[OrderManager] = None) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self.buffer_size = 100
        self.price_buffer: Deque[float] = deque(maxlen=self.buffer_size)
        self.volume_buffer: Deque[float] = deque(maxlen=self.buffer_size)
        self.timestamp_buffer: Deque[datetime] = deque(maxlen=self.buffer_size)
        self.om = order_manager
        # orderbook state
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.last_imbalance: Optional[float] = None
        self.last_spread: Optional[float] = None

    def initialize_buffers(self) -> None:
        self.price_buffer.clear()
        self.volume_buffer.clear()
        self.timestamp_buffer.clear()

    def update_market_data(self, price: float, volume: float, ts: datetime) -> None:
        self.price_buffer.append(float(price))
        self.volume_buffer.append(float(volume))
        self.timestamp_buffer.append(ts)

    def detect_volume_spike(self) -> bool:
        lookback = max(5, int(self.config.volume_lookback_periods))
        if len(self.volume_buffer) < lookback:
            return False
        recent = list(self.volume_buffer)
        base = recent[-lookback:][:-1]
        if not base:
            return False
        avg = sum(base) / len(base)
        last = recent[-1]
        if avg <= 0:
            return False
        ratio = last / avg
        return ratio >= float(self.config.volume_spike_multiplier)

    def calculate_signal_strength(self) -> float:
        strength = 0.0
        if self.detect_volume_spike():
            strength += 0.5
        prices = list(self.price_buffer)
        if len(prices) >= max(26, self.config.rsi_period + 1):
            rsi = calculate_rsi(prices, period=self.config.rsi_period)
            if rsi and (rsi[-1] < self.config.rsi_overbought):
                strength += 0.25
            macd_line, signal_line = calculate_macd(prices, fast=self.config.macd_fast, slow=self.config.macd_slow, signal=self.config.macd_signal)
            if macd_line and signal_line and (macd_line[-1] > signal_line[-1]):
                strength += 0.25
        # orderbook contribution
        if self.last_imbalance is not None and self.last_spread is not None:
            if self.last_imbalance >= float(self.config.orderbook_imbalance_threshold) and self.last_spread <= float(self.config.bid_ask_spread_threshold):
                strength = min(1.0, strength + 0.15)
        return min(1.0, strength)

    def update_orderbook(self, best_bid: float, best_ask: float, bids: Optional[list] = None, asks: Optional[list] = None, levels: int = 5) -> None:
        self.best_bid = best_bid
        self.best_ask = best_ask
        try:
            spread = 0.0 if best_ask <= 0 else (best_ask - best_bid) / best_ask
            self.last_spread = spread
            imb = None
            if bids and asks:
                bvol = sum(float(q) for _, q in bids[:levels]) if bids else 0.0
                avol = sum(float(q) for _, q in asks[:levels]) if asks else 0.0
                denom = (bvol + avol)
                imb = (bvol - avol) / denom if denom > 0 else 0.0
            self.last_imbalance = imb
        except Exception:
            pass

    def generate_trading_signal(self, symbol: str) -> Optional[SpikeSignal]:
        if not self.price_buffer:
            return None
        strength = self.calculate_signal_strength()
        if strength < float(self.config.split_min_signal_strength):
            return None
        return SpikeSignal(
            symbol=symbol,
            price=self.price_buffer[-1],
            volume=self.volume_buffer[-1] if self.volume_buffer else 0.0,
            timestamp=self.timestamp_buffer[-1] if self.timestamp_buffer else datetime.utcnow(),
            strength=strength,
        )

    def execute_signal(self, signal: SpikeSignal) -> Optional[str]:
        if self.om is None:
            self.logger.warning("OrderManager not set for SpikeDetector")
            return None
        try:
            # Position size from TRADE_NOTIONAL_USDT
            if signal.price <= 0.0:
                return None
            qty = (self.config.trade_notional_usdt or 0.0) / signal.price
            if qty <= 0:
                return None
            # Limit entry with offset below current
            entry_price = signal.price * (1.0 - float(self.config.limit_order_offset))
            resp = self.om.place_limit_order(signal.symbol, "Buy", qty, entry_price)
            if not resp:
                return None
            order_id = str(resp.get("orderId") or resp.get("result", {}).get("orderId") or "")
            if not order_id:
                return None
            # Wait for fill then place TP (+ target_profit_pct)
            row = self.om.wait_for_filled(order_id, timeout_s=float(self.config.order_timeout_seconds))
            if row and str(row.get("orderStatus", "")).lower() == "filled":
                self.om.post_fill_tp_sl(signal.symbol, row, tp_pct=float(self.config.target_profit_pct), sl_pct=0.0)
                return order_id
            # timeout — cancel the order
            try:
                self.om.cancel_order(signal.symbol, order_id)
            except Exception:
                pass
            return order_id
        except Exception as e:
            self.logger.error(f"execute_signal error: {e}")
            return None
