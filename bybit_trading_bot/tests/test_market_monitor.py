from __future__ import annotations

from bybit_trading_bot.config.settings import load_settings
from bybit_trading_bot.core.market_monitor import MarketMonitor


def test_monitor_start_stop():
    cfg = load_settings()
    monitor = MarketMonitor(cfg)
    monitor.start_monitoring()
    assert monitor.is_running is True
    monitor.stop_monitoring()
    assert monitor.is_running is False 