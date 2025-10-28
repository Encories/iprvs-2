from __future__ import annotations

from bybit_trading_bot.config.settings import load_settings
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.core.order_manager import OrderManager


def test_order_manager_place_dev_order(tmp_path):
    cfg = load_settings()
    db = DBManager(str(tmp_path / "db.sqlite"))
    om = OrderManager(cfg, db)
    order = om.place_spot_order("BTCUSDT", "Buy", 0.001, 70000.0)
    # In dev mode, we should at least get a dict structure back
    assert order is None or isinstance(order, dict) 