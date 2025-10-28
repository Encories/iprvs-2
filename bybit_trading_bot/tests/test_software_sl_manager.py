import unittest
from unittest.mock import Mock
from datetime import datetime

from bybit_trading_bot.core.software_sl_manager import SoftwareSLManager, SoftwareSLPosition


class TestSoftwareSLManager(unittest.TestCase):

    def setUp(self):
        self.config = Mock()
        self.config.fee_rate = 0.001
        self.config.software_sl_check_interval = 0.05
        self.config.software_sl_price_cache_ttl = 5.0
        self.db = Mock()
        self.notifier = Mock()
        self.sl_manager = SoftwareSLManager(self.config, self.db, self.notifier)

    def test_add_sl_position(self):
        self.sl_manager.add_sl_position("trade123", "BTCUSDT", 1.0, 50000.0, 49000.0)
        self.assertEqual(self.sl_manager.get_active_positions_count(), 1)
        info = self.sl_manager.get_positions_info()[0]
        self.assertEqual(info["trade_id"], "trade123")
        self.assertEqual(info["symbol"], "BTCUSDT")

    def test_price_caching(self):
        # Configure DB to return symbol mapping and price
        rec = Mock()
        rec.id = 1
        rec.spot_symbol = "BTCUSDT"
        self.db.get_active_symbols.return_value = [rec]
        self.db.get_last_price.return_value = 123.45

        p1 = self.sl_manager.get_current_price("BTCUSDT")
        p2 = self.sl_manager.get_current_price("BTCUSDT")
        self.assertEqual(p1, 123.45)
        self.assertEqual(p2, 123.45)
        # get_last_price should be called only once due to caching
        self.db.get_last_price.assert_called_once_with(1)

    def test_sl_trigger(self):
        # Prepare price returns so that trigger happens
        rec = Mock()
        rec.id = 1
        rec.spot_symbol = "BTCUSDT"
        self.db.get_active_symbols.return_value = [rec]
        self.db.get_last_price.return_value = 48000.0

        # Stub order manager close called via SoftwareSLManager._execute_sl_exit
        # We can't easily intercept internal OrderManager; instead check DB updates
        self.db.close_trade = Mock()

        self.sl_manager.add_sl_position("t1", "BTCUSDT", 1.0, 50000.0, 49000.0)
        # Run single check
        self.sl_manager._check_sl_positions()

        # Should have closed trade once
        self.db.close_trade.assert_called_once()
        self.assertEqual(self.sl_manager.get_active_positions_count(), 0)


if __name__ == '__main__':
    unittest.main()


