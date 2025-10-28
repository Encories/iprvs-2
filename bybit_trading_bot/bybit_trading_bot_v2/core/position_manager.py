from __future__ import annotations

from typing import Dict

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.logger import get_logger


class PositionManager:
    def __init__(self, config: Config, db: DBManager) -> None:
        self.config = config
        self.db = db
        self.logger = get_logger(self.__class__.__name__)
        self._open: Dict[str, Dict[str, float]] = {}

    def on_open(self, symbol: str, side: str, qty: float, price: float) -> None:
        self._open[symbol] = {"qty": float(qty), "price": float(price), "side": 1.0 if side == "Buy" else -1.0}

    def on_partial(self, symbol: str, new_qty: float) -> None:
        if symbol in self._open:
            self._open[symbol]["qty"] = float(max(0.0, new_qty))

    def on_close(self, symbol: str) -> None:
        self._open.pop(symbol, None)

    def has_open(self, symbol: str) -> bool:
        return symbol in self._open and self._open[symbol].get("qty", 0.0) > 0.0

    def open_count(self) -> int:
        return sum(1 for v in self._open.values() if v.get("qty", 0.0) > 0.0)


