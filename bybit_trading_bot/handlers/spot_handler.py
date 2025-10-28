from __future__ import annotations

from typing import List, Optional, Callable

from bybit_trading_bot.handlers.websocket_handler import WebSocketHandler, TickerCallback
from bybit_trading_bot.utils.logger import get_logger


class SpotHandler:
    def __init__(self, testnet: bool) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.ws = WebSocketHandler(testnet=testnet)

    def subscribe_tickers(self, symbols: List[str]) -> None:
        self.ws.connect_to_spot_stream(symbols)

    def subscribe_tickers_with_callback(self, symbols: List[str], callback: TickerCallback) -> None:
        self.ws.connect_to_spot_stream(symbols, on_ticker=callback) 