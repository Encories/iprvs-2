from __future__ import annotations

import asyncio
from typing import Callable, Optional

from bybit_trading_bot.utils.logger import get_logger


class WebSocketClient:
    def __init__(self, url: str) -> None:
        self.url = url
        self.logger = get_logger(self.__class__.__name__)
        self._stop = asyncio.Event()
        self._on_message: Optional[Callable[[dict], None]] = None

    def set_message_handler(self, handler: Callable[[dict], None]) -> None:
        self._on_message = handler

    async def connect(self) -> None:
        # Placeholder for real websockets connection
        self.logger.info(f"WS connect to {self.url} (stub)")

    async def subscribe_channels(self, symbol: str) -> None:
        self.logger.info(f"WS subscribe (stub): {symbol}")

    async def close(self) -> None:
        self._stop.set()
        self.logger.info("WS closed (stub)")
