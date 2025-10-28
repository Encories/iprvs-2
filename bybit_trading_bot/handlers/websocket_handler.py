from __future__ import annotations

from typing import Callable, List, Optional

from bybit_trading_bot.utils.logger import get_logger

try:
    from pybit.unified_trading import WebSocket
except Exception:  # pragma: no cover
    WebSocket = None  # type: ignore

import time
import threading
import random
import queue


TickerCallback = Callable[[str, float, float], None]
OrderbookCallback = Callable[[str, float, float, list, list], None]
TradeCallback = Callable[[str, float, float, float, Optional[str]], None]


class WebSocketHandler:
    """Обработка WebSocket соединений для спот-рынка (с авто‑переподключением)."""

    def __init__(self, testnet: bool) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self.testnet = testnet
        # Multiple websocket shards to reduce per-connection load
        self._sockets: List[object] = []
        self._symbols: List[str] = []
        self._on_ticker: Optional[TickerCallback] = None
        self._on_orderbook: Optional[OrderbookCallback] = None
        self._on_trade: Optional[TradeCallback] = None
        self._last_tick_time: float = 0.0
        self._lock = threading.Lock()

        # Reconnect monitor
        self._monitor_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        self._base_backoff = 1.0
        self._backoff_cap = 30.0
        # Allow more headroom under load to avoid false reconnects
        self._stale_seconds = 300.0  # no ticks for this duration → reconnect

        # Offload ticker callback work to a queue processed by a consumer thread
        self._tick_queue = queue.Queue(maxsize=10000)
        self._consumer_thread: Optional[threading.Thread] = None

        # Sharding and throttling parameters (tuned for stability and rate limits)
        self._shard_size = 10  # symbols per socket (smaller load per connection)
        self._subscribe_delay_per_symbol = 0.15  # seconds between subscribe frames (~6-7 msgs/sec per socket)
        self._subscribe_delay_between_shards = 3.0  # seconds between shard connections

        # Resubscribe cooldown to avoid thrashing with pybit's own reconnects
        self._last_resubscribe_ts: float = 0.0

        if WebSocket is not None:
            try:
                # Create a probe socket to validate environment; real shards created on subscribe
                self._sockets = [WebSocket(testnet=False, channel_type="spot")]
            except Exception as e:
                self.logger.error(f"Failed to init WebSocket: {e}")

    def connect_to_spot_stream(self, symbols: List[str], on_ticker: Optional[TickerCallback] = None) -> None:
        """Подключение к потоку спот-данных (тикеры) и запуск мониторинга."""
        if WebSocket is None:
            self.logger.warning("WebSocket not available; running in stub mode")
            return
        with self._lock:
            self._symbols = list(symbols)
            self._on_ticker = on_ticker
        try:
            self._create_sharded_sockets_and_subscribe(symbols, on_ticker)
            self._last_tick_time = time.time()
            self.logger.info(f"Subscribed to {len(symbols)} spot tickers across {len(self._sockets)} sockets (shard_size={self._shard_size})")
        except Exception as e:
            self.logger.error(f"Failed to subscribe to ticker stream: {e}")
        # Start health monitor once
        if self._monitor_thread is None:
            self._monitor_thread = threading.Thread(target=self._monitor_loop, name="WSMonitor", daemon=True)
            self._monitor_thread.start()
        # Start consumer thread for ticks
        if self._consumer_thread is None:
            self._consumer_thread = threading.Thread(target=self._consume_ticks_loop, name="WSTickConsumer", daemon=True)
            self._consumer_thread.start()

    def subscribe_orderbook_and_trades(
        self,
        symbols: List[str],
        on_orderbook: Optional[OrderbookCallback] = None,
        on_trade: Optional[TradeCallback] = None,
    ) -> None:
        if WebSocket is None:
            self.logger.warning("WebSocket not available; running in stub mode (orderbook/trades)")
            return
        with self._lock:
            self._symbols = list(set(self._symbols + list(symbols)))
            self._on_orderbook = on_orderbook
            self._on_trade = on_trade
        try:
            # Subscribe orderbook/trades only on the first shard to limit total load
            ws0 = self._sockets[0] if self._sockets else WebSocket(testnet=self.testnet, channel_type="spot")
            if not self._sockets:
                self._sockets = [ws0]
            for sym in symbols:
                # Orderbook 50 levels
                try:
                    ws0.orderbook_stream(callback=(lambda msg, s=sym: self._on_raw_orderbook(msg, s, on_orderbook)), symbol=sym, depth=50)  # type: ignore[attr-defined]
                except Exception:
                    try:
                        ws0.orderbook_50_stream(callback=(lambda msg, s=sym: self._on_raw_orderbook(msg, s, on_orderbook)), symbol=sym)  # type: ignore[attr-defined]
                    except Exception as e:
                        self.logger.debug(f"Orderbook subscribe not available for {sym}: {e}")
                # Public trades
                try:
                    ws0.public_trade_stream(callback=(lambda msg, s=sym: self._on_raw_trade(msg, s, on_trade)), symbol=sym)  # type: ignore[attr-defined]
                except Exception as e:
                    self.logger.debug(f"Trade subscribe not available for {sym}: {e}")
            self._last_tick_time = time.time()
            self.logger.info(f"Subscribed orderbook/trades for {len(symbols)} symbols on shard #0")
        except Exception as e:
            self.logger.error(f"Failed to subscribe orderbook/trades: {e}")
        if self._monitor_thread is None:
            self._monitor_thread = threading.Thread(target=self._monitor_loop, name="WSMonitor", daemon=True)
            self._monitor_thread.start()
        if self._consumer_thread is None:
            self._consumer_thread = threading.Thread(target=self._consume_ticks_loop, name="WSTickConsumer", daemon=True)
            self._consumer_thread.start()

    def _on_raw_ticker(self, message, symbol: str, on_ticker: Optional[TickerCallback]) -> None:
        try:
            data = None
            if isinstance(message, dict):
                data = message.get("data") or message.get("result") or message
            price = None
            # Data can be a dict (single row) or a list of rows (snapshot/delta)
            if isinstance(data, dict):
                lp = data.get("lastPrice") or data.get("lp") or data.get("price")
                if lp is not None:
                    try:
                        price = float(lp)
                    except Exception:
                        price = None
            elif isinstance(data, list) and data:
                row = data[-1] if isinstance(data[-1], dict) else data[0]
                if isinstance(row, dict):
                    lp = row.get("lastPrice") or row.get("lp") or row.get("price")
                    if lp is not None:
                        try:
                            price = float(lp)
                        except Exception:
                            price = None
            if price is not None:
                ts = time.time()
                self._last_tick_time = ts
                try:
                    self._tick_queue.put_nowait((symbol, price, ts))
                except queue.Full:
                    # Drop oldest items to relieve pressure, then enqueue newest
                    try:
                        drain = max(1, self._tick_queue.qsize() // 2)
                        for _ in range(drain):
                            self._tick_queue.get_nowait()
                    except Exception:
                        pass
                    try:
                        self._tick_queue.put_nowait((symbol, price, ts))
                    except Exception:
                        pass
        except Exception as e:
            self.logger.debug(f"Ticker parse error for {symbol}: {e}")

    def _ensure_ws(self) -> bool:
        if WebSocket is None:
            return False
        try:
            # If sockets already exist, let pybit manage reconnection
            if self._sockets:
                return True
            # No sockets present → create fresh (with subscriptions if symbols exist)
            if not self._symbols:
                self._sockets = [WebSocket(testnet=self.testnet, channel_type="spot")]
            else:
                self._create_sharded_sockets_and_subscribe(self._symbols, self._on_ticker)
            return True
        except Exception as e:
            self.logger.error(f"WebSocket init failed: {e}")
            self._sockets = []
            return False

    def _resubscribe_all(self) -> None:
        if not self._sockets:
            return
        with self._lock:
            symbols = list(self._symbols)
            on_ticker = self._on_ticker
            on_orderbook = self._on_orderbook
            on_trade = self._on_trade
        try:
            now = time.time()
            # throttle resubscribe frequency
            if (now - self._last_resubscribe_ts) < 120.0:
                self.logger.debug("Resubscribe skipped due to cooldown")
            else:
                self._resubscribe_across_existing_sockets(symbols, on_ticker, on_orderbook, on_trade)
                self._last_resubscribe_ts = now
        except Exception as e:
            self.logger.error(f"Resubscribe error: {e}")
        self._last_tick_time = time.time()
        self.logger.info(f"Resubscribed to {len(symbols)} spot tickers across {len(self._sockets)} sockets")

    def _monitor_loop(self) -> None:
        backoff = self._base_backoff
        while not self._stop_event.is_set():
            try:
                # Avoid reconnects based on lack of ticks (illiquid symbols). Only reconnect if all sockets are gone.
                needs_reconnect = not self._sockets
                if needs_reconnect:
                    self.logger.warning(
                        f"WS stale or closed. Reconnecting (backoff {backoff:.1f}s)..."
                    )
                    time.sleep(backoff * (0.5 + random.random()))  # jitter
                    if self._ensure_ws():
                        try:
                            self._resubscribe_all()
                            backoff = self._base_backoff
                        except Exception as e:
                            self.logger.error(f"Resubscribe error: {e}")
                            backoff = min(self._backoff_cap, max(self._base_backoff, backoff * 2))
                    else:
                        backoff = min(self._backoff_cap, max(self._base_backoff, backoff * 2))
                time.sleep(2.0)
            except Exception as e:
                self.logger.error(f"WS monitor error: {e}")
                time.sleep(2.0)

    def _consume_ticks_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                symbol, price, ts = self._tick_queue.get(timeout=1.0)
            except Exception:
                continue
            try:
                cb = self._on_ticker
                if callable(cb):
                    cb(symbol, price, ts)
            except Exception as e:
                self.logger.debug(f"Tick consumer error for {symbol}: {e}")

    def _on_raw_orderbook(self, message, symbol: str, on_orderbook: Optional[OrderbookCallback]) -> None:
        if not callable(on_orderbook):
            return
        try:
            data = None
            if isinstance(message, dict):
                data = message.get("data") or message.get("result") or message
            bids = []
            asks = []
            best_bid = None
            best_ask = None
            if isinstance(data, dict):
                b = data.get("b") or data.get("bid") or data.get("bids")
                a = data.get("a") or data.get("ask") or data.get("asks")
                if isinstance(b, list):
                    for row in b:
                        try:
                            price = float(row[0]) if isinstance(row, (list, tuple)) else float(row.get("price"))
                            qty = float(row[1]) if isinstance(row, (list, tuple)) else float(row.get("size"))
                            bids.append((price, qty))
                        except Exception:
                            continue
                if isinstance(a, list):
                    for row in a:
                        try:
                            price = float(row[0]) if isinstance(row, (list, tuple)) else float(row.get("price"))
                            qty = float(row[1]) if isinstance(row, (list, tuple)) else float(row.get("size"))
                            asks.append((price, qty))
                        except Exception:
                            continue
                if bids:
                    best_bid = bids[0][0]
                if asks:
                    best_ask = asks[0][0]
            if best_bid is not None and best_ask is not None:
                on_orderbook(symbol, best_bid, best_ask, bids, asks)
        except Exception as e:
            self.logger.debug(f"Orderbook parse error for {symbol}: {e}")

    def _on_raw_trade(self, message, symbol: str, on_trade: Optional[TradeCallback]) -> None:
        if not callable(on_trade):
            return
        try:
            data = None
            if isinstance(message, dict):
                data = message.get("data") or message.get("result") or message
            if isinstance(data, dict):
                rows = data.get("list") or data.get("data") or []
                if isinstance(rows, list):
                    for row in rows:
                        try:
                            p = float(row.get("p") or row.get("price"))
                            v = float(row.get("v") or row.get("size") or row.get("qty") or 0.0)
                            t = float(row.get("T") or row.get("ts") or 0.0)
                            side_field = row.get("S") or row.get("side")
                            side = None
                            if isinstance(side_field, str):
                                s = side_field.strip().lower()
                                if s in {"buy", "sell"}:
                                    side = s.capitalize()
                            on_trade(symbol, p, v, t, side)
                        except Exception:
                            continue
        except Exception as e:
            self.logger.debug(f"Trade parse error for {symbol}: {e}")

    def reconnect_on_failure(self) -> None:
        self.logger.info("Manual reconnection requested")
        self._last_tick_time = 0.0

    def stop(self) -> None:
        self._stop_event.set()
        # Best-effort: allow monitor thread to exit
        try:
            if self._monitor_thread and self._monitor_thread.is_alive():
                self._monitor_thread.join(timeout=3)
        except Exception:
            pass
        self._monitor_thread = None
        self._sockets = []
        try:
            if self._consumer_thread and self._consumer_thread.is_alive():
                # Allow consumer to see stop_event and exit
                time.sleep(0.2)
        except Exception:
            pass
        self._consumer_thread = None

    # ---- helpers ----
    def _create_sharded_sockets_and_subscribe(self, symbols: List[str], on_ticker: Optional[TickerCallback]) -> None:
        if WebSocket is None:
            raise RuntimeError("WebSocket API not available")
        # Close previous sockets by dropping references (pybit cleans up on GC)
        self._sockets = []
        if not symbols:
            self._sockets = [WebSocket(testnet=False, channel_type="spot")]
            return
        # Chunk symbols
        chunks: List[List[str]] = []
        batch: List[str] = []
        for s in symbols:
            batch.append(s)
            if len(batch) >= self._shard_size:
                chunks.append(batch)
                batch = []
        if batch:
            chunks.append(batch)
        # Create sockets and subscribe per chunk (staggered and throttled)
        for idx, chunk in enumerate(chunks):
            ws = WebSocket(testnet=False, channel_type="spot")
            self._sockets.append(ws)
            # Small gap before starting a new shard to avoid simultaneous spikes
            if idx > 0:
                time.sleep(self._subscribe_delay_between_shards)
            for sym in chunk:
                try:
                    ws.ticker_stream(callback=(lambda msg, s=sym: self._on_raw_ticker(msg, s, on_ticker)), symbol=sym)
                except Exception as e:
                    self.logger.error(f"Subscribe failed for {sym} on shard {idx}: {e}")
                # Respect rate limits: throttle per-symbol subscription messages
                time.sleep(self._subscribe_delay_per_symbol)

    def _resubscribe_across_existing_sockets(
        self,
        symbols: List[str],
        on_ticker: Optional[TickerCallback],
        on_orderbook: Optional[OrderbookCallback],
        on_trade: Optional[TradeCallback],
    ) -> None:
        if WebSocket is None:
            raise RuntimeError("WebSocket API not available")
        # Ensure at least one socket exists
        if not self._sockets:
            self._sockets = [WebSocket(testnet=self.testnet, channel_type="spot")]
        # Split symbols into chunks targeting existing sockets
        chunks: List[List[str]] = []
        batch: List[str] = []
        for s in symbols:
            batch.append(s)
            if len(batch) >= self._shard_size:
                chunks.append(batch)
                batch = []
        if batch:
            chunks.append(batch)
        # Create more sockets if needed
        while len(self._sockets) < len(chunks):
            self._sockets.append(WebSocket(testnet=False, channel_type="spot"))
            time.sleep(self._subscribe_delay_between_shards)
        # Subscribe tickers per socket chunk
        for idx, chunk in enumerate(chunks):
            ws = self._sockets[idx]
            for sym in chunk:
                try:
                    ws.ticker_stream(callback=(lambda msg, s=sym: self._on_raw_ticker(msg, s, on_ticker)), symbol=sym)
                except Exception as e:
                    self.logger.debug(f"Resubscribe failed for {sym} on shard {idx}: {e}")
                time.sleep(self._subscribe_delay_per_symbol)
        # Re-subscribe OB/trades on shard #0 if handlers present
        if on_orderbook or on_trade:
            ws0 = self._sockets[0]
            for sym in symbols:
                if on_orderbook:
                    try:
                        ws0.orderbook_stream(callback=(lambda msg, s=sym: self._on_raw_orderbook(msg, s, on_orderbook)), symbol=sym, depth=50)  # type: ignore[attr-defined]
                    except Exception:
                        try:
                            ws0.orderbook_50_stream(callback=(lambda msg, s=sym: self._on_raw_orderbook(msg, s, on_orderbook)), symbol=sym)  # type: ignore[attr-defined]
                        except Exception:
                            pass
                    time.sleep(self._subscribe_delay_per_symbol)
                if on_trade:
                    try:
                        ws0.public_trade_stream(callback=(lambda msg, s=sym: self._on_raw_trade(msg, s, on_trade)), symbol=sym)  # type: ignore[attr-defined]
                    except Exception:
                        pass
                    time.sleep(self._subscribe_delay_per_symbol)