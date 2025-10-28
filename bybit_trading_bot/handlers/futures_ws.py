from __future__ import annotations

from typing import Callable, Optional, List, Dict, Any
import threading
import time

from ..utils.logger import get_logger

try:
    from pybit.unified_trading import WebSocket
except Exception:  # pragma: no cover
    WebSocket = None  # type: ignore


KlineCallback = Callable[[str, Dict[str, Any]], None]
KlineCallbackHTF = Callable[[str, Dict[str, Any], str], None]


class FuturesWS:
    """Minimal futures (linear) WebSocket wrapper for 5m klines and tickers."""

    def __init__(self, testnet: bool, config: Any | None = None) -> None:
        self.testnet = testnet
        self.logger = get_logger(self.__class__.__name__)
        self._ws = None
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._monitor: Optional[threading.Thread] = None
        self._on_kline: Optional[KlineCallback] = None
        self._symbols: List[str] = []
        # Sharded sockets to avoid server/topic limits
        self._sockets: List[object] = []
        # Be conservative with topics per socket to avoid throttling/timeouts
        # Allow tuning via config
        try:
            self._shard_size = int(getattr(config, "scalp_wss_shard_size", 20))
        except Exception:
            self._shard_size = 20
        try:
            sub_delay_ms = int(getattr(config, "scalp_wss_sub_delay_ms", 600))
        except Exception:
            sub_delay_ms = 600
        try:
            shard_delay_ms = int(getattr(config, "scalp_wss_shard_delay_ms", 5000))
        except Exception:
            shard_delay_ms = 5000
        self._subscribe_delay_per_symbol = max(0.05, float(sub_delay_ms) / 1000.0)
        self._subscribe_delay_between_shards = max(0.5, float(shard_delay_ms) / 1000.0)
        self._last_kline_time: float = 0.0
        self._last_resubscribe_ts: float = 0.0
        self._subscribed_topics: set[str] = set()

        if WebSocket is not None:
            try:
                # Force mainnet regardless of input
                self._ws = WebSocket(testnet=False, channel_type="linear")
            except Exception as e:
                self.logger.error(f"FuturesWS init failed: {e}")

    def subscribe_kline_5m(self, symbols: List[str], on_kline: Optional[KlineCallback]) -> None:
        if WebSocket is None:
            self.logger.warning("FuturesWS not available; running in stub mode")
            return
        with self._lock:
            self._symbols = list(symbols)
            self._on_kline = on_kline
        try:
            self._create_sharded_sockets_and_subscribe(symbols, self._on_kline, interval="5")
            self._last_kline_time = time.time()  # avoid immediate resubscribe before first ticks
            if self._monitor is None:
                self._monitor = threading.Thread(target=self._monitor_loop, name="FuturesWSMonitor", daemon=True)
                self._monitor.start()
            self.logger.info(f"Subscribed {len(symbols)} futures klines (5m) across {len(self._sockets)} sockets")
        except Exception as e:
            self.logger.error(f"FuturesWS subscribe error: {e}")

    # Optional: subscribe generic kline interval for MTF (e.g., 15m)
    def subscribe_kline(self, symbols: List[str], interval: str, on_kline: Optional[KlineCallbackHTF]) -> None:
        if WebSocket is None:
            self.logger.warning("FuturesWS not available; running in stub mode (kline generic)")
            return
        if not callable(on_kline):
            return
        try:
            # Reuse existing sockets (append) to avoid tearing down 5m subscriptions
            self._create_sharded_sockets_and_subscribe(
                symbols,
                (lambda sym, k: on_kline(sym, k, interval) if callable(on_kline) else None),
                interval=interval,
                append=True,
            )
        except Exception as e:
            self.logger.error(f"FuturesWS subscribe_kline error: {e}")

    def _create_sharded_sockets_and_subscribe(self, symbols: List[str], on_kline: Optional[KlineCallback], interval: str = "5", append: bool = False) -> None:
        if WebSocket is None:
            raise RuntimeError("WebSocket API not available")
        # Close previous sockets only if not appending
        if not append:
            try:
                for ws in self._sockets:
                    try:
                        if hasattr(ws, "exit"):
                            ws.exit()
                    except Exception:
                        pass
            finally:
                self._sockets = []
                # Clear topic cache on full rebuild to avoid stale dedupe
                try:
                    self._subscribed_topics.clear()
                except Exception:
                    pass
        if not symbols and not append:
            self._sockets = [WebSocket(testnet=False, channel_type="linear")]
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
        if append and self._sockets:
            # Distribute new interval subscriptions across existing sockets round-robin
            sock_count = len(self._sockets)
            idx = 0
            for chunk in chunks:
                for sym in chunk:
                    topic_key = f"kline.{interval}.{sym.upper()}"
                    if topic_key in self._subscribed_topics:
                        continue
                    ws = self._sockets[idx % sock_count]
                    try:
                        if interval == "5":
                            ws.kline_stream(callback=(lambda msg, s=sym: self._on_raw_kline(msg, s)), symbol=sym, interval="5")
                        else:
                            ws.kline_stream(callback=(lambda msg, s=sym: self._on_raw_kline_generic(msg, s, on_kline, interval)), symbol=sym, interval=interval)
                        self._subscribed_topics.add(topic_key)
                    except Exception as e:
                        self.logger.error(f"Subscribe kline {interval} failed {sym} on shard {idx % sock_count}: {e}")
                    idx += 1
                    time.sleep(self._subscribe_delay_per_symbol)
        else:
            # Create new sockets for these subscriptions
            for idx, chunk in enumerate(chunks):
                ws = WebSocket(testnet=False, channel_type="linear")
                self._sockets.append(ws)
                if idx > 0:
                    time.sleep(self._subscribe_delay_between_shards)
                for sym in chunk:
                    topic_key = f"kline.{interval}.{sym.upper()}"
                    if topic_key in self._subscribed_topics:
                        continue
                    try:
                        if interval == "5":
                            ws.kline_stream(callback=(lambda msg, s=sym: self._on_raw_kline(msg, s)), symbol=sym, interval="5")
                        else:
                            ws.kline_stream(callback=(lambda msg, s=sym: self._on_raw_kline_generic(msg, s, on_kline, interval)), symbol=sym, interval=interval)
                        self._subscribed_topics.add(topic_key)
                    except Exception as e:
                        self.logger.error(f"Subscribe kline {interval} failed {sym} on shard {idx}: {e}")
                    time.sleep(self._subscribe_delay_per_symbol)

    def _on_raw_kline_generic(self, message, symbol: str, on_kline: Optional[KlineCallback], interval: str) -> None:
        if not callable(on_kline):
            return
        try:
            k = self._extract_kline(message)
            if k:
                on_kline(symbol, k)
        except Exception as e:
            self.logger.debug(f"kline generic parse error {symbol}: {e}")

    @staticmethod
    def _extract_kline(message) -> Dict[str, Any]:
        try:
            if not isinstance(message, dict):
                return {}
            container = message.get("data") or message.get("result") or message
            rows: List[Dict[str, Any]] = []
            if isinstance(container, list):
                rows = container
            elif isinstance(container, dict):
                rows = container.get("list") or container.get("kline") or container.get("data") or []  # type: ignore
            if isinstance(rows, list) and rows:
                row = rows[-1]
                def _f(v):
                    try:
                        return float(v)
                    except Exception:
                        return None
                # Support dict and list row formats
                if isinstance(row, dict):
                    start_v = _f(row.get("start") or row.get("t") or row.get("startTime"))
                    end_v = _f(row.get("end") or row.get("T") or row.get("endTime"))
                    open_v = _f(row.get("open") or row.get("o"))
                    high_v = _f(row.get("high") or row.get("h"))
                    low_v = _f(row.get("low") or row.get("l"))
                    close_v = _f(row.get("close") or row.get("c"))
                    vol_v = _f(row.get("volume") or row.get("v") or row.get("turnover"))
                    confirm_raw = row.get("confirm") or row.get("isClosed")
                    confirm_v = False
                    if confirm_raw is not None:
                        try:
                            if isinstance(confirm_raw, bool):
                                confirm_v = confirm_raw
                            else:
                                confirm_v = str(confirm_raw).strip().lower() in {"1", "true", "yes"}
                        except Exception:
                            confirm_v = False
                    k = {
                        "start": start_v,
                        "end": end_v,
                        "open": open_v,
                        "high": high_v,
                        "low": low_v,
                        "close": close_v,
                        "volume": vol_v,
                        "confirm": confirm_v,
                    }
                elif isinstance(row, (list, tuple)):
                    # Try to map by common Bybit array order: [start, open, high, low, close, volume, ..., confirm?]
                    arr = list(row)
                    start_v = _f(arr[0] if len(arr) > 0 else None)
                    open_v = _f(arr[1] if len(arr) > 1 else None)
                    high_v = _f(arr[2] if len(arr) > 2 else None)
                    low_v = _f(arr[3] if len(arr) > 3 else None)
                    close_v = _f(arr[4] if len(arr) > 4 else None)
                    vol_v = _f(arr[5] if len(arr) > 5 else None)
                    # end may not be present in array rows; try to compute later
                    end_v = _f(arr[6]) if len(arr) > 6 else None
                    confirm_v = False
                    if len(arr) > 7:
                        try:
                            confirm_v = bool(arr[7])
                        except Exception:
                            confirm_v = False
                    k = {
                        "start": start_v,
                        "end": end_v,
                        "open": open_v,
                        "high": high_v,
                        "low": low_v,
                        "close": close_v,
                        "volume": vol_v,
                        "confirm": confirm_v,
                    }
                else:
                    return {}
                # Fallback confirm: if 'end' not provided by stream, consider bar closed when end <= now-2s
                try:
                    if not k.get("confirm"):
                        now_ms = time.time() * 1000.0
                        end_v = k.get("end") or 0.0
                        if end_v and (now_ms - float(end_v)) >= 2000.0:
                            k["confirm"] = True
                        else:
                            # If 'end' is missing, approximate expected end as start + 5m (default stream interval here)
                            st = k.get("start") or 0.0
                            if st:
                                expected_end = float(st) + 5 * 60 * 1000.0
                                if (now_ms - expected_end) >= 2000.0:
                                    k["confirm"] = True
                except Exception:
                    pass
                return k
        except Exception:
            return {}
        return {}

    def _on_raw_kline(self, message, symbol: str) -> None:
        cb = self._on_kline
        if not callable(cb):
            return
        try:
            k = self._extract_kline(message)
            if k:
                self._last_kline_time = time.time()
                cb(symbol, k)
        except Exception as e:
            self.logger.debug(f"kline parse error {symbol}: {e}")

    def _monitor_loop(self) -> None:
        while not self._stop.is_set():
            try:
                # If no klines received for > 90s, attempt resubscribe (debounced)
                now = time.time()
                # Increase inactivity threshold and debounce to avoid churn under load
                if (now - self._last_kline_time) > 150.0 and (now - self._last_resubscribe_ts) > 300.0:
                    with self._lock:
                        syms = list(self._symbols)
                        cb = self._on_kline
                    if syms:
                        try:
                            self._create_sharded_sockets_and_subscribe(syms, cb, interval="5")
                            self._last_kline_time = now
                            self._last_resubscribe_ts = now
                            self.logger.info("FuturesWS resubscribed due to inactivity")
                        except Exception as e:
                            self.logger.debug(f"Resubscribe error: {e}")
                time.sleep(2.0)
            except Exception:
                time.sleep(2.0)

    def stop(self) -> None:
        self._stop.set()
        # Close sockets on stop
        try:
            for ws in self._sockets:
                try:
                    if hasattr(ws, "exit"):
                        ws.exit()
                except Exception:
                    pass
        finally:
            self._sockets = []

