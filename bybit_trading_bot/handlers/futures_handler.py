from __future__ import annotations

from typing import Optional, Tuple, List, Dict
import time

from ..utils.logger import get_logger
from ..utils.http_client import RateLimitedHTTP

try:
    from pybit.unified_trading import HTTP
    # Optional: requests adapter tuning when pybit uses requests under the hood
    try:
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry  # type: ignore
    except Exception:
        requests = None  # type: ignore
        HTTPAdapter = None  # type: ignore
except Exception:  # pragma: no cover
    HTTP = None  # type: ignore


class FuturesHandler:
    def __init__(self, testnet: bool) -> None:
        self.logger = get_logger(self.__class__.__name__)
        self._raw_http = None
        self._http = None
        if HTTP is not None:
            try:
                # Force mainnet regardless of input
                # If requests is available, increase pool size for the default session used by pybit
                if 'requests' in globals() and requests is not None and HTTPAdapter is not None:
                    try:
                        session = requests.Session()
                        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=Retry(total=2, backoff_factor=0.1))
                        session.mount('https://', adapter)
                        session.mount('http://', adapter)
                        self._raw_http = HTTP(testnet=False, session=session)
                    except Exception:
                        self._raw_http = HTTP(testnet=False)
                else:
                    self._raw_http = HTTP(testnet=False)
                # Extra-tight rate limiting to avoid bursts during backfill
                self._http = RateLimitedHTTP(self._raw_http, max_requests=30, per_seconds=3.0)
            except Exception as e:
                self.logger.error(f"Failed to init HTTP for futures: {e}")

    def get_open_interest(self, symbol: str) -> Optional[float]:
        if self._http is None:
            return None
        try:
            resp = self._http.request("get_open_interest", category="linear", symbol=symbol, intervalTime="5min")
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            if not rows:
                return None
            latest = rows[-1]
            # Prefer monetary OI value; return both as tuple-like via encoding string? No: return mon value here.
            oi_val = latest.get("openInterestValue") or latest.get("open_interest_value")
            if oi_val is not None:
                return float(oi_val)
            oi_contracts = latest.get("openInterest") or latest.get("open_interest")
            return float(oi_contracts) if oi_contracts is not None else None
        except Exception as e:
            self.logger.error(f"Failed to fetch OI for {symbol}: {e}")
            return None

    def get_open_interest_contracts(self, symbol: str) -> Optional[float]:
        if self._http is None:
            return None
        try:
            resp = self._http.request("get_open_interest", category="linear", symbol=symbol, intervalTime="5min")
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            if not rows:
                return None
            latest = rows[-1]
            oi_contracts = latest.get("openInterest") or latest.get("open_interest")
            return float(oi_contracts) if oi_contracts is not None else None
        except Exception as e:
            self.logger.error(f"Failed to fetch OI (contracts) for {symbol}: {e}")
            return None

    def get_mark_price(self, symbol: str) -> Optional[float]:
        if self._http is None:
            return None
        try:
            resp = self._http.request("get_tickers", category="linear", symbol=symbol)
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            if not rows:
                return None
            mp = rows[0].get("markPrice") or rows[0].get("lastPrice")
            return float(mp) if mp is not None else None
        except Exception as e:
            self.logger.error(f"Failed to fetch OI/price for {symbol}: {e}")
            return None 

    def get_best_bid_ask(self, symbol: str) -> Optional[Tuple[float, float]]:
        if self._http is None:
            return None
        try:
            resp = self._http.request("get_orderbook", category="linear", symbol=symbol)
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            bids = (result.get("b") or result.get("bid") or result.get("bids") or [])
            asks = (result.get("a") or result.get("ask") or result.get("asks") or [])
            best_bid = None
            best_ask = None
            if isinstance(bids, list) and bids:
                row = bids[0]
                best_bid = float(row[0]) if isinstance(row, (list, tuple)) else float(row.get("price"))
            if isinstance(asks, list) and asks:
                row = asks[0]
                best_ask = float(row[0]) if isinstance(row, (list, tuple)) else float(row.get("price"))
            if best_bid is not None and best_ask is not None:
                return best_bid, best_ask
            return None
        except Exception as e:
            self.logger.error(f"Failed to fetch best bid/ask for {symbol}: {e}")
            return None

    def get_klines(self, symbol: str, interval: str = "5", limit: int = 200, start_ms: Optional[int] = None) -> List[Dict]:
        """Fetch klines via REST for backfill. Optional start_ms to page older history.

        Returns list of dicts with o/h/l/c/v and start timestamp (ascending by start).
        """
        if self._http is None:
            return []
        try:
            params = {"category": "linear", "symbol": symbol, "interval": interval, "limit": limit}
            if start_ms is not None:
                params["start"] = int(start_ms)
            resp = self._http.request("get_kline", **params)
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            out: List[Dict] = []
            for r in rows:
                try:
                    if isinstance(r, dict):
                        out.append({
                            "start": float(r.get("start")),
                            "open": float(r.get("open")),
                            "high": float(r.get("high")),
                            "low": float(r.get("low")),
                            "close": float(r.get("close")),
                            "volume": float(r.get("volume") or r.get("turnover") or 0.0),
                            "confirm": True,
                        })
                    elif isinstance(r, (list, tuple)) and len(r) >= 6:
                        # Bybit sometimes returns kline rows as arrays: [start, open, high, low, close, volume, ...]
                        start_v = float(r[0])
                        open_v = float(r[1])
                        high_v = float(r[2])
                        low_v = float(r[3])
                        close_v = float(r[4])
                        volume_v = float(r[5])
                        out.append({
                            "start": start_v,
                            "open": open_v,
                            "high": high_v,
                            "low": low_v,
                            "close": close_v,
                            "volume": volume_v,
                            "confirm": True,
                        })
                    else:
                        continue
                except Exception:
                    continue
            # API returns newest first sometimes; sort by start asc
            try:
                out.sort(key=lambda x: x.get("start", 0))
            except Exception:
                pass
            return out
        except Exception as e:
            self.logger.error(f"Failed to fetch klines for {symbol}: {e}")
            return []

    # ---- paged history helper ----
    def get_klines_paged(
        self,
        symbol: str,
        interval: str,
        min_bars: int,
        window_bars: int = 500,
        max_windows: int = 8,
    ) -> List[Dict]:
        """Accumulate at least min_bars klines by paging backwards in time.

        Returns ascending-by-start list when available; best-effort empty on failures.
        """
        if min_bars <= 0:
            return []
        acc: List[Dict] = []
        try:
            # Determine milliseconds per bar for the interval
            mul = 60_000
            if str(interval) in {"1", "3", "5", "15", "30"}:
                mins = int(interval)
                bar_ms = mins * mul
            elif str(interval) == "60":
                bar_ms = 60 * mul
            elif str(interval) == "120":
                bar_ms = 120 * mul
            elif str(interval) == "240":
                bar_ms = 240 * mul
            elif str(interval) == "D":
                bar_ms = 1440 * mul
            else:
                bar_ms = 5 * mul
            now_ms = int(time.time() * 1000)
            # First try most-recent window
            first = self.get_klines(symbol, interval=interval, limit=min(window_bars, 1000))
            if first:
                acc.extend(first)
            # Page older windows until enough
            i = 1
            while len(acc) < min_bars and i <= max_windows:
                start_ms = max(0, now_ms - i * bar_ms * window_bars)
                older = self.get_klines(symbol, interval=interval, limit=window_bars, start_ms=start_ms)
                if older:
                    # Prepend older rows
                    acc = older + acc
                i += 1
            # Deduplicate by start and sort ascending
            seen = set()
            unique: List[Dict] = []
            for r in acc:
                st = r.get("start")
                if st in seen:
                    continue
                seen.add(st)
                unique.append(r)
            try:
                unique.sort(key=lambda x: x.get("start", 0))
            except Exception:
                pass
            # Trim to most recent min_bars if oversized
            if len(unique) > min_bars:
                unique = unique[-min_bars:]
            return unique
        except Exception as e:
            self.logger.error(f"get_klines_paged error {symbol}: {e}")
            return []