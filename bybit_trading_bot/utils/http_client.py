from __future__ import annotations

import random
import threading
import time
from typing import Any, Callable, Optional

from bybit_trading_bot.utils.logger import get_logger


class RateLimitedHTTP:
    """Thin wrapper over pybit HTTP client with token-bucket rate limiting and backoff.

    - Limits average request rate (token bucket)
    - Retries on transient/rate-limit errors with exponential backoff and jitter
    - Logs rate events
    """

    def __init__(
        self,
        http_client: Any,
        max_requests: int = 120,
        per_seconds: float = 1.0,
        max_retries: int = 5,
        base_backoff: float = 0.2,
        backoff_cap: float = 3.0,
    ) -> None:
        self.http = http_client
        self.logger = get_logger(self.__class__.__name__)

        # Token bucket
        self.capacity = float(max_requests)
        self.tokens = float(max_requests)
        self.refill_rate = float(max_requests) / float(per_seconds)
        self.last_refill = time.time()
        self._lock = threading.Lock()

        # Backoff
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.backoff_cap = backoff_cap

    # ---- Rate limiting ----
    def _refill(self) -> None:
        now = time.time()
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        self.tokens = min(self.capacity, self.tokens + elapsed * (self.refill_rate))
        self.last_refill = now

    def _acquire_token(self) -> None:
        while True:
            with self._lock:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                # Not enough tokens; compute sleep until 1 token available
                needed = 1.0 - self.tokens
                sleep_time = max(needed / self.refill_rate, 0.005)
            time.sleep(sleep_time)

    # ---- Backoff + request ----
    def _should_retry(self, resp: Any, err: Optional[Exception]) -> bool:
        if err is not None:
            msg = str(err).lower()
            return (
                "429" in msg
                or "rate" in msg
                or "limit" in msg
                or "timeout" in msg
                or "temporarily" in msg
                or "timestamp" in msg  # Retry on timestamp errors
                or "recv_window" in msg  # Retry on recv_window errors
            )
        if isinstance(resp, dict):
            code = resp.get("retCode")
            if code is None:
                return False
            try:
                code = int(code)
            except Exception:
                return False
            # Retry on timestamp errors (10002) and other transient errors
            return code != 0 and code in [10002, 10003, 10004]  # Common timestamp/server errors
        return False

    def request(self, method_name: str, **kwargs) -> Any:
        attempt = 0
        while True:
            self._acquire_token()
            err: Optional[Exception] = None
            resp = None
            try:
                method: Callable[..., Any] = getattr(self.http, method_name)
                resp = method(**kwargs)
            except Exception as e:  # pybit FailedRequestError or network issues
                err = e

            # Success
            if err is None and isinstance(resp, dict) and int(resp.get("retCode", -1)) == 0:
                return resp

            # Retry?
            if attempt >= self.max_retries or not self._should_retry(resp, err):
                if err is not None:
                    raise err
                return resp

            # Backoff with jitter - increased base backoff for timestamp errors
            base_backoff = self.base_backoff
            if isinstance(resp, dict) and resp.get("retCode") == 10002:
                base_backoff = 1.0  # Longer base backoff for timestamp errors
            
            backoff = min(self.backoff_cap, base_backoff * (2 ** attempt))
            backoff *= 0.5 + random.random()  # jitter 0.5x..1.5x
            self.logger.warning(
                f"Rate/backoff: method={method_name} attempt={attempt+1} sleeping={backoff:.2f}s"
            )
            time.sleep(backoff)
            attempt += 1 