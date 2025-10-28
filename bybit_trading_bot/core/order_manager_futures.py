from __future__ import annotations

from typing import Dict, Optional
from decimal import Decimal, ROUND_DOWN, InvalidOperation

from ..config.settings import Config
from ..utils.logger import get_logger
from ..utils.http_client import RateLimitedHTTP

try:
    from pybit.unified_trading import HTTP
    try:
        import requests
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry  # type: ignore
    except Exception:
        requests = None  # type: ignore
        HTTPAdapter = None  # type: ignore
except Exception:  # pragma: no cover
    HTTP = None  # type: ignore


class FuturesOrderManager:
    """Bybit USDT-M futures order manager with basic sizing and SL/TP helpers."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self._raw_http = None
        self._http = None
        self._instrument_cache: Dict[str, Dict[str, Decimal | str]] = {}
        if HTTP is not None:
            try:
                if 'requests' in globals() and requests is not None and HTTPAdapter is not None:
                    try:
                        session = requests.Session()
                        adapter = HTTPAdapter(pool_connections=50, pool_maxsize=100, max_retries=Retry(total=2, backoff_factor=0.1))
                        session.mount('https://', adapter)
                        session.mount('http://', adapter)
                        self._raw_http = HTTP(
                            testnet=False,
                            api_key=self.config.bybit_api_key or "",
                            api_secret=self.config.bybit_api_secret or "",
                            recv_window=30000,
                            session=session,
                        )
                    except Exception:
                        self._raw_http = HTTP(
                            testnet=False,
                            api_key=self.config.bybit_api_key or "",
                            api_secret=self.config.bybit_api_secret or "",
                            recv_window=30000,
                        )
                else:
                    self._raw_http = HTTP(
                        testnet=False,
                        api_key=self.config.bybit_api_key or "",
                        api_secret=self.config.bybit_api_secret or "",
                        recv_window=30000,
                    )
                self._http = RateLimitedHTTP(self._raw_http, max_requests=30, per_seconds=3.0)
            except Exception as e:
                self.logger.error(f"Failed to init Bybit HTTP futures client: {e}")
        # Cache for symbol leverage (best-effort)
        self._last_leverage_set: Dict[str, float] = {}

    # ---- instrument filters ----
    def _format_decimal(self, value: Decimal, step: Decimal) -> str:
        try:
            q = value.quantize(step, rounding=ROUND_DOWN)
        except (InvalidOperation, ValueError):
            exponent = Decimal(str(step)).normalize().as_tuple().exponent
            q = value.quantize(Decimal((0, (1,), exponent)), rounding=ROUND_DOWN)
        s = format(q, 'f')
        return s

    def _get_filters(self, symbol: str) -> Dict[str, Decimal | str]:
        if symbol in self._instrument_cache:
            return self._instrument_cache[symbol]
        filters: Dict[str, Decimal | str] = {
            "qty_step": Decimal("0.001"),
            "min_qty": Decimal("0.0"),
            "tick_size": Decimal("0.5"),
            "min_notional": Decimal("0.0"),
        }
        if self._http is None:
            self._instrument_cache[symbol] = filters
            return filters
        try:
            resp = self._http.request("get_instruments_info", category="linear", symbol=symbol)
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            lst = result.get("list", []) if isinstance(result, dict) else []
            if lst:
                it = lst[0]
                lot = it.get("lotSizeFilter", {})
                price = it.get("priceFilter", {})
                if lot.get("qtyStep"):
                    filters["qty_step"] = Decimal(str(lot.get("qtyStep")))
                if lot.get("minOrderQty"):
                    filters["min_qty"] = Decimal(str(lot.get("minOrderQty")))
                # some schemas expose min notional as minOrderAmt or minOrderValue
                if lot.get("minOrderAmt"):
                    filters["min_notional"] = Decimal(str(lot.get("minOrderAmt")))
                if lot.get("minOrderValue"):
                    try:
                        mv = Decimal(str(lot.get("minOrderValue")))
                        if mv > 0:
                            filters["min_notional"] = mv
                    except Exception:
                        pass
                if price.get("tickSize"):
                    filters["tick_size"] = Decimal(str(price.get("tickSize")))
        except Exception as e:
            self.logger.debug(f"Failed to fetch futures instrument filters {symbol}: {e}")
        self._instrument_cache[symbol] = filters
        return filters

    def _format_price(self, symbol: str, price: float) -> str:
        filters = self._get_filters(symbol)
        return self._format_decimal(Decimal(str(price)), filters["tick_size"])  # type: ignore[arg-type]

    def _format_qty(self, symbol: str, qty: float) -> str:
        filters = self._get_filters(symbol)
        q = Decimal(str(max(0.0, qty)))
        if q < filters["min_qty"]:  # type: ignore[operator]
            q = filters["min_qty"]  # type: ignore[index]
        return self._format_decimal(q, filters["qty_step"])  # type: ignore[arg-type]

    def get_qty_step(self, symbol: str) -> float:
        filters = self._get_filters(symbol)
        try:
            return float(filters["qty_step"])  # type: ignore[index]
        except Exception:
            return 0.001

    def snap_down_to_step(self, symbol: str, qty_float: float) -> float:
        """Floor qty to the exchange step without raising to min_qty (for reduce-only partials)."""
        filters = self._get_filters(symbol)
        step = filters["qty_step"]  # type: ignore[assignment]
        try:
            q = Decimal(str(max(0.0, qty_float)))
            steps = (q / step).to_integral_value(rounding=ROUND_DOWN)
            q = steps * step
            return float(self._format_decimal(q, step))  # type: ignore[arg-type]
        except Exception:
            return max(0.0, qty_float)

    def ensure_min_notional(self, symbol: str, qty_float: float, price_float: float) -> float:
        """Increase qty to satisfy exchange min notional for the symbol at given price.

        Returns adjusted qty (floored to step), not smaller than min_qty, and meeting min_notional if defined.
        """
        if price_float <= 0.0:
            return max(0.0, qty_float)
        filters = self._get_filters(symbol)
        step = filters["qty_step"]  # type: ignore[assignment]
        min_qty = filters["min_qty"]  # type: ignore[assignment]
        min_notional = filters.get("min_notional", Decimal("0"))  # type: ignore[assignment]
        qty = Decimal(str(max(0.0, qty_float)))
        price = Decimal(str(price_float))
        # floor to step first
        try:
            from decimal import ROUND_DOWN
            steps = (qty / step).to_integral_value(rounding=ROUND_DOWN)
            qty = steps * step
        except Exception:
            pass
        if qty < min_qty:
            qty = min_qty  # type: ignore[assignment]
        if min_notional > 0:
            # compute needed qty to satisfy min_notional at this price
            try:
                needed = (min_notional / price)
                steps_needed = (needed / step).to_integral_value(rounding=ROUND_DOWN)
                if (steps_needed * step) < needed:
                    steps_needed = steps_needed + 1
                candidate = steps_needed * step
                if candidate > qty:
                    qty = candidate
            except Exception:
                pass
        try:
            return float(self._format_decimal(qty, step))
        except Exception:
            return float(qty)

    # ---- sizing ----
    def calc_risk_based_qty(self, symbol: str, entry_price: float, stop_price: float, equity_usdt: float) -> float:
        """Qty = (risk_usdt) / (|entry - stop|) with leverage applied implicitly by Bybit.

        Caps to non-negative and respects min qty. Caller may further cap by leverage/margin.
        """
        risk_pct = max(0.0, float(self.config.scalp_risk_pct)) / 100.0
        risk_usdt = max(0.0, equity_usdt * risk_pct)
        dist = abs(entry_price - stop_price)
        if entry_price <= 0.0 or dist <= 0.0:
            return 0.0
        qty = risk_usdt / dist
        # Optional: scale by (1/leverage) if desired; Bybit margin will handle leverage execution.
        return max(0.0, qty)

    # ---- orders ----
    def place_market(self, symbol: str, side: str, qty: float, reduce_only: bool = False) -> Optional[Dict]:
        if self._http is None:
            fake = {"orderId": f"DEV-{symbol}-{side}-{qty}", "symbol": symbol}
            self.logger.info(f"(DEV) Futures market {side} {symbol} qty={qty}")
            return fake
        try:
            # Best-effort set leverage once per symbol
            try:
                lev = float(self.config.scalp_leverage)
                if lev > 0 and symbol not in self._last_leverage_set:
                    _ = self._http.request(
                        "set_leverage", category="linear", symbol=symbol, buyLeverage=str(lev), sellLeverage=str(lev)
                    )
                    self._last_leverage_set[symbol] = lev
            except Exception:
                pass
            qty_str = self._format_qty(symbol, qty)
            resp = self._http.request(
                "place_order",
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Market",
                qty=qty_str,
                reduceOnly=reduce_only,
            )
            if int(resp.get("retCode", -1)) != 0:
                self.logger.error(f"Futures market order failed {symbol}: {resp}")
                return None
            return resp
        except Exception as e:
            self.logger.error(f"place_market error {symbol}: {e}")
            return None

    def place_tp_sl(self, symbol: str, position_side: str, take_profit: float | None, stop_loss: float | None) -> bool:
        if self._http is None:
            self.logger.info(f"(DEV) Futures set TP/SL {symbol} tp={take_profit} sl={stop_loss}")
            return True
        ok = True
        try:
            if take_profit is not None:
                tp = self._http.request(
                    "set_trading_stop",
                    category="linear",
                    symbol=symbol,
                    takeProfit=self._format_price(symbol, take_profit),
                    positionIdx=0,
                )
                ok = ok and int(tp.get("retCode", -1)) == 0
            if stop_loss is not None:
                sl = self._http.request(
                    "set_trading_stop",
                    category="linear",
                    symbol=symbol,
                    stopLoss=self._format_price(symbol, stop_loss),
                    positionIdx=0,
                )
                ok = ok and int(sl.get("retCode", -1)) == 0
        except Exception as e:
            self.logger.error(f"place_tp_sl error {symbol}: {e}")
            return False
        return ok

    def place_reduce_only_limit(self, symbol: str, side: str, qty: float, price: float) -> Optional[Dict]:
        if self._http is None:
            self.logger.info(f"(DEV) Futures reduce-only limit {side} {symbol} qty={qty} price={price}")
            return {"orderId": f"DEV-RO-{symbol}-{side}-{qty}-{price}"}
        try:
            qty_str = self._format_qty(symbol, qty)
            price_str = self._format_price(symbol, price)
            resp = self._http.request(
                "place_order",
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=qty_str,
                price=price_str,
                reduceOnly=True,
            )
            if int(resp.get("retCode", -1)) != 0:
                self.logger.error(f"Reduce-only limit failed {symbol}: {resp}")
                return None
            return resp
        except Exception as e:
            self.logger.error(f"place_reduce_only_limit error {symbol}: {e}")
            return None

    def place_limit(self, symbol: str, side: str, qty: float, price: float) -> Optional[Dict]:
        if self._http is None:
            self.logger.info(f"(DEV) Futures limit {side} {symbol} qty={qty} price={price}")
            return {"orderId": f"DEV-LIM-{symbol}-{side}-{qty}-{price}"}
        try:
            qty_str = self._format_qty(symbol, qty)
            price_str = self._format_price(symbol, price)
            resp = self._http.request(
                "place_order",
                category="linear",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=qty_str,
                price=price_str,
            )
            if int(resp.get("retCode", -1)) != 0:
                self.logger.error(f"Limit order failed {symbol}: {resp}")
                return None
            return resp
        except Exception as e:
            self.logger.error(f"place_limit error {symbol}: {e}")
            return None

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        if self._http is None:
            return True
        try:
            resp = self._http.request("cancel_order", category="linear", symbol=symbol, orderId=order_id)
            return int(resp.get("retCode", -1)) == 0
        except Exception as e:
            self.logger.error(f"cancel_order error {symbol}/{order_id}: {e}")
            return False


    # ---- queries (status/positions) ----
    def get_open_orders(self, symbol: str) -> Optional[Dict]:
        """Return Bybit open orders response for symbol (or None in stub/error)."""
        if self._http is None:
            return None
        try:
            resp = self._http.request("get_open_orders", category="linear", symbol=symbol)
            return resp if isinstance(resp, dict) else None
        except Exception as e:
            self.logger.error(f"get_open_orders error {symbol}: {e}")
            return None

    def get_order_history(self, symbol: str, order_id: str) -> Optional[Dict]:
        """Fetch a single order's history record when available."""
        if self._http is None:
            return {"orderId": order_id, "orderStatus": "Filled"}
        try:
            resp = self._http.request("get_order_history", category="linear", symbol=symbol, orderId=order_id)
            return resp if isinstance(resp, dict) else None
        except Exception as e:
            self.logger.debug(f"get_order_history error {symbol}/{order_id}: {e}")
            return None

    def get_position(self, symbol: str) -> Optional[Dict]:
        """Return current net position info for symbol (best effort).

        Expected useful fields when available:
        - size (float), avgPrice (float), side ("Buy"/"Sell")
        """
        if self._http is None:
            return None
        try:
            resp = self._http.request("get_positions", category="linear", symbol=symbol)
            if not isinstance(resp, dict):
                return None
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            if not isinstance(rows, list) or not rows:
                return {"size": 0.0}
            # Prefer net position if provided, otherwise infer from long/short
            # Unified response often includes entries for both sides; pick the one with non-zero size
            chosen = None
            for r in rows:
                try:
                    sz = float(r.get("size") or r.get("positionAmt") or 0.0)
                except Exception:
                    sz = 0.0
                if abs(sz) > 0:
                    chosen = r
                    break
            if chosen is None:
                chosen = rows[0]
            out = {
                "size": float(chosen.get("size") or chosen.get("positionAmt") or 0.0),
                "avgPrice": float(chosen.get("avgPrice") or chosen.get("entryPrice") or 0.0),
                "side": (str(chosen.get("side")) if chosen.get("side") else ("Buy" if float(chosen.get("size") or 0.0) > 0 else ("Sell" if float(chosen.get("size") or 0.0) < 0 else ""))),
            }
            return out
        except Exception as e:
            self.logger.error(f"get_position error {symbol}: {e}")
            return None

    def get_mark_price_safe(self, symbol: str) -> Optional[float]:
        """Lightweight mark price fetcher for PnL calculations if needed."""
        try:
            if self._http is None:
                return None
            resp = self._http.request("get_tickers", category="linear", symbol=symbol)
            result = resp.get("result", {}) if isinstance(resp, dict) else {}
            rows = result.get("list", []) if isinstance(result, dict) else []
            if not rows:
                return None
            mp = rows[0].get("markPrice") or rows[0].get("lastPrice")
            return float(mp) if mp is not None else None
        except Exception:
            return None


