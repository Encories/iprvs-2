from __future__ import annotations

from typing import Dict, Optional
from decimal import Decimal, ROUND_DOWN, InvalidOperation
import time

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.http_client import RateLimitedHTTP

try:
    from pybit.unified_trading import HTTP
except Exception:  # pragma: no cover
    HTTP = None  # type: ignore


class OrderManager:
    """Управление торговыми ордерами (с валидацией ответов и нормализацией количества)."""

    def __init__(self, config: Config, db: DBManager) -> None:
        self.config = config
        self.db = db
        self.logger = get_logger(self.__class__.__name__)
        self._raw_http = None
        self._http = None
        self._symbol_cache: Dict[str, Dict[str, Decimal | str]] = {}
        if HTTP is not None:
            try:
                self._raw_http = HTTP(
                    testnet=self.config.bybit_testnet,
                    api_key=self.config.bybit_api_key or "",
                    api_secret=self.config.bybit_api_secret or "",
                    recv_window=30000,  # Increased to 30 seconds
                )
                self._http = RateLimitedHTTP(self._raw_http, max_requests=90, per_seconds=3.0)
                
                # Sync time with server before first request
                self._sync_server_time()
            except Exception as e:
                self.logger.error(f"Failed to init Bybit HTTP trading client: {e}")

    def _sync_server_time(self) -> None:
        """Sync local time with Bybit server to avoid timestamp errors."""
        try:
            # If cancellations are disabled and there are open orders, skip close to avoid reserved balance errors
            try:
                if self._http is not None and not getattr(self.config, "cancel_orders_enabled", True):
                    resp_check_open = self._http.request("get_open_orders", category="spot", symbol=symbol)
                    if int(resp_check_open.get("retCode", -1)) == 0:
                        rows_open = (resp_check_open.get("result", {}) or {}).get("list", [])
                        if rows_open:
                            self.logger.info(f"Cancellation disabled and {len(rows_open)} open orders present; skipping market close for {symbol}")
                            return None
            except Exception:
                pass
            import requests
            import json
            
            # Get server time from Bybit
            url = "https://api.bybit.com/v5/market/time"
            if self.config.bybit_testnet:
                url = "https://api-testnet.bybit.com/v5/market/time"
            
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                server_data = response.json()
                server_time = int(server_data.get("result", {}).get("timeSecond", 0)) * 1000
                local_time = int(time.time() * 1000)
                
                time_diff = server_time - local_time
                self.logger.info(f"Server time sync: diff={time_diff}ms")
                
                # If difference is significant, we could adjust, but pybit handles this internally
                if abs(time_diff) > 5000:  # More than 5 seconds difference
                    self.logger.warning(f"Large time difference detected: {time_diff}ms")
                    
        except Exception as e:
            self.logger.debug(f"Server time sync failed: {e}")
            # Not critical, continue without sync

    def _get_symbol_filters(self, symbol: str) -> Dict[str, Decimal | str]:
        if symbol in self._symbol_cache:
            return self._symbol_cache[symbol]
        filters: Dict[str, Decimal | str] = {
            "qty_step": Decimal("0.000001"),
            "min_qty": Decimal("0.0"),
            "min_notional": Decimal("0.0"),
            "tick_size": Decimal("0.01"),
            "quote_step": Decimal("0.00000001"),
            "base_coin": "",
        }
        if self._http is None:
            self._symbol_cache[symbol] = filters
            return filters
        try:
            info = self._http.request("get_instruments_info", category="spot", symbol=symbol)
            result = info.get("result", {}) if isinstance(info, dict) else {}
            items = result.get("list", []) if isinstance(result, dict) else []
            if items:
                it = items[0]
                lot = it.get("lotSizeFilter", {})
                price_filter = it.get("priceFilter", {})
                if lot.get("basePrecision"):
                    filters["qty_step"] = Decimal(str(lot.get("basePrecision")))
                elif lot.get("qtyStep"):
                    filters["qty_step"] = Decimal(str(lot.get("qtyStep")))
                if lot.get("quotePrecision"):
                    filters["quote_step"] = Decimal(str(lot.get("quotePrecision")))
                if lot.get("minOrderQty"):
                    filters["min_qty"] = Decimal(str(lot.get("minOrderQty")))
                if lot.get("minOrderAmt"):
                    filters["min_notional"] = Decimal(str(lot.get("minOrderAmt")))
                if price_filter.get("tickSize"):
                    filters["tick_size"] = Decimal(str(price_filter.get("tickSize")))
                if it.get("baseCoin"):
                    filters["base_coin"] = str(it.get("baseCoin"))
        except Exception as e:
            self.logger.debug(f"Failed to fetch symbol filters for {symbol}: {e}")
        self._symbol_cache[symbol] = filters
        return filters

    def _format_decimal(self, value: Decimal, step: Decimal) -> str:
        try:
            q = value.quantize(step, rounding=ROUND_DOWN)
        except (InvalidOperation, ValueError):
            exponent = Decimal(str(step)).normalize().as_tuple().exponent
            q = value.quantize(Decimal((0, (1,), exponent)), rounding=ROUND_DOWN)
        s = format(q, 'f')
        return s

    def _normalize_and_format_qty(self, symbol: str, qty_float: float, price_float: float | None) -> str:
        filters = self._get_symbol_filters(symbol)
        qty = Decimal(str(qty_float))
        price = Decimal(str(price_float)) if price_float is not None else None
        step = filters["qty_step"]  # type: ignore[assignment]
        if qty < filters["min_qty"]:  # type: ignore[operator]
            qty = filters["min_qty"]  # type: ignore[index]
        if price is not None and filters["min_notional"] > 0:  # type: ignore[operator]
            needed = (filters["min_notional"] / price)  # type: ignore[operator]
            steps_needed = (needed / step).to_integral_value(rounding=ROUND_DOWN)
            if (steps_needed * step) < needed:
                steps_needed = steps_needed + 1
            candidate = steps_needed * step
            if qty < candidate:
                qty = candidate
            if (price * qty) < filters["min_notional"]:  # type: ignore[operator]
                qty = qty + step
        return self._format_decimal(qty, step)

    def _format_quote_amount(self, symbol: str, notional_float: float) -> str:
        filters = self._get_symbol_filters(symbol)
        amt = Decimal(str(notional_float))
        quote_step = filters["quote_step"]  # type: ignore[assignment]
        return self._format_decimal(amt, quote_step)

    def _format_price(self, symbol: str, price_float: float) -> str:
        filters = self._get_symbol_filters(symbol)
        price = Decimal(str(price_float))
        tick = filters["tick_size"]  # type: ignore[assignment]
        return self._format_decimal(price, tick)

    def get_available_base_qty(self, symbol: str, max_wait_s: float = 6.0) -> float:
        """Poll wallet balance for available base qty; retry if zero/unavailable."""
        if self._http is None:
            return 0.0
        base_coin = str(self._get_symbol_filters(symbol).get("base_coin") or "")
        if not base_coin:
            return 0.0
        deadline = time.time() + max_wait_s
        last_qty = 0.0
        while time.time() < deadline:
            try:
                resp = self._http.request("get_wallet_balance", accountType="UNIFIED")
                result = resp.get("result", {}) if isinstance(resp, dict) else {}
                lst = result.get("list", []) if isinstance(result, dict) else []
                coins = lst[0].get("coin", []) if lst else []
                coin = next((c for c in coins if c.get("coin") == base_coin), None)
                if coin:
                    val = coin.get("availableToTrade") or coin.get("availableToWithdraw") or coin.get("walletBalance")
                    try:
                        last_qty = float(val)
                    except Exception:
                        last_qty = last_qty
                    if last_qty > 0.0:
                        break
            except Exception as e:
                self.logger.debug(f"get_available_base_qty error {symbol}: {e}")
            time.sleep(0.5)
        return max(0.0, last_qty)

    def adjust_qty_for_safety(self, symbol: str, qty_float: float) -> float:
        """Reduce qty by one step and snap to step; ensure >= min qty."""
        filters = self._get_symbol_filters(symbol)
        step = filters["qty_step"]  # type: ignore[assignment]
        min_qty = filters["min_qty"]  # type: ignore[assignment]
        qty = Decimal(str(max(0.0, qty_float)))
        # subtract one step for safety
        try:
            qty = qty - step  # type: ignore[operator]
        except Exception:
            pass
        if qty < min_qty:  # type: ignore[operator]
            qty = min_qty  # type: ignore[assignment]
        qty_str = self._format_decimal(qty, step)
        try:
            return float(qty_str)
        except Exception:
            return float(qty)

    def get_filled_base_qty(self, symbol: str, order_id: str, max_wait_s: float = 6.0) -> float:
        """Poll order history for filled base quantity; if not available yet, retry up to max_wait_s."""
        if self._http is None:
            return 0.0
        deadline = time.time() + max_wait_s
        last_qty = 0.0
        while time.time() < deadline:
            try:
                resp = self._http.request("get_order_history", category="spot", orderId=order_id)
                result = resp.get("result", {}) if isinstance(resp, dict) else {}
                rows = result.get("list", []) if isinstance(result, dict) else []
                if rows:
                    row = rows[0]
                    qty = row.get("cumExecQty") or row.get("cumExecBaseQty") or row.get("qty")
                    try:
                        last_qty = float(qty)
                    except Exception:
                        last_qty = last_qty
                    status = (row.get("orderStatus") or "").lower()
                    if last_qty > 0.0 and status in {"filled", "partially_filled", "partiallyfilled"}:
                        break
            except Exception as e:
                self.logger.debug(f"get_filled_base_qty error {symbol}/{order_id}: {e}")
            time.sleep(0.5)
        return max(0.0, last_qty)

    def calculate_position_size(self, symbol: str, current_price: float, account_equity_usdt: float) -> float:
        if current_price <= 0.0:
            return 0.0
        if self.config.trade_notional_usdt and self.config.trade_notional_usdt > 0.0:
            raw_qty = self.config.trade_notional_usdt / current_price
            return max(0.0, raw_qty)
        percent = max(0.0, float(self.config.max_position_size_percent)) / 100.0
        notional = account_equity_usdt * percent
        raw_qty = notional / current_price
        return max(0.0, raw_qty)

    def _is_success(self, resp: Dict) -> bool:
        if not isinstance(resp, dict):
            return False
        return int(resp.get("retCode", -1)) == 0

    def place_spot_order(self, symbol: str, side: str, quantity: float, take_profit: float, reference_price: float, stop_loss: Optional[float] = None) -> Optional[Dict]:
        try:
            filters = self._get_symbol_filters(symbol)
            if side.lower() == "buy":
                if self.config.spot_market_unit == "quote":
                    notional = self.config.trade_notional_usdt or float(Decimal(str(quantity)) * Decimal(str(reference_price)))
                    min_notional = float(filters["min_notional"]) if filters["min_notional"] > 0 else 0.0
                    if min_notional > 0 and notional < min_notional:
                        if self.config.skip_below_min_notional:
                            self.logger.warning(
                                f"Skipping order {symbol}: notional {notional} < minOrderAmt {min_notional} and SKIP_BELOW_MIN_NOTIONAL=True"
                            )
                            try:
                                self.db.set_symbol_active(symbol, False)
                                self.logger.info(f"Deactivated symbol {symbol} due to min notional skip")
                            except Exception as e:
                                self.logger.debug(f"Failed to deactivate {symbol}: {e}")
                            return None
                        notional = min_notional + float(filters["quote_step"])  # type: ignore[index]
                    qty_field = self._format_quote_amount(symbol, notional)
                    market_unit = "quoteCoin"
                else:
                    qty_field = self._normalize_and_format_qty(symbol, quantity, reference_price)
                    market_unit = None
            else:
                qty_field = self._normalize_and_format_qty(symbol, quantity, None)
                market_unit = None

            if self._http is None:
                order = {
                    "orderId": f"DEV-{symbol}-{side}-{qty_field}",
                    "symbol": symbol,
                    "side": side,
                    "qty": qty_field,
                }
                self.logger.info(
                    f"✅ ORDER PLACED: {symbol} - Qty: {qty_field}, TP target: {take_profit} (DEV)"
                )
                return order

            kwargs = dict(category="spot", symbol=symbol, side=side, orderType="Market", qty=qty_field)
            if market_unit:
                kwargs["marketUnit"] = market_unit
            # No preset SL is sent for spot marketUnit=quoteCoin.
            resp = self._http.request("place_order", **kwargs)
            if not self._is_success(resp):
                self.logger.error(f"❌ ORDER FAILED: {symbol} - API retCode={resp.get('retCode')} retMsg={resp.get('retMsg')}")
                return None
            self.logger.info(
                f"✅ ORDER PLACED: {symbol} - Qty: {qty_field}, TP target: {take_profit}{' (preset SL)' if stop_loss else ''}"
            )
            return resp
        except Exception as e:
            self.logger.error(f"❌ ORDER FAILED: {symbol} - Error: {e}")
            return None

    def place_tp_limit(self, symbol: str, quantity: float, tp_price: float) -> Optional[Dict]:
        try:
            # Ensure TP notional meets minOrderAmt; try to raise qty up to available balance
            filters = self._get_symbol_filters(symbol)
            step = filters["qty_step"]  # type: ignore[assignment]
            min_notional = float(filters["min_notional"]) if filters["min_notional"] > 0 else 0.0
            price_str = self._format_price(symbol, tp_price)
            # available base qty
            available = self.get_available_base_qty(symbol, max_wait_s=6.0)
            base_target = float(quantity)
            if available > 0.0:
                base_target = min(base_target, available)
            # snap down to step
            try:
                from decimal import Decimal
                base_dec = Decimal(str(base_target))
                step_dec = step  # type: ignore[assignment]
                # floor to step
                steps = (base_dec / step_dec).to_integral_value(rounding=ROUND_DOWN)
                qty_dec = steps * step_dec
            except Exception:
                qty_dec = Decimal(str(base_target))  # type: ignore[name-defined]
            # if subtracting one step for safety drops below min notional, we'll bump later
            try:
                # safety: minus one step if possible
                if qty_dec > step_dec:
                    qty_dec = qty_dec - step_dec
            except Exception:
                pass
            # compute needed qty for min_notional
            if min_notional > 0.0 and tp_price > 0.0:
                try:
                    needed = Decimal(str(min_notional)) / Decimal(str(tp_price))  # type: ignore[name-defined]
                    needed_steps = (needed / step_dec).to_integral_value(rounding=ROUND_DOWN)
                    if (needed_steps * step_dec) < needed:
                        needed_steps = needed_steps + 1
                    needed_qty = needed_steps * step_dec
                except Exception:
                    needed_qty = qty_dec
                # cap by available
                try:
                    if available > 0.0:
                        avail_steps = (Decimal(str(available)) / step_dec).to_integral_value(rounding=ROUND_DOWN)  # type: ignore[name-defined]
                        avail_qty = avail_steps * step_dec
                    else:
                        avail_qty = qty_dec
                except Exception:
                    avail_qty = qty_dec
                # choose final qty
                if needed_qty <= avail_qty and needed_qty > qty_dec:
                    qty_dec = needed_qty
                # final notional check
                try:
                    notional_ok = (Decimal(str(tp_price)) * qty_dec) >= Decimal(str(min_notional))  # type: ignore[name-defined]
                except Exception:
                    notional_ok = True
                if min_notional > 0.0 and not notional_ok:
                    self.logger.warning(
                        f"TP skip for {symbol}: notional below min (tp={tp_price}, qty={qty_dec}, minNotional={min_notional}, available={available})"
                    )
                    return None
            # format qty
            qty_str = self._format_decimal(qty_dec, step)  # type: ignore[arg-type]
            if self._http is None:
                order = {
                    "orderId": f"DEV-TP-{symbol}-{qty_str}",
                    "symbol": symbol,
                    "side": "Sell",
                    "qty": qty_str,
                    "price": price_str,
                }
                self.logger.info(f"Placed TP limit (DEV): {symbol} qty={qty_str} price={price_str}")
                return order
            kwargs = dict(
                category="spot",
                symbol=symbol,
                side="Sell",
                orderType="Limit",
                qty=qty_str,
                price=price_str,
                timeInForce="GTC",
            )
            if self.config.post_only_tp:
                kwargs["isPostOnly"] = True  # Bybit V5 supports isPostOnly flag
            resp = self._http.request("place_order", **kwargs)
            if not self._is_success(resp):
                self.logger.error(f"Failed to place TP limit for {symbol}: retCode={resp.get('retCode')} retMsg={resp.get('retMsg')}")
                return None
            self.logger.info(f"Placed TP limit: {symbol} qty={qty_str} price={price_str}")
            return resp
        except Exception as e:
            self.logger.error(f"Failed to place TP limit for {symbol}: {e}")
            return None

    # Exchange SL placement removed in favor of Software SL.

    # --- Split mode helpers ---
    def place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Optional[Dict]:
        """Place a spot limit order (GTC). Quantity is in base units."""
        try:
            qty_str = self._normalize_and_format_qty(symbol, quantity, price)
            price_str = self._format_price(symbol, price)
            if self._http is None:
                order = {
                    "orderId": f"DEV-LMT-{symbol}-{qty_str}-{price_str}",
                    "symbol": symbol,
                    "side": side,
                    "qty": qty_str,
                    "price": price_str,
                }
                self.logger.info(f"Placed limit (DEV): {symbol} {side} qty={qty_str} price={price_str}")
                return order
            resp = self._http.request(
                "place_order",
                category="spot",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=qty_str,
                price=price_str,
                timeInForce="GTC",
            )
            if not self._is_success(resp):
                self.logger.error(f"Failed to place limit for {symbol}: retCode={resp.get('retCode')} retMsg={resp.get('retMsg')}")
                return None
            self.logger.info(f"Placed limit: {symbol} {side} qty={qty_str} price={price_str}")
            return resp
        except Exception as e:
            self.logger.error(f"Failed to place limit for {symbol}: {e}")
            return None

    def wait_for_filled(self, order_id: str, timeout_s: float = 30.0) -> Optional[Dict]:
        """Poll order history until Filled or timeout. Returns history row dict if filled."""
        if self._http is None:
            return {"orderStatus": "Filled", "avgPrice": None}
        deadline = time.time() + timeout_s
        last_row: Optional[Dict] = None
        while time.time() < deadline:
            try:
                resp = self._http.request("get_order_history", category="spot", orderId=order_id)
                if int(resp.get("retCode", -1)) != 0:
                    time.sleep(0.5)
                    continue
                rows = (resp.get("result", {}) or {}).get("list", [])
                if rows:
                    row = rows[0]
                    last_row = row
                    status = str(row.get("orderStatus") or "").lower()
                    if status == "filled":
                        return row
                time.sleep(0.5)
            except Exception:
                time.sleep(0.5)
        return last_row

    def get_order_fill_row(self, order_id: str) -> Optional[Dict]:
        """Single request to fetch order history row by id; returns row dict if exists."""
        if self._http is None:
            return None
        try:
            resp = self._http.request("get_order_history", category="spot", orderId=order_id)
            if int(resp.get("retCode", -1)) != 0:
                return None
            rows = (resp.get("result", {}) or {}).get("list", [])
            if rows:
                return rows[0]
            return None
        except Exception:
            return None

    def get_order_fill_row(self, order_id: str, max_retries: int = 3) -> Optional[Dict]:
        """Fetch order history row by id with simple retry/backoff."""
        if self._http is None:
            return None
        for attempt in range(max_retries):
            try:
                resp = self._http.request("get_order_history", category="spot", orderId=order_id)
                if int(resp.get("retCode", -1)) != 0:
                    if attempt < max_retries - 1:
                        time.sleep(0.5 * (attempt + 1))
                        continue
                    return None
                rows = (resp.get("result", {}) or {}).get("list", [])
                return rows[0] if rows else None
            except Exception as e:
                self.logger.debug(f"Order history fetch attempt {attempt+1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(1.0 * (attempt + 1))
                    continue
                return None

    def sync_recent_orders(self) -> None:
        """Placeholder to avoid runtime errors; can be extended to sync fills in batch."""
        return

    def has_open_spot_order(self, symbol: str) -> bool:
        """Check via API if there are any open spot orders for the given symbol."""
        if self._http is None:
            return False
        try:
            resp = self._http.request("get_open_orders", category="spot", symbol=symbol)
            if not isinstance(resp, dict) or int(resp.get("retCode", -1)) != 0:
                return False
            rows = (resp.get("result", {}) or {}).get("list", [])
            return bool(rows)
        except Exception:
            return False

    def post_fill_tp_sl(self, symbol: str, filled_row: Dict, tp_pct: float, sl_pct: float) -> None:
        """After a buy fill, place TP limit (+pct). Exchange SL placement removed."""
        try:
            entry = filled_row.get("avgPrice") or filled_row.get("price")
            qty = filled_row.get("cumExecQty") or filled_row.get("qty")
            entry_f = float(entry)
            qty_f = float(qty)
            tp_price = entry_f * (1.0 + float(tp_pct))
            safe_qty = self.adjust_qty_for_safety(symbol, qty_f)
            self.place_tp_limit(symbol, safe_qty, tp_price)
        except Exception as e:
            self.logger.error(f"post_fill_tp_sl failed for {symbol}: {e}")

    def cancel_order(self, symbol: str, order_id: str) -> bool:
        """Cancel a spot order by orderId."""
        if self._http is None:
            self.logger.info(f"Cancelled (DEV): {symbol} orderId={order_id}")
            return True
        try:
            resp = self._http.request(
                "cancel_order",
                category="spot",
                symbol=symbol,
                orderId=order_id,
            )
            ok = self._is_success(resp)
            if not ok:
                self.logger.error(f"Failed to cancel {symbol} {order_id}: retCode={resp.get('retCode')} retMsg={resp.get('retMsg')}")
            else:
                self.logger.info(f"Cancelled order: {symbol} {order_id}")
            return ok
        except Exception as e:
            self.logger.error(f"Cancel order error {symbol} {order_id}: {e}")
            return False

    def close_position_market(self, symbol: str, quantity: float) -> Optional[Dict]:
        try:
            # 1) Optionally cancel ALL open orders for this symbol to free reserved balance
            try:
                if getattr(self.config, "cancel_orders_enabled", True) and self._http is not None:
                    resp_open = self._http.request("get_open_orders", category="spot", symbol=symbol)
                    if int(resp_open.get("retCode", -1)) == 0:
                        rows = (resp_open.get("result", {}) or {}).get("list", [])
                        if rows:
                            self.logger.info(f"Closing position: Found {len(rows)} open orders for {symbol}, cancelling all")
                            for order in rows:
                                order_id = str(order.get("orderId", ""))
                                if order_id:
                                    try:
                                        self.cancel_order(symbol, order_id)
                                        self.logger.debug(f"Cancelled order {order_id} for {symbol}")
                                    except Exception as e:
                                        self.logger.warning(f"Failed to cancel order {order_id} for {symbol}: {e}")
                            deadline = time.time() + 3.0
                            while time.time() < deadline:
                                try:
                                    resp_check = self._http.request("get_open_orders", category="spot", symbol=symbol)
                                    if int(resp_check.get("retCode", -1)) == 0:
                                        remaining = (resp_check.get("result", {}) or {}).get("list", [])
                                        if not remaining:
                                            self.logger.info(f"All orders cancelled for {symbol}")
                                            break
                                    time.sleep(0.2)
                                except Exception:
                                    pass
                            time.sleep(0.5)
                        else:
                            self.logger.debug(f"No open orders found for {symbol}")
                else:
                    self.logger.info(f"Cancellation disabled; skipping order cancellations for {symbol}")
            except Exception as e:
                self.logger.error(f"Error cancelling orders for {symbol}: {e}")

            time.sleep(0.25)

            # 2) Ensure notional >= minNotional; wait for wallet refresh after cancellations if needed
            # Fetch last price and filters
            last_price: float | None = None
            try:
                sym_id = next((rec.id for rec in self.db.get_active_symbols() if rec.spot_symbol == symbol), None)
            except Exception:
                sym_id = None
            if sym_id is not None:
                try:
                    p = self.db.get_last_price(sym_id)
                    if p and p > 0:
                        last_price = float(p)
                except Exception:
                    last_price = None
            filters = self._get_symbol_filters(symbol)
            step = filters["qty_step"]  # type: ignore[assignment]
            min_notional = float(filters["min_notional"]) if filters["min_notional"] > 0 else 0.0

            def _ceil_to_step(val: Decimal, st: Decimal) -> Decimal:
                steps = (val / st).to_integral_value(rounding=ROUND_DOWN)
                if (steps * st) < val:
                    steps = steps + 1
                return steps * st

            available = self.get_available_base_qty(symbol, max_wait_s=4.0)
            target_qty = float(quantity)
            if available > 0.0:
                target_qty = min(target_qty, available)

            # If we have minNotional and last_price, compute required minimum qty and wait up to 5s if needed
            if last_price and last_price > 0.0 and min_notional > 0.0:
                try:
                    required_qty_dec = _ceil_to_step(Decimal(str(min_notional)) / Decimal(str(last_price)), step)  # type: ignore[name-defined]
                except Exception:
                    required_qty_dec = Decimal(str(min_notional / last_price))  # type: ignore[name-defined]
                try:
                    required_qty = float(self._format_decimal(required_qty_dec, step))  # type: ignore[arg-type]
                except Exception:
                    required_qty = float(required_qty_dec)

                deadline = time.time() + 5.0
                while target_qty < required_qty and time.time() < deadline:
                    avail2 = self.get_available_base_qty(symbol, max_wait_s=1.0)
                    if avail2 > 0.0:
                        target_qty = min(float(quantity), avail2)
                    time.sleep(0.2)
                if target_qty < required_qty:
                    self.logger.error(
                        f"Abort close for {symbol}: target_qty={target_qty} < required_min_qty={required_qty} (minNotional={min_notional} price={last_price})"
                    )
                    return None
            target_qty = self.adjust_qty_for_safety(symbol, target_qty)
            if target_qty <= 0.0:
                self.logger.error(f"Failed to close position for {symbol}: no available base balance after cancellations")
                return None

            qty_str = self._normalize_and_format_qty(symbol, target_qty, None)
            if self._http is None:
                order = {
                    "orderId": f"DEV-CLOSE-{symbol}-{qty_str}",
                    "symbol": symbol,
                    "side": "Sell",
                    "qty": qty_str,
                }
                self.logger.info(f"Closed position (DEV): {symbol} qty={qty_str}")
                return order
            resp = self._http.request(
                "place_order",
                category="spot",
                symbol=symbol,
                side="Sell",
                orderType="Market",
                qty=qty_str,
            )
            if not self._is_success(resp):
                # Fallback: brief wait for reservation release and retry once
                try:
                    time.sleep(0.3)
                    # Re-evaluate available and cap once more
                    available2 = self.get_available_base_qty(symbol, max_wait_s=1.5)
                    tgt2 = float(target_qty)
                    if available2 > 0.0:
                        tgt2 = min(tgt2, available2)
                    tgt2 = self.adjust_qty_for_safety(symbol, tgt2)
                    if tgt2 <= 0.0:
                        self.logger.error(f"Failed to close position for {symbol}: no available base balance on retry")
                        return None
                    qty_str2 = self._normalize_and_format_qty(symbol, tgt2, None)
                    resp2 = self._http.request(
                        "place_order",
                        category="spot",
                        symbol=symbol,
                        side="Sell",
                        orderType="Market",
                        qty=qty_str2,
                    )
                    if not self._is_success(resp2):
                        self.logger.error(f"Failed to close position for {symbol}: retCode={resp2.get('retCode')} retMsg={resp2.get('retMsg')}")
                        return None
                    self.logger.info(f"Closed position (retry): {symbol} qty={qty_str2}")
                    return resp2
                except Exception as e:
                    self.logger.error(f"Close fallback failed for {symbol}: {e}")
                    return None
            self.logger.info(f"Closed position: {symbol} qty={qty_str}")
            return resp
        except Exception as e:
            self.logger.error(f"Failed to close position for {symbol}: {e}")
            return None

    def close_position(self, order_id: str) -> None:
        self.logger.info(f"Attempting to close position for order {order_id} (placeholder)")

    def monitor_open_positions(self) -> None:
        pass

    def sync_cancellations(self) -> None:
        """Best-effort cancellation sync based on open orders presence.

        Fetch current open spot orders and mark local open trades as 'cancelled'
        when their orderId is not present among exchange open orders.
        """
        if self._http is None:
            return
        try:
            resp = self._http.request("get_open_orders", category="spot")
            if not isinstance(resp, dict) or int(resp.get("retCode", -1)) != 0:
                return
            result = resp.get("result", {})
            rows = result.get("list", []) if isinstance(result, dict) else []
            remote_ids = {str(r.get("orderId")) for r in rows if r.get("orderId")}
            
            # Debug logging
            self.logger.debug(f"SYNC CANCELLATIONS: Found {len(remote_ids)} open orders on exchange: {list(remote_ids)[:5]}...")
            
            open_trades = self.db.get_open_trades()
            self.logger.debug(f"SYNC CANCELLATIONS: Found {len(open_trades)} open trades in DB")
            
            for tr in open_trades:
                try:
                    self.logger.debug(f"SYNC CANCELLATIONS: Checking trade {tr.order_id} (status: {tr.status}, side: {tr.side})")
                    if str(tr.order_id) not in remote_ids:
                        # Don't mark buy orders as cancelled - they might be filled
                        # Only mark sell orders (TP/SL) as cancelled
                        if tr.side and tr.side.upper() == "BUY":
                            self.logger.debug(f"SYNC CANCELLATIONS: Buy order {tr.order_id} not found in API - likely filled, keeping status")
                        else:
                            self.logger.warning(f"SYNC CANCELLATIONS: Marking sell order {tr.order_id} as cancelled - not found in exchange orders")
                            self.db.set_trade_status(tr.order_id, "cancelled")
                    else:
                        self.logger.debug(f"SYNC CANCELLATIONS: Trade {tr.order_id} found in exchange orders - keeping status")
                except Exception as e:
                    self.logger.debug(f"Failed to update cancelled for {tr.order_id}: {e}")
        except Exception as e:
            self.logger.error(f"Failed to sync cancellations: {e}")