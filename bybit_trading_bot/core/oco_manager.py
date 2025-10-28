from __future__ import annotations

import time
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Any
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, InvalidOperation

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.http_client import RateLimitedHTTP
from bybit_trading_bot.utils.notifier import Notifier

try:
    from pybit.unified_trading import HTTP
except Exception:  # pragma: no cover
    HTTP = None  # type: ignore


@dataclass(frozen=True)
class OCOOrder:
    """Represents a pair of OCO orders (TP + SL)."""
    symbol: str
    quantity: float
    entry_price: float
    tp_order_id: str
    sl_order_id: str
    tp_price: float
    sl_price: float
    created_at: datetime
    status: str  # 'active', 'tp_filled', 'sl_filled', 'cancelled'


class OCOManager:
    """Управление OCO (One-Cancels-the-Other) ордерами для Bybit.
    
    Реализует OCO функциональность через размещение двух условных ордеров:
    - Take Profit: лимитная продажа выше рынка
    - Stop Loss: лимитная продажа ниже рынка
    
    При исполнении одного ордера автоматически отменяет второй.
    """

    def __init__(self, config: Config, db: DBManager, notifier: Optional[Notifier] = None) -> None:
        self.config = config
        self.db = db
        self.logger = get_logger(self.__class__.__name__)
        self.notifier = notifier
        
        # HTTP client
        self._raw_http = None
        self._http = None
        
        # Symbol cache for filters
        self._symbol_cache: Dict[str, Dict[str, Any]] = {}
        
        # OCO state tracking
        self._active_oco_orders: Dict[str, OCOOrder] = {}  # symbol -> OCOOrder
        self._oco_lock = threading.Lock()
        self._monitoring_thread: Optional[threading.Thread] = None
        self._stop_monitoring = threading.Event()
        
        # Initialize HTTP client
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
                self.logger.error(f"Failed to init Bybit HTTP client for OCO: {e}")

    def _sync_server_time(self) -> None:
        """Sync local time with Bybit server to avoid timestamp errors."""
        try:
            import requests
            
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
                self.logger.info(f"OCO Server time sync: diff={time_diff}ms")
                
                # If difference is significant, we could adjust, but pybit handles this internally
                if abs(time_diff) > 5000:  # More than 5 seconds difference
                    self.logger.warning(f"OCO Large time difference detected: {time_diff}ms")
                    
        except Exception as e:
            self.logger.debug(f"OCO Server time sync failed: {e}")
            # Not critical, continue without sync

    def place_oco_orders(self, symbol: str, quantity: float, entry_price: float, 
                        profit_percentage: float, loss_percentage: float) -> Dict:
        """Размещает ордер с встроенными TP/SL параметрами (встроенная OCO функциональность).
        
        Args:
            symbol: Торговая пара (например, "BTCUSDT")
            quantity: Количество для продажи
            entry_price: Цена входа в позицию
            profit_percentage: Процент прибыли для TP
            loss_percentage: Процент убытка для SL
            
        Returns:
            dict: {
                'success': bool,
                'order_id': str,
                'tp_price': float,
                'sl_price': float,
                'message': str,
                'oco_order': OCOOrder
            }
        """
        try:
            # Validate parameters
            if not self._validate_oco_params(symbol, quantity, entry_price, profit_percentage, loss_percentage):
                return {
                    'success': False,
                    'order_id': None,
                    'tp_price': None,
                    'sl_price': None,
                    'message': 'Invalid OCO parameters',
                    'oco_order': None
                }
            
            # Check if symbol already has active OCO
            with self._oco_lock:
                if symbol in self._active_oco_orders:
                    return {
                        'success': False,
                        'order_id': None,
                        'tp_price': None,
                        'sl_price': None,
                        'message': f'OCO orders already active for {symbol}',
                        'oco_order': None
                    }
            
            # Calculate TP/SL prices
            tp_price = entry_price * (1.0 + profit_percentage / 100.0)
            sl_price = entry_price * (1.0 - loss_percentage / 100.0)
            
            # Place order with built-in TP/SL
            result = self._place_order_with_tpsl(
                symbol=symbol,
                side="Sell",
                quantity=quantity,
                entry_price=entry_price,
                tp_price=tp_price,
                sl_price=sl_price
            )
            
            if not result['success']:
                return {
                    'success': False,
                    'order_id': None,
                    'tp_price': None,
                    'sl_price': None,
                    'message': f'Failed to place order with TP/SL: {result["message"]}',
                    'oco_order': None
                }
            
            # Create OCO order record (Buy with TP/SL)
            oco_order = OCOOrder(
                symbol=symbol,
                quantity=quantity,
                entry_price=entry_price,
                tp_order_id=result['order_id'],  # Same order ID for TP/SL
                sl_order_id=result['order_id'],  # Same order ID for TP/SL
                tp_price=tp_price,  # TP price (above buy price)
                sl_price=sl_price,  # SL price (below buy price)
                created_at=datetime.now(),
                status='active'
            )
            
            # Store in memory
            with self._oco_lock:
                self._active_oco_orders[symbol] = oco_order
            
            # Save to database
            self._save_oco_order_to_db(oco_order)
            
            self.logger.info(f"✅ BUY ORDER WITH TP/SL PLACED: {symbol} Buy={entry_price:.6f} TP={tp_price:.6f} SL={sl_price:.6f}")
            # Telegram notification about successful placement
            try:
                if self.notifier:
                    planned_per_unit = tp_price - entry_price
                    try:
                        planned_total = planned_per_unit * float(quantity)
                    except Exception:
                        planned_total = planned_per_unit
                    self.notifier.send_telegram(
                        f"✅ ORDER PLACED (OCO): {symbol}\n"
                        f"Qty: {quantity}\nEntry: {entry_price:.6f}\nTP: {tp_price:.6f}\nSL: {sl_price:.6f}\n"
                        f"Planned profit total: {planned_total:.6f}\n"
                        f"OrderId: {result['order_id']}"
                    )
            except Exception:
                pass
            
            return {
                'success': True,
                'order_id': result['order_id'],
                'tp_price': tp_price,
                'sl_price': sl_price,
                'message': 'Buy order with TP/SL placed successfully',
                'oco_order': oco_order
            }
            
        except Exception as e:
            self.logger.error(f"Failed to place order with TP/SL for {symbol}: {e}")
            return {
                'success': False,
                'order_id': None,
                'tp_price': None,
                'sl_price': None,
                'message': f'Exception: {str(e)}',
                'oco_order': None
            }

    def _place_order_with_tpsl(self, symbol: str, side: str, quantity: float, 
                              entry_price: float, tp_price: float, sl_price: float) -> Dict:
        """Размещает ордер с встроенными TP/SL параметрами."""
        try:
            if self._http is None:
                # Dev mode
                order_id = f"DEV-TPSL-{symbol}-{int(time.time())}"
                return {
                    'success': True,
                    'order_id': order_id,
                    'message': 'Dev mode order with TP/SL'
                }
            
            # Format quantities and prices
            qty_str = self._format_quantity(symbol, quantity)
            tp_str = self._format_price(symbol, tp_price)
            sl_str = self._format_price(symbol, sl_price)
            
            # Place order with built-in TP/SL parameters (Buy order)
            resp = self._http.request(
                "place_order",
                category="spot",
                symbol=symbol,
                side="Buy",  # Always Buy for OCO
                orderType="Limit",
                qty=qty_str,
                price=self._format_price(symbol, entry_price),  # Buy price
                timeInForce="GTC",
                takeProfit=tp_str,      # TP trigger price (above buy price)
                stopLoss=sl_str,        # SL trigger price (below buy price)
                tpOrderType="Market",   # Market order when TP triggers
                slOrderType="Market",   # Market order when SL triggers
                orderLinkId=f"buy-tpsl-{symbol}-{int(time.time())}"
            )
            
            if not self._is_success(resp):
                return {
                    'success': False,
                    'order_id': None,
                    'message': f'API error: {resp.get("retMsg", "Unknown error")}'
                }
            
            # Extract order ID from response
            order_id = None
            if isinstance(resp, dict):
                result = resp.get("result", {})
                order_id = result.get("orderId")
            
            if not order_id:
                return {
                    'success': False,
                    'order_id': None,
                    'message': 'No order ID in response'
                }
            
            return {
                'success': True,
                'order_id': str(order_id),
                'message': 'Order with TP/SL placed successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'order_id': None,
                'message': f'Exception: {str(e)}'
            }

    def _place_conditional_order(self, symbol: str, side: str, quantity: float, 
                               trigger_price: float, order_price: float, order_link_id: str) -> Dict:
        """Place a regular limit order (not conditional for spot trading)."""
        try:
            if self._http is None:
                # Dev mode
                order_id = f"DEV-{order_link_id}"
                return {
                    'success': True,
                    'order_id': order_id,
                    'message': 'Dev mode order'
                }
            
            # Format quantities and prices
            qty_str = self._format_quantity(symbol, quantity)
            price_str = self._format_price(symbol, order_price)
            
            # Use regular limit order instead of conditional order for spot trading
            resp = self._http.request(
                "place_order",
                category="spot",
                symbol=symbol,
                side=side,
                orderType="Limit",
                qty=qty_str,
                price=price_str,
                timeInForce="GTC",
                orderLinkId=order_link_id
            )
            
            if not self._is_success(resp):
                return {
                    'success': False,
                    'order_id': None,
                    'message': f"API error: {resp.get('retCode')} {resp.get('retMsg')}"
                }
            
            order_id = str(resp.get("result", {}).get("orderId", ""))
            return {
                'success': True,
                'order_id': order_id,
                'message': 'Order placed successfully'
            }
            
        except Exception as e:
            return {
                'success': False,
                'order_id': None,
                'message': f'Exception: {str(e)}'
            }

    def cancel_oco_orders(self, symbol: str) -> Dict:
        """Отменяет все OCO ордера для символа."""
        try:
            with self._oco_lock:
                if symbol not in self._active_oco_orders:
                    return {
                        'success': False,
                        'message': f'No active OCO orders for {symbol}'
                    }
                
                oco_order = self._active_oco_orders[symbol]
                
                # Cancel both orders
                tp_cancelled = self._cancel_order(oco_order.tp_order_id)
                sl_cancelled = self._cancel_order(oco_order.sl_order_id)
                
                # Remove from memory
                del self._active_oco_orders[symbol]
                
                # Update database
                self._update_oco_order_status(symbol, 'cancelled')
                
                self.logger.info(f"✅ OCO ORDERS CANCELLED: {symbol}")
                
                return {
                    'success': True,
                    'message': f'OCO orders cancelled for {symbol}',
                    'tp_cancelled': tp_cancelled,
                    'sl_cancelled': sl_cancelled
                }
                
        except Exception as e:
            self.logger.error(f"Failed to cancel OCO orders for {symbol}: {e}")
            return {
                'success': False,
                'message': f'Exception: {str(e)}'
            }

    def start_monitoring(self) -> None:
        """Запускает мониторинг OCO ордеров."""
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            self.logger.warning("OCO monitoring already running")
            return
        
        self._stop_monitoring.clear()
        self._monitoring_thread = threading.Thread(
            target=self._monitor_oco_orders,
            name="OCOMonitor",
            daemon=True
        )
        self._monitoring_thread.start()
        self.logger.info("OCO monitoring started")

    def stop_monitoring(self) -> None:
        """Останавливает мониторинг OCO ордеров."""
        self._stop_monitoring.set()
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5.0)
        self.logger.info("OCO monitoring stopped")

    def _monitor_oco_orders(self) -> None:
        """Мониторинг исполнения OCO ордеров."""
        self.logger.info("OCO monitoring worker started")
        
        while not self._stop_monitoring.is_set():
            try:
                with self._oco_lock:
                    symbols_to_check = list(self._active_oco_orders.keys())
                
                for symbol in symbols_to_check:
                    self._check_oco_order_execution(symbol)
                
                time.sleep(2.0)  # Check every 2 seconds
                
            except Exception as e:
                self.logger.error(f"OCO monitoring error: {e}")
                time.sleep(5.0)
        
        self.logger.info("OCO monitoring worker stopped")

    def _check_oco_order_execution(self, symbol: str) -> None:
        """Проверяет исполнение ордера с TP/SL для символа."""
        try:
            with self._oco_lock:
                if symbol not in self._active_oco_orders:
                    return
                oco_order = self._active_oco_orders[symbol]
            
            # Check main order status (same ID for both TP/SL)
            order_status = self._get_order_status(oco_order.tp_order_id)
            
            # If order is filled, it means either TP or SL was triggered
            if order_status == 'Filled':
                # Try to determine which one was triggered by checking the fill price
                fill_price = self._get_order_fill_price(oco_order.tp_order_id)
                if fill_price:
                    if fill_price >= oco_order.tp_price:
                        self.logger.info(f"🎯 TP TRIGGERED: {symbol} at {fill_price:.6f}")
                        # Telegram TP notification with PnL
                        try:
                            if self.notifier:
                                pnl = (fill_price - oco_order.entry_price) * float(oco_order.quantity)
                                pnl_pct = ((fill_price / max(oco_order.entry_price, 1e-12)) - 1.0) * 100.0
                                self.notifier.send_telegram(
                                    f"🎯 TP FILLED: {symbol}\nQty: {oco_order.quantity}\nEntry: {oco_order.entry_price:.6f}\nTP: {oco_order.tp_price:.6f}\nFill: {fill_price:.6f}\nPnL: {pnl:.4f} ({pnl_pct:.2f}%)"
                                )
                        except Exception:
                            pass
                        self._finalize_oco_order(symbol, 'tp_filled')
                    elif fill_price <= oco_order.sl_price:
                        self.logger.info(f"🛑 SL TRIGGERED: {symbol} at {fill_price:.6f}")
                        # Telegram SL notification with PnL
                        try:
                            if self.notifier:
                                pnl = (fill_price - oco_order.entry_price) * float(oco_order.quantity)
                                pnl_pct = ((fill_price / max(oco_order.entry_price, 1e-12)) - 1.0) * 100.0
                                self.notifier.send_telegram(
                                    f"🛑 SL FILLED: {symbol}\nQty: {oco_order.quantity}\nEntry: {oco_order.entry_price:.6f}\nSL: {oco_order.sl_price:.6f}\nFill: {fill_price:.6f}\nPnL: {pnl:.4f} ({pnl_pct:.2f}%)"
                                )
                        except Exception:
                            pass
                        self._finalize_oco_order(symbol, 'sl_filled')
                    else:
                        self.logger.info(f"✅ ORDER FILLED: {symbol} at {fill_price:.6f}")
                        self._finalize_oco_order(symbol, 'filled')
                else:
                    self.logger.info(f"✅ ORDER FILLED: {symbol}")
                    self._finalize_oco_order(symbol, 'filled')
                return
            
            # If order is cancelled or rejected, remove from tracking
            if order_status in ['Cancelled', 'Rejected']:
                self.logger.info(f"❌ ORDER FAILED: {symbol} - {order_status}")
                self._finalize_oco_order(symbol, 'cancelled')
                
        except Exception as e:
            self.logger.error(f"Error checking order execution for {symbol}: {e}")

    def _finalize_oco_order(self, symbol: str, final_status: str) -> None:
        """Завершает OCO ордер и обновляет состояние."""
        try:
            with self._oco_lock:
                if symbol in self._active_oco_orders:
                    del self._active_oco_orders[symbol]
            
            # Update database
            self._update_oco_order_status(symbol, final_status)
            
        except Exception as e:
            self.logger.error(f"Error finalizing OCO order for {symbol}: {e}")

    def get_active_oco_orders(self) -> Dict[str, OCOOrder]:
        """Возвращает активные OCO ордера."""
        with self._oco_lock:
            return self._active_oco_orders.copy()

    def _validate_oco_params(self, symbol: str, quantity: float, entry_price: float, 
                           profit_percentage: float, loss_percentage: float) -> bool:
        """Валидация параметров OCO ордеров."""
        try:
            if not symbol or not isinstance(symbol, str):
                return False
            
            if quantity <= 0 or entry_price <= 0:
                return False
            
            if profit_percentage <= 0 or loss_percentage <= 0:
                return False
            
            if profit_percentage >= 100 or loss_percentage >= 100:
                return False
            
            return True
            
        except Exception:
            return False

    def _cancel_order(self, order_id: str) -> bool:
        """Отменяет ордер по ID."""
        try:
            if self._http is None:
                return True  # Dev mode
            
            resp = self._http.request(
                "cancel_order",
                category="spot",
                orderId=order_id
            )
            
            return self._is_success(resp)
            
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    def _get_order_fill_price(self, order_id: str) -> Optional[float]:
        """Получает цену исполнения ордера."""
        try:
            if self._http is None:
                return None
            
            resp = self._http.request("get_open_orders", category="spot", orderId=order_id)
            if not self._is_success(resp):
                return None
            
            result = resp.get("result", {})
            lst = result.get("list", [])
            if lst:
                order = lst[0]
                avg_price = order.get("avgPrice")
                if avg_price:
                    return float(avg_price)
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Failed to get fill price for order {order_id}: {e}")
            return None

    def _get_order_status(self, order_id: str) -> str:
        """Получает статус ордера."""
        try:
            if self._http is None:
                return 'Unknown'  # Dev mode
            
            resp = self._http.request(
                "get_open_orders",
                category="spot",
                orderId=order_id
            )
            
            if not self._is_success(resp):
                return 'Unknown'
            
            orders = resp.get("result", {}).get("list", [])
            for order in orders:
                if str(order.get("orderId")) == order_id:
                    return str(order.get("orderStatus", "Unknown"))
            
            return 'Filled'  # Not in open orders, likely filled
            
        except Exception as e:
            self.logger.error(f"Failed to get order status for {order_id}: {e}")
            return 'Unknown'

    def _is_success(self, resp: Dict) -> bool:
        """Проверяет успешность API ответа."""
        if not isinstance(resp, dict):
            return False
        return int(resp.get("retCode", -1)) == 0

    def _format_quantity(self, symbol: str, quantity: float) -> str:
        """Форматирует количество для API с правильной точностью."""
        self.logger.info(f"Formatting quantity for {symbol}: {quantity}")
        
        try:
            # Use the same approach as order_manager
            filters = self._get_symbol_filters(symbol)
            qty = Decimal(str(quantity))
            step = filters.get("qty_step", Decimal("0.01"))
            
            # Ensure quantity meets minimum requirements
            min_qty = filters.get("min_qty", Decimal("0.01"))
            if qty < min_qty:
                qty = min_qty
            
            # Format using step size
            formatted_qty = self._format_decimal(qty, step)
            self.logger.info(f"Formatted quantity: {formatted_qty} (step={step}, min_qty={min_qty})")
            
            # For XPLUSDT, try rounding to integer if step is 0.01
            if symbol == "XPLUSDT" and step == Decimal("0.01"):
                try:
                    int_qty = int(float(formatted_qty))
                    self.logger.info(f"XPLUSDT: Trying integer quantity: {int_qty}")
                    return str(int_qty)
                except Exception as e:
                    self.logger.warning(f"XPLUSDT: Failed to convert to integer: {e}")
            
            return formatted_qty
            
        except Exception as e:
            self.logger.error(f"Failed to format quantity for {symbol}: {e}")
            # Final fallback - try to round to 2 decimal places
            try:
                rounded_qty = round(quantity, 2)
                return f"{rounded_qty:.2f}"
            except Exception:
                return f"{quantity:.6f}".rstrip('0').rstrip('.')

    def _get_symbol_filters(self, symbol: str) -> Dict[str, Any]:
        """Получает фильтры символа из кэша или API."""
        if symbol in self._symbol_cache:
            self.logger.info(f"Using cached filters for {symbol}: {self._symbol_cache[symbol]}")
            return self._symbol_cache[symbol]
        
        if self._http is None:
            # Dev mode - return default filters
            return {
                "tick_size": Decimal("0.0001"),
                "min_order_qty": Decimal("0.01"),
                "min_order_amt": Decimal("5.0"),
                "base_coin": symbol.replace("USDT", ""),
                "quote_coin": "USDT"
            }
        
        try:
            resp = self._http.request("get_instruments_info", category="spot", symbol=symbol)
            if resp and resp.get("retCode") == 0:
                result = resp.get("result", {})
                lst = result.get("list", [])
                if lst:
                    info = lst[0]
                    lot = info.get("lotSizeFilter", {})
                    price_filter = info.get("priceFilter", {})
                    # Prefer explicit qtyStep; some symbols expose basePrecision instead
                    qty_step_val = lot.get("qtyStep") if lot.get("qtyStep") is not None else lot.get("basePrecision")
                    try:
                        qty_step = Decimal(str(qty_step_val)) if qty_step_val is not None else Decimal("0.01")
                    except Exception:
                        qty_step = Decimal("0.01")
                    # Minimum quantity and notional
                    try:
                        min_qty = Decimal(str(lot.get("minOrderQty", "0.0")))
                    except Exception:
                        min_qty = Decimal("0.0")
                    try:
                        min_notional = Decimal(str(lot.get("minOrderAmt", "0.0")))
                    except Exception:
                        min_notional = Decimal("0.0")
                    # Tick size
                    try:
                        tick_size = Decimal(str(price_filter.get("tickSize", "0.0001")))
                    except Exception:
                        tick_size = Decimal("0.0001")
                    filters = {
                        "qty_step": qty_step,
                        "min_qty": min_qty,
                        "min_notional": min_notional,
                        "tick_size": tick_size,
                        "base_coin": info.get("baseCoin", symbol.replace("USDT", "")),
                        "quote_coin": info.get("quoteCoin", "USDT"),
                    }
                    self._symbol_cache[symbol] = filters
                    self.logger.info(f"Resolved filters for {symbol}: {filters}")
                    return filters
        except Exception as e:
            self.logger.debug(f"Failed to get symbol filters for {symbol}: {e}")
        
        # Fallback to default filters
        default_filters = {
            "qty_step": Decimal("0.01"),
            "min_qty": Decimal("0.0"),
            "min_notional": Decimal("0.0"),
            "tick_size": Decimal("0.0001"),
            "base_coin": symbol.replace("USDT", ""),
            "quote_coin": "USDT",
        }
        self._symbol_cache[symbol] = default_filters
        self.logger.info(f"Using default filters for {symbol}: {default_filters}")
        return default_filters

    def _format_decimal(self, value: Decimal, step: Decimal) -> str:
        """Форматирует Decimal значение согласно шагу."""
        try:
            q = value.quantize(step, rounding=ROUND_DOWN)
        except (InvalidOperation, ValueError):
            exponent = Decimal(str(step)).normalize().as_tuple().exponent
            q = value.quantize(Decimal((0, (1,), exponent)), rounding=ROUND_DOWN)
        s = format(q, 'f')
        return s

    def _format_price(self, symbol: str, price: float) -> str:
        """Форматирует цену для API с правильной точностью."""
        filters = self._get_symbol_filters(symbol)
        price_decimal = Decimal(str(price))
        tick_size = filters["tick_size"]
        return self._format_decimal(price_decimal, tick_size)

    def _save_oco_order_to_db(self, oco_order: OCOOrder) -> None:
        """Сохраняет OCO ордер в базу данных."""
        try:
            conn = self.db._get_conn()
            cur = conn.cursor()
            
            # Get symbol_id
            symbol_id = self.db.get_symbol_id(oco_order.symbol)
            if symbol_id is None:
                self.logger.warning(f"Unknown symbol {oco_order.symbol} for OCO order")
                return
            
            # Insert OCO order record
            cur.execute(
                """
                INSERT INTO oco_orders (
                    symbol_id, symbol, quantity, entry_price, tp_order_id, sl_order_id,
                    tp_price, sl_price, status, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol_id,
                    oco_order.symbol,
                    oco_order.quantity,
                    oco_order.entry_price,
                    oco_order.tp_order_id,
                    oco_order.sl_order_id,
                    oco_order.tp_price,
                    oco_order.sl_price,
                    oco_order.status,
                    oco_order.created_at
                )
            )
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Failed to save OCO order to DB: {e}")

    def _update_oco_order_status(self, symbol: str, status: str) -> None:
        """Обновляет статус OCO ордера в базе данных."""
        try:
            conn = self.db._get_conn()
            cur = conn.cursor()
            
            cur.execute(
                "UPDATE oco_orders SET status = ?, closed_at = CURRENT_TIMESTAMP WHERE symbol = ? AND status = 'active'",
                (status, symbol)
            )
            conn.commit()
            
        except Exception as e:
            self.logger.error(f"Failed to update OCO order status: {e}")
