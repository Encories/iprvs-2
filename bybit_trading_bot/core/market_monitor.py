from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List, Tuple, Dict

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
try:
    from bybit_trading_bot.momentum_enhancements.momentum_gradient import GradientMomentumDetector
    from bybit_trading_bot.momentum_enhancements.momentum_exhaustion import MomentumExhaustionDetector
except Exception:  # pragma: no cover
    GradientMomentumDetector = None  # type: ignore
    MomentumExhaustionDetector = None  # type: ignore
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.notifier import Notifier, TelegramCommandListener
from bybit_trading_bot.core.data_processor import calculate_percentage_change
from bybit_trading_bot.indicators.technical import calculate_rsi, calculate_macd
from bybit_trading_bot.core.symbol_mapper import SymbolMapper
from bybit_trading_bot.core.order_manager import OrderManager
from bybit_trading_bot.core.software_sl_manager import SoftwareSLManager
from bybit_trading_bot.core.oco_manager import OCOManager
from bybit_trading_bot.handlers.spot_handler import SpotHandler
from bybit_trading_bot.handlers.futures_handler import FuturesHandler
from bybit_trading_bot.core.spike_detector import SpikeDetector

try:
    from pybit.unified_trading import HTTP as BYBIT_HTTP
except Exception:
    BYBIT_HTTP = None  # type: ignore


@dataclass
class Signal:
    symbol_id: int
    price_change_percent: float
    oi_change_percent: float


class MarketMonitor:
    """Основной класс для мониторинга двух рынков.

    Запускает 4 параллельных потока:
    - Спот WebSocket (получение цен в реальном времени)
    - OI polling (HTTP запросы каждые N секунд)
    - Анализатор (проверка условий сигналов)
    - Исполнитель (размещение ордеров по сигналам)

    На этом этапе реализованы каркасы потоков и безопасная остановка.
    """

    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

        self._stop_event = threading.Event()
        self._threads: List[threading.Thread] = []
        self.is_running: bool = False

        # Infra
        self.db = DBManager(self.config.database_path)
        self.symbol_mapper = SymbolMapper(self.config, self.db)
        self.order_manager = OrderManager(self.config, self.db)
        self.notifier = Notifier(self.config)
        self.tg_listener = TelegramCommandListener(
            self.config, 
            on_stop=self._activate_runtime_emergency, 
            on_rate=self._build_rate_report, 
            on_start=self._deactivate_runtime_emergency, 
            on_signal=self._on_tg_signal, 
            on_orders=self._build_open_orders_report, 
            on_panics=self._build_panic_sell_report,
            on_oco=self._build_oco_report
        )
        # Software SL manager
        self.software_sl = SoftwareSLManager(self.config, self.db, self.notifier)
        # OCO manager (with notifier for Telegram)
        self.oco_manager = OCOManager(self.config, self.db, notifier=self.notifier)
        self.spot = SpotHandler(testnet=self.config.bybit_testnet)
        # in-memory recent trades with side per symbol for order-flow delta
        self._recent_trades: Dict[str, List[Tuple[float, float, float, str | None]]] = {}
        self.futures = FuturesHandler(testnet=self.config.bybit_testnet)
        # Split mode state
        self._split_detectors: dict[str, SpikeDetector] = {}
        self._split_last_price: dict[str, float] = {}

        # Queues
        self._pending_signals: List[Signal] = []
        self._signals_lock = threading.Lock()
        self._last_sync_time: float = 0.0
        self._last_analyzer_summary: float = 0.0
        self._last_api_sync_time: float = 0.0
        self._last_panic_check_time: float = 0.0
        self._panic_notified_orders: set = set()  # Track orders that already got panic notifications
        self._runtime_emergency_stop: bool = False
        self._loss_notify_sent: bool = False
        # In-memory 1m quote-volume buckets per symbol (epoch minute -> USDT volume)
        self._minute_volumes: Dict[str, Dict[int, float]] = {}
        # In-memory best quotes to infer trade side from orderbook if exchange omits it
        self._best_quotes: Dict[str, Tuple[float, float]] = {}
        # Momentum block state (per-symbol) for RSI+Candle exhaustion persistence
        # Tracks last reasons key, consecutive count, and cooldown deadline (epoch seconds)
        self._momentum_block_state: Dict[str, Dict[str, object]] = {}

    def _build_rate_report(self) -> str:
        try:
            balance_str = "N/A"
            if BYBIT_HTTP is not None and self.config.bybit_api_key and self.config.bybit_api_secret:
                http = BYBIT_HTTP(testnet=self.config.bybit_testnet, api_key=self.config.bybit_api_key, api_secret=self.config.bybit_api_secret)
                d = http.get_wallet_balance(accountType='UNIFIED')
                lst = (d or {}).get('result', {}).get('list', [])
                coins = lst[0].get('coin', []) if lst else []
                usdt = [c for c in coins if c.get('coin') == 'USDT']
                if usdt:
                    bal = usdt[0].get('availableToTrade') or usdt[0].get('availableToWithdraw') or usdt[0].get('walletBalance')
                    balance_str = str(bal)
            total_pnl = self.db.get_total_pnl()
            # ROI% относительно текущего баланса, если доступен
            roi = None
            try:
                roi = (float(total_pnl) / float(balance_str)) * 100.0 if balance_str not in {"N/A", None, "0", 0} else None
            except Exception:
                roi = None
            lines = [
                f"Account balance (USDT): {balance_str}",
                f"Total PnL (USDT): {total_pnl:.4f}",
                f"ROI (%): {roi:.2f}" if roi is not None else "ROI (%): N/A",
            ]
            return "\n".join(lines)
        except Exception as e:
            self.logger.error(f"Build rate report error: {e}")
            return "Rate error"

    def _build_open_orders_report(self) -> str:
        """Fetch live open spot orders from Bybit and format a concise report."""
        try:
# from bybit_trading_bot.core.order_manager import OrderManager  # unused here
            om = self.order_manager
            if om is None or getattr(om, "_http", None) is None:
                return "Open orders: not available (HTTP client offline)"
            # Fetch all open spot orders
            try:
                resp = om._http.request("get_open_orders", category="spot")
                if int(resp.get("retCode", -1)) != 0:
                    return f"Open orders fetch failed: {resp.get('retCode')} {resp.get('retMsg')}"
                rows = (resp.get("result", {}) or {}).get("list", [])
            except Exception as e:
                return f"Open orders error: {e}"
            if not rows:
                return "No open spot orders"
            # Build lines
            out: List[str] = ["Open spot orders:"]
            for r in rows[:50]:
                try:
                    sym = str(r.get("symbol") or "?")
                    side = str(r.get("side") or "?")
                    otype = str(r.get("orderType") or "?")
                    qty = r.get("qty") or r.get("baseQty") or r.get("cumExecQty") or "?"
                    price = r.get("price") or r.get("avgPrice") or "-"
                    status = str(r.get("orderStatus") or "open")
                    oid = str(r.get("orderId") or "")
                    out.append(f"{sym} {side} {otype} qty={qty} price={price} status={status} id={oid}")
                except Exception:
                    continue
            return "\n".join(out)
        except Exception as e:
            self.logger.error(f"Build open orders report error: {e}")
            return "Open orders error"

    def _build_panic_sell_report(self) -> str:
        """Build a report of all active panic sell monitoring positions based on API data."""
        try:
            # Check if panic is enabled
            panic_enabled = getattr(self.config, "panic_enabled", True)
            if not panic_enabled:
                return "Panic monitoring is disabled"
            
            # Check if panic sell is enabled
            panic_sell_enabled = getattr(self.config, "panic_sell_enabled", False)
            if not panic_sell_enabled:
                return "Panic sell monitoring is disabled"
            
            # Get all open spot orders from API
            if not self.order_manager or not hasattr(self.order_manager, '_http') or not self.order_manager._http:
                return "Panic sell monitoring: API client not available"
            
            try:
                resp = self.order_manager._http.request("get_open_orders", category="spot")
                if int(resp.get("retCode", -1)) != 0:
                    return f"Panic sell monitoring: API error {resp.get('retCode')} {resp.get('retMsg')}"
                
                open_orders = (resp.get("result", {}) or {}).get("list", [])
                if not open_orders:
                    return "No open spot orders found for panic sell monitoring"
                
                # Filter only buy orders (our positions)
                buy_orders = [o for o in open_orders if str(o.get("side", "")).upper() == "BUY"]
                if not buy_orders:
                    return "No open buy orders found for panic sell monitoring"
                
                panic_threshold = float(getattr(self.config, "panic_sell_drop_pct", 2.0))
                lines = [f"Panic Sell Monitoring (threshold: {panic_threshold:.1f}%):"]
                
                for order in buy_orders:
                    try:
                        symbol = str(order.get("symbol", "UNKNOWN"))
                        order_id = str(order.get("orderId", ""))
                        _ = float(order.get("qty", 0))
                        price = float(order.get("price", 0))
                        avg_price = float(order.get("avgPrice", 0))
                        _ = str(order.get("orderStatus", ""))
                        
                        # Use average price if available, otherwise use order price
                        entry_price = avg_price if avg_price > 0 else price
                        if entry_price <= 0:
                            continue
                        
                        # Get current market price
                        symbol_id = self.db.get_symbol_id(symbol)
                        last_price = self.db.get_last_price(symbol_id) if symbol_id else None
                        
                        if last_price is None:
                            lines.append(f"  {symbol}: entry={entry_price:.6f} last=NO_DATA order_id={order_id}")
                            continue
                        
                        # Calculate current drop percentage
                        drop_pct = (last_price - entry_price) / entry_price * 100.0
                        trigger_price = entry_price * (1.0 - panic_threshold / 100.0)
                        
                        # Status indicators
                        if drop_pct > -panic_threshold:
                            status_icon = "🟢 SAFE"
                        elif drop_pct <= -panic_threshold * 0.8:  # 80% of threshold
                            status_icon = "🟡 WARNING"
                        else:
                            status_icon = "🔴 TRIGGERED"
                        
                        lines.append(f"  {symbol}: entry={entry_price:.6f} last={last_price:.6f} drop={drop_pct:.2f}% trigger={trigger_price:.6f} {status_icon}")
                        
                    except Exception as e:
                        self.logger.error(f"Error processing order {order.get('orderId', 'unknown')}: {e}")
                        continue
                
                return "\n".join(lines)
                
            except Exception as e:
                self.logger.error(f"API error in panic sell report: {e}")
                return f"Panic sell monitoring: API error - {e}"
                
        except Exception as e:
            self.logger.error(f"Build panic sell report error: {e}")
            return "Panic sell report error"

    def _build_oco_report(self) -> str:
        """Build a report of all active OCO orders."""
        try:
            # Check if OCO is enabled
            oco_enabled = getattr(self.config, "oco_enabled", False)
            if not oco_enabled:
                return "OCO orders are disabled"
            
            # Get active OCO orders
            active_oco_orders = self.oco_manager.get_active_oco_orders()
            if not active_oco_orders:
                return "No active OCO orders"
            
            report_lines = [f"🎯 ACTIVE OCO ORDERS ({len(active_oco_orders)}):"]
            report_lines.append("")
            
            for symbol, oco_order in active_oco_orders.items():
                try:
                    report_lines.append(f"📊 {symbol}:")
                    report_lines.append(f"  Entry: {oco_order.entry_price:.6f}")
                    report_lines.append(f"  Qty: {oco_order.quantity:.2f}")
                    report_lines.append(f"  TP: {oco_order.tp_price:.6f} (ID: {oco_order.tp_order_id})")
                    report_lines.append(f"  SL: {oco_order.sl_price:.6f} (ID: {oco_order.sl_order_id})")
                    report_lines.append(f"  Status: {oco_order.status}")
                    report_lines.append(f"  Created: {oco_order.created_at.strftime('%H:%M:%S')}")
                    report_lines.append("")
                
                except Exception as e:
                    self.logger.error(f"Error building OCO report for {symbol}: {e}")
                    continue
            
            return "\n".join(report_lines)
            
        except Exception as e:
            self.logger.error(f"Build OCO report error: {e}")
            return "OCO report error"

    def _perform_test_purchase(self) -> None:
        """Perform test purchase of XPL/USDT with OCO orders."""
        try:
            symbol = "XPLUSDT"
            test_amount_usdt = 10.0  # Increased from 2.0 to meet minimum order requirements
            
            self.logger.info(f"🧪 TEST PURCHASE: Starting test purchase of {symbol} for {test_amount_usdt} USDT")
            
            # Check if all components are ready
            if not self.order_manager:
                self.logger.error("TEST PURCHASE: OrderManager not available")
                return
            
            if not self.oco_manager:
                self.logger.error("TEST PURCHASE: OCOManager not available")
                return
            
            if not self.db:
                self.logger.error("TEST PURCHASE: Database not available")
                return
            
            # Add delay before first API request
            self.logger.info("TEST PURCHASE: Waiting 2 seconds before first API request...")
            time.sleep(2.0)
            
            # Get current price
            if not self.order_manager or not hasattr(self.order_manager, '_http') or not self.order_manager._http:
                self.logger.error("TEST PURCHASE: API client not available")
                return
            
            # Additional check for OCO manager
            if not self.oco_manager or not hasattr(self.oco_manager, '_http') or not self.oco_manager._http:
                self.logger.error("TEST PURCHASE: OCO API client not available")
                return
            
            # Get ticker data for current price with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                resp = self.order_manager._http.request("get_tickers", category="spot", symbol=symbol)
                if self._is_success(resp):
                    break
                
                self.logger.warning(f"TEST PURCHASE: Attempt {attempt + 1} failed for {symbol}: {resp.get('retMsg')}")
                
                # If timestamp error, wait longer and retry
                if resp.get('retCode') == 10002:
                    wait_time = (attempt + 1) * 3  # 3, 6, 9 seconds
                    self.logger.info(f"TEST PURCHASE: Timestamp error, waiting {wait_time} seconds and retrying...")
                    time.sleep(wait_time)
                else:
                    break
            
            if not self._is_success(resp):
                self.logger.error(f"TEST PURCHASE: All attempts failed for {symbol}: {resp.get('retMsg')}")
                return
            
            tickers = resp.get("result", {}).get("list", [])
            if not tickers:
                self.logger.error(f"TEST PURCHASE: No ticker data for {symbol}")
                return
            
            current_price = float(tickers[0].get("lastPrice", 0))
            if current_price <= 0:
                self.logger.error(f"TEST PURCHASE: Invalid price for {symbol}: {current_price}")
                return
            
            # Calculate quantity for 2 USDT
            quantity = test_amount_usdt / current_price
            
            self.logger.info(f"TEST PURCHASE: {symbol} current_price={current_price:.6f} quantity={quantity:.6f}")
            
            # Place OCO order directly (Buy with TP/SL)
            # Use the same TP percent as the system uses for signals, not OCO_PROFIT_PERCENTAGE
            oco_profit_pct = getattr(self.config, "take_profit_percent", getattr(self.config, "oco_profit_percentage", 2.0))
            oco_loss_pct = getattr(self.config, "oco_loss_percentage", 2.0)
            
            oco_result = self.oco_manager.place_oco_orders(
                symbol=symbol,
                quantity=quantity,
                entry_price=current_price,
                profit_percentage=oco_profit_pct,
                loss_percentage=oco_loss_pct
            )
            
            if not oco_result['success']:
                self.logger.error(f"TEST PURCHASE: Failed to place OCO order for {symbol}: {oco_result['message']}")
                return
            
            # Save to database
            try:
                symbol_id = self.db.get_symbol_id(symbol)
                if symbol_id:
                    self.db.insert_trade(
                        symbol_id=symbol_id,
                        order_id=oco_result['order_id'],
                        side="Buy",
                        quantity=quantity,
                        entry_price=current_price,
                        take_profit_price=oco_result['tp_price'],
                        status="open",
                        tp_order_id=oco_result['order_id'],
                        is_oco_order=True
                    )
                    self.logger.info(f"TEST PURCHASE: OCO trade saved to DB {symbol} order_id={oco_result['order_id']}")
                else:
                    self.logger.warning(f"TEST PURCHASE: Symbol {symbol} not found in DB")
            except Exception as e:
                self.logger.error(f"TEST PURCHASE: Failed to save OCO trade to DB: {e}")
            
            # Send notifications
            try:
                self.notifier.send_telegram(
                    f"🧪 TEST PURCHASE COMPLETED: {symbol}\n"
                    f"Entry: {current_price:.6f}\n"
                    f"Qty: {quantity:.6f}\n"
                    f"TP: {oco_result['tp_price']:.6f} (+{oco_profit_pct:.1f}%)\n"
                    f"SL: {oco_result['sl_price']:.6f} (-{oco_loss_pct:.1f}%)\n"
                    f"Order ID: {oco_result['order_id']}"
                )
            except Exception as e:
                self.logger.error(f"TEST PURCHASE: Failed to send notification: {e}")
            
            self.logger.info(f"✅ TEST PURCHASE SUCCESS: {symbol} with OCO order placed")
            
        except Exception as e:
            self.logger.error(f"TEST PURCHASE ERROR: {e}")

    def _is_success(self, resp: Dict) -> bool:
        """Check if API response is successful."""
        if not isinstance(resp, dict):
            return False
        return int(resp.get("retCode", -1)) == 0

    def _monitor_panic_sell_from_api(self) -> None:
        """Monitor panic sell conditions based on API data from open orders."""
        try:
            # Check if panic is enabled
            panic_enabled = getattr(self.config, "panic_enabled", True)
            if not panic_enabled:
                return
            
            # Check if panic sell is enabled
            panic_sell_enabled = getattr(self.config, "panic_sell_enabled", False)
            if not panic_sell_enabled:
                return
            
            # Get all open spot orders from API
            if not self.order_manager or not hasattr(self.order_manager, '_http') or not self.order_manager._http:
                return
            
            try:
                resp = self.order_manager._http.request("get_open_orders", category="spot")
                if int(resp.get("retCode", -1)) != 0:
                    return
                
                open_orders = (resp.get("result", {}) or {}).get("list", [])
                if not open_orders:
                    return
                
                # Filter only buy orders (our positions)
                buy_orders = [o for o in open_orders if str(o.get("side", "")).upper() == "BUY"]
                if not buy_orders:
                    return
                
                panic_threshold = float(getattr(self.config, "panic_sell_drop_pct", 2.0))
                
                for order in buy_orders:
                    try:
                        symbol = str(order.get("symbol", "UNKNOWN"))
                        order_id = str(order.get("orderId", ""))
                        _ = float(order.get("qty", 0))
                        price = float(order.get("price", 0))
                        avg_price = float(order.get("avgPrice", 0))
                        
                        # Use average price if available, otherwise use order price
                        entry_price = avg_price if avg_price > 0 else price
                        if entry_price <= 0:
                            continue
                        
                        # Get current market price
                        symbol_id = self.db.get_symbol_id(symbol)
                        last_price = self.db.get_last_price(symbol_id) if symbol_id else None
                        
                        if last_price is None:
                            continue
                        
                        # Calculate current drop percentage
                        drop_pct = (last_price - entry_price) / entry_price * 100.0
                        
                        # Check if panic sell should trigger
                        if drop_pct <= -panic_threshold:
                            self.logger.warning(f"PANIC SELL TRIGGERED (API): {symbol} drop={drop_pct:.2f}% entry={entry_price:.6f} last={last_price:.6f}")
                            
                            # Notify immediately
                            try:
                                self.notifier.send_telegram(
                                    f"🚨 PANIC SELL TRIGGERED (API): {symbol} drop={drop_pct:.2f}% entry={entry_price:.6f} last={last_price:.6f}. Cancelling orders and selling at market."
                                )
                            except Exception:
                                pass
                            
                            # Cancel all orders for this symbol and sell at market
                            try:
                                # Cancel all open orders for this symbol
                                if self.order_manager._http:
                                    resp_open = self.order_manager._http.request("get_open_orders", category="spot", symbol=symbol)
                                    if int(resp_open.get("retCode", -1)) == 0:
                                        rows = (resp_open.get("result", {}) or {}).get("list", [])
                                        if rows:
                                            if getattr(self.config, "cancel_orders_enabled", True):
                                                self.logger.info(f"PANIC SELL: Found {len(rows)} open orders for {symbol}, cancelling all")
                                                # Cancel all orders
                                                for order in rows:
                                                    order_id = str(order.get("orderId", ""))
                                                    if order_id:
                                                        try:
                                                            self.order_manager.cancel_order(symbol, order_id)
                                                            self.logger.debug(f"PANIC SELL: Cancelled order {order_id} for {symbol}")
                                                        except Exception as e:
                                                            self.logger.warning(f"PANIC SELL: Failed to cancel order {order_id} for {symbol}: {e}")
                                                # Wait for cancellations to take effect
                                                deadline = time.time() + 3.0
                                                while time.time() < deadline:
                                                    try:
                                                        resp_check = self.order_manager._http.request("get_open_orders", category="spot", symbol=symbol)
                                                        if int(resp_check.get("retCode", -1)) == 0:
                                                            remaining = (resp_check.get("result", {}) or {}).get("list", [])
                                                            if not remaining:
                                                                self.logger.info(f"PANIC SELL: All orders cancelled for {symbol}")
                                                                break
                                                        time.sleep(0.2)
                                                    except Exception:
                                                        pass
                                            else:
                                                self.logger.info("PANIC SELL: Cancellation disabled; skipping order cancel phase")
                                            
                                            # Wait for balance refresh
                                            time.sleep(1.0)
                                
                                # Get available balance and sell at market
                                available_qty = self.order_manager.get_available_base_qty(symbol, max_wait_s=3.0)
                                if available_qty > 0:
                                    if getattr(self.config, "cancel_orders_enabled", True):
                                        sell_resp = self.order_manager.close_position_market(symbol, available_qty)
                                    else:
                                        self.logger.info(f"Close disabled; skipping market close for {symbol}")
                                    if sell_resp:
                                        try:
                                            close_id = str(sell_resp.get("orderId") or sell_resp.get("result", {}).get("orderId")) if isinstance(sell_resp, dict) else None
                                        except Exception:
                                            close_id = None
                                        
                                        close_price = last_price
                                        if close_id:
                                            row = self.order_manager.wait_for_filled(close_id, timeout_s=5.0)
                                            try:
                                                if isinstance(row, dict):
                                                    cp = row.get("avgPrice") or row.get("price")
                                                    if cp is not None:
                                                        close_price = float(cp)
                                            except Exception:
                                                pass
                                        
                                        # Update or create trade record in DB
                                        try:
                                            # Try to find existing trade record
                                            existing_trade = None
                                            try:
                                                conn = self.db._get_conn()
                                                cur = conn.cursor()
                                                cur.execute("SELECT * FROM trades WHERE order_id = ? LIMIT 1", (order_id,))
                                                row = cur.fetchone()
                                                if row:
                                                    existing_trade = row
                                            except Exception:
                                                pass
                                            
                                            if existing_trade:
                                                # Update existing trade
                                                self.db.set_trade_close_info(order_id, close_id, close_price)
                                                pnl = (close_price - entry_price) * available_qty
                                                self.db.close_trade(order_id, pnl)
                                            else:
                                                # Create new trade record
                                                self.db.insert_trade(
                                                    symbol_id=symbol_id,
                                                    order_id=order_id,
                                                    side="Buy",
                                                    quantity=available_qty,
                                                    entry_price=entry_price,
                                                    take_profit_price=0.0,
                                                    status="closed",
                                                    close_order_id=close_id,
                                                    close_price=close_price
                                                )
                                            
                                            self.logger.info(f"PANIC SELL COMPLETED: {symbol} qty={available_qty} entry={entry_price:.6f} close={close_price:.6f}")
                                            
                                        except Exception as e:
                                            self.logger.error(f"Error updating trade record for panic sell: {e}")
                                        
                                        try:
                                            self.notifier.send_telegram(f"✅ PANIC SELL COMPLETED: {symbol} qty={available_qty} entry={entry_price:.6f} close={close_price:.6f}")
                                        except Exception:
                                            pass
                                    else:
                                        self.logger.error(f"PANIC SELL FAILED: {symbol} - could not place market sell order")
                                        try:
                                            self.notifier.send_telegram(f"❌ PANIC SELL FAILED: {symbol} - could not place market sell order")
                                        except Exception:
                                            pass
                                else:
                                    self.logger.error(f"PANIC SELL FAILED: {symbol} - no available balance")
                                    try:
                                        self.notifier.send_telegram(f"❌ PANIC SELL FAILED: {symbol} - no available balance")
                                    except Exception:
                                        pass
                                        
                            except Exception as e:
                                self.logger.error(f"Error executing panic sell for {symbol}: {e}")
                                try:
                                    self.notifier.send_telegram(f"❌ PANIC SELL ERROR: {symbol} - {e}")
                                except Exception:
                                    pass
                        
                    except Exception as e:
                        self.logger.error(f"Error processing order {order.get('orderId', 'unknown')} for panic sell: {e}")
                        continue
                        
            except Exception as e:
                self.logger.error(f"API error in panic sell monitoring: {e}")
                
        except Exception as e:
            self.logger.error(f"Panic sell monitoring error: {e}")

    def _close_position_with_pnl(
        self,
        trade,
        symbol: str,
        close_price: float,
        close_reason: str,
        close_order_id: str | None = None,
    ) -> None:
        """Unified close: calculates net PnL with fees, updates DB, cancels residual TP/SL, and notifies."""
        try:
            try:
                fee_rate = float(self.config.fee_rate)
            except Exception:
                fee_rate = 0.001
            fee_entry = trade.entry_price * trade.quantity * fee_rate
            fee_exit = close_price * trade.quantity * fee_rate
            pnl_net = (close_price - trade.entry_price) * trade.quantity - (fee_entry + fee_exit)
            self.db.close_trade(trade.order_id, pnl_net)
            if close_order_id:
                try:
                    self.db.set_trade_close_info(trade.order_id, close_order_id, close_price)
                    self.db.update_trade_fees(trade.order_id, fee_exit=fee_exit)
                except Exception:
                    pass
            # best-effort cancel residual TP (exchange SL deprecated) — optional
            try:
                if getattr(self.config, "cancel_orders_enabled", True):
                    if getattr(trade, "tp_order_id", None):
                        if not (close_order_id and str(close_order_id) == str(trade.tp_order_id)):
                            self.order_manager.cancel_order(symbol, str(trade.tp_order_id))
                else:
                    self.logger.debug("Cancellation disabled; skipping residual TP cancel")
            except Exception:
                pass
            notional = trade.entry_price * trade.quantity
            pct = (pnl_net / notional * 100.0) if notional > 0 else 0.0
            msg = f"{close_reason}: {symbol} qty={trade.quantity} close={close_price:.6f} pnl_net={pnl_net:.4f} ({pct:.2f}%)"
            try:
                self.notifier.send_telegram(msg)
            except Exception:
                pass
            self.logger.info(msg)
        except Exception as e:
            self.logger.error(f"Unified close error {symbol}: {e}")

    def _log_account_status_on_start(self) -> None:
        """Log Bybit account connectivity and available USDT balance on startup (trade mode)."""
        try:
            report = self._build_rate_report()
            try:
                # Print to console for immediate visibility
                print(report)
            except Exception:
                pass
            for line in (report or "").splitlines():
                self.logger.info(line)
        except Exception as e:
            self.logger.warning(f"Account status check failed: {e}")

    # ---- Public API ----
    def start_monitoring(self) -> None:
        """Запуск всех потоков мониторинга."""
        if self.is_running:
            self.logger.warning("Monitor already running")
            return

        self._runtime_emergency_stop = False
        self._loss_notify_sent = False
        self.logger.info(f"Starting market monitoring... DB: {self.config.database_path}")
        self._stop_event.clear()

        if self.config.switch_mode == "split":
            # For now, only start Telegram listener and a placeholder thread
            try:
                self.tg_listener.start()
            except Exception as e:
                self.logger.error(f"Failed to start Telegram listener: {e}")
            # Initialize detectors for active symbols
            try:
                # Ensure symbols mapping is created/refreshed for split mode
                try:
                    count = self.symbol_mapper.create_symbol_mapping()
                    self.logger.info(f"Split: symbol map (re)initialized with {count} entries")
                except Exception as e:
                    self.logger.error(f"Split: failed to create symbol mapping: {e}")
                if self.config.split_trading_pairs:
                    raw = self.config.split_trading_pairs
                    wl = [s.strip().upper() for s in (raw.split(",") if raw else []) if s.strip()]
                    all_map = {rec.spot_symbol.upper(): rec.spot_symbol for rec in self.db.get_active_symbols()}
                    symbols = [all_map[s] for s in wl if s in all_map]
                    if not symbols:
                        self.logger.warning("SPLIT_TRADING_PAIRS provided but none matched mapping; falling back to all active symbols")
                        symbols = [rec.spot_symbol for rec in self.db.get_active_symbols()]
                else:
                    symbols = [rec.spot_symbol for rec in self.db.get_active_symbols()]
                for sym in symbols:
                    self._split_detectors[sym] = SpikeDetector(self.config, order_manager=self.order_manager)
                if symbols:
                    # Subscribe tickers and orderbook/trades; route to split handlers
                    self.spot.subscribe_tickers_with_callback(symbols, self._on_split_ticker)
                    try:
                        # Access underlying ws handler to subscribe extra channels
                        from bybit_trading_bot.handlers.websocket_handler import WebSocketHandler as _WS
                        if isinstance(self.spot.ws, _WS):
                            self.spot.ws.subscribe_orderbook_and_trades(symbols, on_orderbook=self._on_split_orderbook, on_trade=self._on_split_trade)  # type: ignore[attr-defined]
                    except Exception as e:
                        self.logger.debug(f"Split extra subscriptions failed: {e}")
            except Exception as e:
                self.logger.error(f"Split init failed: {e}")
            self._threads = [
                threading.Thread(target=self._split_loop, name="SplitMode", daemon=True),
            ]
            for t in self._threads:
                t.start()
            self.is_running = True
            self.logger.info("MarketMonitor started (split mode)")
            return

        # Scalp 5m mode branch
        if self.config.switch_mode == "scalp_5m":
            try:
                from bybit_trading_bot.core.scalp5m_engine import Scalp5mEngine
            except Exception as e:
                self.logger.error(f"Failed to import Scalp5mEngine: {e}")
                return
            try:
                engine = Scalp5mEngine(self.config, self.db, self.notifier)
                # persist engine instance for graceful stop
                self._scalp_engine = engine  # type: ignore[attr-defined]
                t = threading.Thread(target=engine.run, name="Scalp5mEngine", daemon=True)
                self._threads = [t]
                t.start()
                self.is_running = True
                self.logger.info("MarketMonitor started (scalp_5m mode)")
                return
            except Exception as e:
                self.logger.error(f"Failed to start scalp_5m engine: {e}")
                return

        # Build mapping once at startup
        try:
            count = self.symbol_mapper.create_symbol_mapping()
            self.logger.info(f"Symbol map initialized with {count} entries")
        except Exception as e:
            self.logger.error(f"Failed to create symbol mapping: {e}")

        # Trade mode: print account connectivity and available balance
        try:
            if self.config.switch_mode == "trade":
                self._log_account_status_on_start()
        except Exception as e:
            self.logger.debug(f"Startup account status log failed: {e}")

        # Subscribe to spot tickers (best-effort, list may be empty offline)
        try:
            symbols = [rec.spot_symbol for rec in self.db.get_active_symbols()]
            if symbols:
                self.spot.subscribe_tickers_with_callback(symbols, self._on_spot_ticker)
                try:
                    # also subscribe public trades and orderbook to collect side-aware trades
                    self.spot.ws.subscribe_orderbook_and_trades(symbols, on_orderbook=self._on_orderbook_quote, on_trade=self._on_public_trade)
                except Exception:
                    pass
                # RVOL uses public trades; disable for trade mode per request
                if getattr(self.config, "enable_rvol_filter", False) and self.config.switch_mode != "trade":
                    try:
                        from bybit_trading_bot.handlers.websocket_handler import WebSocketHandler as _WS
                        if isinstance(self.spot.ws, _WS):
                            self.spot.ws.subscribe_orderbook_and_trades(symbols, on_orderbook=self._on_orderbook_quote, on_trade=self._on_trade_tick)  # type: ignore[attr-defined]
                    except Exception:
                        pass
        except Exception as e:
            self.logger.error(f"Failed to subscribe tickers: {e}")

        # Start Telegram commands listener
        try:
            self.tg_listener.start()
        except Exception as e:
            self.logger.error(f"Failed to start Telegram listener: {e}")

        # Запустить Software SL Manager (если не отключен STOP_SL)
        try:
            if not getattr(self.config, "stop_sl", False):
                self.software_sl.start()
            else:
                self.logger.info("Software SL is disabled by STOP_SL flag")
        except Exception as e:
            self.logger.error(f"Failed to start Software SL Manager: {e}")

        # Start OCO monitoring if enabled
        try:
            if getattr(self.config, "oco_enabled", False):
                self.oco_manager.start_monitoring()
                self.logger.info("OCO monitoring started")
            else:
                self.logger.info("OCO is disabled")
        except Exception as e:
            self.logger.error(f"Failed to start OCO Manager: {e}")

        # Initial backfill sync
        # Removed initial order sync to revert last change

        self._threads = [
            threading.Thread(target=self._run_spot_ws, name="SpotWS", daemon=True),
            threading.Thread(target=self._run_oi_poll, name="OIPoll", daemon=True),
            threading.Thread(target=self._run_analyzer, name="Analyzer", daemon=True),
            threading.Thread(target=self._run_executor, name="Executor", daemon=True),
            threading.Thread(target=self._run_api_sync, name="APISync", daemon=True),
        ]

        for t in self._threads:
            t.start()

        self.is_running = True
        self.logger.info("MarketMonitor started")

        # Test purchase at startup disabled by request

    def stop_monitoring(self) -> None:
        """Корректная остановка всех процессов."""
        if not self.is_running:
            return

        self.logger.info("Stopping market monitoring...")
        self._stop_event.set()
        # stop scalp engine if running
        try:
            eng = getattr(self, "_scalp_engine", None)
            if eng is not None and hasattr(eng, "stop"):
                eng.stop()
        except Exception:
            pass
        try:
            self.tg_listener.stop()
        except Exception:
            pass
        for t in self._threads:
            if t.is_alive():
                t.join(timeout=5)

        self._threads.clear()
        self.is_running = False
        self.logger.info("MarketMonitor stopped")

        # Остановить Software SL Manager
        try:
            self.software_sl.stop()
        except Exception:
            pass

        # Stop OCO monitoring
        try:
            self.oco_manager.stop_monitoring()
        except Exception:
            pass

    def _can_open_new_position(self, symbol_id: int) -> bool:
        if self.db.count_open_positions() >= self.config.max_simultaneous_positions:
            return False
        if self.db.has_open_position(symbol_id):
            return False
        cooldown = timedelta(minutes=max(1, int(self.config.trade_cooldown_minutes)))
        last_trade = self.db.get_last_trade_time(symbol_id)
        if last_trade and (datetime.utcnow() - last_trade) < cooldown:
            return False
        return True

    def check_trading_conditions(self, symbol_id: int) -> Tuple[bool, float, float]:
        """Проверка условий: цена +X% И OI +Y%.

        OI считается по времени относительно 5‑минутных баров: берём текущее значение и
        значение на момент (now - N*5 минут), где N = SIGNAL_WINDOW_MINUTES // 5.
        Если подходящей точки нет, используем первую в окне; при недостатке точек возвращаем 0%.
        """
        price_series = self.db.get_recent_price_series(symbol_id, minutes=self.config.signal_window_minutes)
        oi_series = self.db.get_recent_oi_series(symbol_id, minutes=self.config.signal_window_minutes)
        price_change = calculate_percentage_change(price_series)

        # Momentum mode: early impulse detection prioritizes early gradient over legacy dual condition
        try:
            if getattr(self.config, "momentum_mode_enabled", False) or getattr(self.config, "switch_mode", "").lower() == "momentum":
                if GradientMomentumDetector is None:
                    return False, price_change, 0.0
                detector = GradientMomentumDetector(self.config)
                grad, acc = detector.calculate_momentum_gradient(price_series)
                early_ok = detector.validate_early_signal(grad, None)
                # Optionally require acceleration positive when available
                if early_ok and acc is not None and acc <= 0:
                    early_ok = False
                # Exhaustion guardrails with real inputs
                if MomentumExhaustionDetector is not None:
                    ex = MomentumExhaustionDetector(self.config)
                    # Resolve symbol string for notifications/logs
                    try:
                        _recs = self.db.get_active_symbols()
                        _rec = next((r for r in _recs if int(r.id) == int(symbol_id)), None)
                        _symbol = _rec.spot_symbol if _rec else f"id={symbol_id}"
                    except Exception:
                        _symbol = f"id={symbol_id}"
                    # RSI from recent prices
                    rsi_block = False
                    try:
                        prices_only = [p for (_, p) in price_series]
                        if len(prices_only) >= max(16, int(self.config.rsi_period) + 1):
                            rsi_vals = calculate_rsi(prices_only, period=int(self.config.rsi_period))
                            if rsi_vals:
                                rsi_block = ex.detect_rsi_exhaustion(rsi_vals[-1], +1)
                    except Exception:
                        rsi_block = False
                    # Volume exhaustion: use in-memory minute volumes
                    vol_block = False
                    try:
                        buckets = self._minute_volumes.get(_symbol)
                        if buckets:
                            mins = sorted(buckets.keys())
                            if len(mins) >= 3:
                                last_min = mins[-1]
                                current_vol = float(buckets.get(last_min, 0.0))
                                period = max(3, int(getattr(self.config, "rvol_period", 20)))
                                prev_mins = [m for m in mins if m < last_min][-period:]
                                if prev_mins:
                                    avg_vol = sum(float(buckets[m]) for m in prev_mins) / float(len(prev_mins))
                                    vol_block = ex.detect_volume_exhaustion(current_vol, avg_vol)
                    except Exception:
                        vol_block = False
                    # Candle exhaustion: use 1m candle body size vs average bodies
                    candle_block = False
                    try:
                        # rebuild simple 1m candles from price_series assuming 1m spacing
                        if len(price_series) >= 6:
                            closes = [price_series[i][1] for i in range(len(price_series))]
                            bodies = [abs(closes[i] - closes[i-1]) for i in range(1, len(closes))]
                            body = bodies[-1]
                            lookback = max(5, int(getattr(self.config, "volume_lookback_periods", 20)))
                            ref = bodies[-(lookback+1):-1] if len(bodies) > lookback else bodies[:-1]
                            if ref:
                                avg_body = sum(ref) / float(len(ref))
                                candle_block = ex.detect_candle_exhaustion(body, avg_body)
                    except Exception:
                        candle_block = False
                    # Divergence check is optional; needs momentum series. Skip for now.
                    # Require at least two independent reasons to block to reduce over-filtering
                    block_reasons = []
                    if rsi_block:
                        block_reasons.append("RSI")
                    if vol_block:
                        block_reasons.append("Volume")
                    if candle_block:
                        block_reasons.append("Candle")
                    # Early analytics log: emit when noteworthy (grad>=0.05 or any exhaustion reason)
                    try:
                        noteworthy = (grad is not None and float(grad) >= 0.05) or rsi_block or vol_block or candle_block
                    except Exception:
                        noteworthy = False
                    if noteworthy:
                        try:
                            # Compute body ratio for context if available
                            _body_ratio = None
                            try:
                                if 'body' in locals() and 'avg_body' in locals() and avg_body and avg_body > 0:
                                    _body_ratio = body / avg_body
                            except Exception:
                                _body_ratio = None
                            self.logger.info(
                                f"ANALYTICS | EARLY | symbol={_symbol} grad={grad} acc={acc} "
                                f"rsi_block={rsi_block} candle_block={candle_block} vol_block={vol_block} "
                                f"bodyx={_body_ratio if _body_ratio is not None else 'NA'} early_ok={early_ok}"
                            )
                        except Exception:
                            pass

                    if len(block_reasons) >= 2:
                        # Stateful consecutive block policy: require 3 consecutive cycles, then 90s cooldown
                        try:
                            now_ts = time.time()
                            state = self._momentum_block_state.setdefault(
                                _symbol, {"last_reasons": None, "count": 0, "cooldown_until": 0.0}
                            )
                            cooldown_until = float(state.get("cooldown_until", 0.0) or 0.0)
                            if now_ts < cooldown_until:
                                # During cooldown, do not block to avoid permanent suppression
                                pass
                            else:
                                key = tuple(sorted(set(block_reasons)))
                                if state.get("last_reasons") == key:
                                    state["count"] = int(state.get("count", 0)) + 1
                                else:
                                    state["last_reasons"] = key
                                    state["count"] = 1
                                if int(state["count"]) >= 3:
                                    try:
                                        self.logger.info(
                                            f"ANALYTICS | BLOCK | symbol={_symbol} reasons={','.join(block_reasons)} consecutive=3 cooldown=90s"
                                        )
                                    except Exception:
                                        pass
                                    state["cooldown_until"] = now_ts + 90.0
                                    early_ok = False
                        except Exception:
                            # If state handling fails, keep previous relaxed behavior (no immediate block here)
                            pass
                # Order flow delta confirmation (institutional flow)
                try:
                    from bybit_trading_bot.momentum_enhancements.order_flow_delta import OrderFlowDeltaAnalyzer
                    if getattr(self.config, "order_flow_momentum_filter", True):
                        recs = self.db.get_active_symbols()
                        rec = next((r for r in recs if int(r.id) == int(symbol_id)), None)
                        sym = rec.spot_symbol if rec else None
                        trades = self._recent_trades.get(sym, []) if sym else []
                        of = OrderFlowDeltaAnalyzer(self.config)
                        dr = of.calculate_delta_volume(trades)
                        if not of.validate_institutional_flow(dr.imbalance, dr.aggression_ratio):
                            try:
                                self.logger.info(
                                    f"ANALYTICS | BLOCK | symbol={sym or symbol_id} reasons=OrderFlow"
                                    f" imb={dr.imbalance} agg={dr.aggression_ratio}"
                                )
                            except Exception:
                                pass
                            early_ok = False
                except Exception:
                    pass

                # Relax block policy: require at least 2 independent reasons to block
                # (we already combined rsi/volume/candle into early_ok above; double-check here)
                # In momentum mode we only require early_ok; OI is advisory
                oi_change = calculate_percentage_change(oi_series)
                return early_ok, price_change, oi_change
        except Exception as _e:  # fallback to legacy flow on any error
            pass

        # RSI filter (trade mode): block buys on overbought
        if getattr(self.config, "enable_rsi_filter", False) and self.config.switch_mode == "trade":
            try:
                prices_only = [p for (_, p) in price_series]
                if len(prices_only) >= max(16, int(self.config.rsi_period) + 1):
                    rsi_vals = calculate_rsi(prices_only, period=int(self.config.rsi_period))
                    if rsi_vals:
                        last_rsi = rsi_vals[-1]
                        if last_rsi >= float(self.config.trade_rsi_overbought):
                            return False, price_change, 0.0
            except Exception:
                pass

        # MACD filter (trade mode): confirm trend, with breakout override
        if getattr(self.config, "enable_macd_filter", False) and self.config.switch_mode == "trade":
            try:
                prices_only = [p for (_, p) in price_series]
                fast = int(self.config.macd_fast)
                slow = int(self.config.macd_slow)
                sig = int(self.config.macd_signal)
                min_bars = max(1, int(self.config.trade_macd_min_bars))
                if len(prices_only) >= (slow + sig + min_bars):
                    macd_line, signal_line = calculate_macd(prices_only, fast=fast, slow=slow, signal=sig)
                    if macd_line and signal_line:
                        # breakout override: if price breakout strong, skip MACD filter
                        breakout_ok = price_change >= self.config.price_only_breakout_threshold
                        if not breakout_ok:
                            # require last N bars macd > signal and optionally > 0
                            ok = True
                            for i in range(1, min_bars + 1):
                                if i > len(macd_line) or i > len(signal_line):
                                    ok = False
                                    break
                                if getattr(self.config, "trade_macd_require_above_signal", True):
                                    if macd_line[-i] <= signal_line[-i]:
                                        ok = False
                                        break
                                if getattr(self.config, "trade_macd_require_above_zero", True):
                                    if macd_line[-i] <= 0:
                                        ok = False
                                        break
                            if not ok:
                                return False, price_change, 0.0
            except Exception:
                pass

        # RVOL filter is disabled for trade mode per request
        if getattr(self.config, "enable_rvol_filter", False) and self.config.switch_mode != "trade":
            try:
                symbol = self.db._get_spot_symbol_by_id(symbol_id) or ""
                if symbol:
                    rvol = self._compute_rvol(symbol, period=int(self.config.rvol_period))
                    breakout_ok = price_change >= self.config.price_only_breakout_threshold
                    threshold = float(self.config.rvol_breakout_min if breakout_ok else self.config.rvol_threshold)
                    if rvol is None or rvol < threshold:
                        return False, price_change, 0.0
                    # Optional absolute liquidity guard over last 5 minutes
                    min_qv = float(self.config.min_quote_volume_5m_usdt)
                    if min_qv > 0:
                        vol5 = self._sum_quote_volume(symbol, 5)
                        if vol5 < min_qv:
                            return False, price_change, 0.0
            except Exception:
                pass

        # Дедуп OI по 5м барам (квантование по времени) и расчёт с учётом настроек
        def _dedup_5m_bars(series: List[Tuple[datetime, float]]) -> List[Tuple[int, float]]:
            seen: set[int] = set()
            out: List[Tuple[int, float]] = []
            for ts, val in series:
                try:
                    bar = int((ts.timestamp() * 1000) // (5 * 60_000))
                except Exception:
                    continue
                if bar in seen:
                    continue
                seen.add(bar)
                out.append((bar, float(val)))
            return out

        bars = _dedup_5m_bars(oi_series)
        n_bars = max(1, int(self.config.signal_window_minutes) // 5)
        oi_change: float | None = None
        if len(bars) >= max(self.config.min_unique_oi_bars, n_bars + 1):
            last_oi = float(bars[-1][1])
            prev_oi = float(bars[-(n_bars + 1)][1])
            oi_change = 0.0 if prev_oi == 0 else (last_oi - prev_oi) / prev_oi * 100.0
        elif bars:
            # мягкая оценка по имеющимся точкам
            seq = [v for _, v in bars]
            from bybit_trading_bot.core.data_processor import calculate_percentage_change_from_series
            oi_change = calculate_percentage_change_from_series(seq)

        # Проверки OI-ограничений/деградации
        oi_ok = True
        if oi_change is None and self.config.require_oi_for_signal:
            oi_ok = False
        if oi_change is not None and oi_change < self.config.oi_negative_block_threshold:
            oi_ok = False

        if self.config.price_only_mode:
            return (price_change >= self.config.price_change_threshold and oi_ok), price_change, oi_change or 0.0
        # Лестница условий: breakout price-only, затем строгий AND
        if price_change >= self.config.price_only_breakout_threshold and oi_ok:
            return True, price_change, oi_change or 0.0
        ok = (
            oi_ok
            and price_change >= self.config.price_change_threshold
            and (oi_change is not None)
            and (oi_change >= self.config.oi_change_threshold)
        )
        return ok, price_change, oi_change or 0.0

    def execute_trade_signal(self, symbol_id: int, price_change: float, oi_change: float) -> None:
        """Выполнение торговой сделки при срабатывании сигнала с OCO ордером на покупку."""
        try:
            if not self._can_open_new_position(symbol_id):
                return
            symbol_rec = next((r for r in self.db.get_active_symbols() if r.id == symbol_id), None)
            if not symbol_rec:
                return
            symbol = symbol_rec.spot_symbol
            last_price = self.db.get_last_price(symbol_id) or 0.0
            if last_price <= 0.0:
                return
            qty = self.order_manager.calculate_position_size(
                symbol=symbol,
                current_price=last_price,
                account_equity_usdt=self.config.account_equity_usdt,
            )
            if qty <= 0.0:
                return
            # API-level guard: skip if there are already open spot orders for this symbol
            try:
                if self.order_manager.has_open_spot_order(symbol):
                    self.logger.warning(f"Skip placing order for {symbol}: open spot order exists (API)")
                    try:
                        self.notifier.send_telegram(f"SKIP: {symbol} has an existing open order (API)")
                    except Exception:
                        pass
                    return
            except Exception:
                pass
            # If OCO is enabled, place a single Buy order with embedded TP/SL and exit
            oco_enabled = getattr(self.config, "oco_enabled", False)
            if oco_enabled:
                try:
                    oco_profit_pct = getattr(self.config, "take_profit_percent", getattr(self.config, "oco_profit_percentage", 2.0))
                    oco_loss_pct = getattr(self.config, "oco_loss_percentage", 2.0)
                    oco_result = self.oco_manager.place_oco_orders(
                        symbol=symbol,
                        quantity=qty,
                        entry_price=last_price,
                        profit_percentage=oco_profit_pct,
                        loss_percentage=oco_loss_pct,
                    )
                    if not oco_result.get('success'):
                        self.notifier.send_telegram(f"ORDER FAILED (OCO): {symbol} — {oco_result.get('message')}")
                        return
                    # Save OCO trade record
                    try:
                        order_id = str(oco_result.get('order_id'))
                        self.db.insert_trade(symbol_id, order_id, "Buy", qty, last_price, 0.0, "open", stop_loss_price=None, tp_order_id=order_id, is_oco_order=True)
                        self.logger.info(f"OCO TRADE SAVED TO DB: {symbol} order_id={order_id} entry={last_price:.6f} qty={qty}")
                    except Exception as e:
                        self.logger.error(f"FAILED TO SAVE OCO TRADE TO DB: {symbol} error={e}")
                    # Do not duplicate Telegram placement notice here; OCOManager already sends it
                except Exception as e:
                    self.logger.error(f"Failed to place OCO order for {symbol}: {e}")
                return

            # Legacy path: place market/limit buy then manage TP normally
            tp_price = last_price * (1.0 + self.config.take_profit_percent / 100.0)
            order = self.order_manager.place_spot_order(
                symbol,
                "Buy",
                qty,
                tp_price,
                reference_price=last_price,
            )
            if not order:
                self.notifier.send_telegram(f"ORDER FAILED: {symbol}")
                return
            order_id = str(order.get("orderId") or order.get("result", {}).get("orderId") or f"UNKWN-{symbol}")
            # try to wait fill briefly to get exact filled qty and avgPrice
            filled = self.order_manager.wait_for_filled(order_id, timeout_s=6.0)
            if filled and isinstance(filled, dict):
                try:
                    avg = float(filled.get("avgPrice") or filled.get("price") or last_price)
                except Exception:
                    avg = last_price
                try:
                    fqty = float(filled.get("cumExecQty") or filled.get("qty") or qty)
                except Exception:
                    fqty = qty
                # fees (approx entry)
                try:
                    fee_rate = float(self.config.fee_rate)
                    fee_entry = avg * fqty * fee_rate
                except Exception:
                    fee_entry = None
                if fqty > 0.0:
                    safe_qty = self.order_manager.adjust_qty_for_safety(symbol, fqty)
                    
                    # Check if OCO is enabled
                    oco_enabled = getattr(self.config, "oco_enabled", False)
                    
                    if oco_enabled:
                        # Use OCO orders instead of regular TP
                        # Use the same TP percent as the system uses for signals, not OCO_PROFIT_PERCENTAGE
                        oco_profit_pct = getattr(self.config, "take_profit_percent", getattr(self.config, "oco_profit_percentage", 2.0))
                        oco_loss_pct = getattr(self.config, "oco_loss_percentage", 2.0)
                        
                        oco_result = self.oco_manager.place_oco_orders(
                            symbol=symbol,
                            quantity=safe_qty,
                            entry_price=avg,
                            profit_percentage=oco_profit_pct,
                            loss_percentage=oco_loss_pct
                        )
                        
                        if not oco_result['success']:
                            try:
                                if getattr(self.config, "cancel_orders_enabled", True):
                                    self.order_manager.close_position_market(symbol, safe_qty)
                            except Exception:
                                pass
                            self.notifier.send_telegram(f"EMERGENCY CLOSE: {symbol} - failed to place OCO orders: {oco_result['message']}")
                            return
                        
                        # Create trade record with OCO order ID
                        try:
                            self.db.insert_trade(symbol_id, order_id, "Buy", safe_qty, avg, 0.0, "open", stop_loss_price=None, tp_order_id=oco_result['order_id'], is_oco_order=True)
                            self.logger.info(f"OCO TRADE SAVED TO DB: {symbol} order_id={order_id} entry={avg:.6f} qty={safe_qty} oco_id={oco_result['order_id']}")
                        except Exception as e:
                            self.logger.error(f"FAILED TO SAVE OCO TRADE TO DB: {symbol} order_id={order_id} error={e}")
                        
                        # Do not send separate SL placement notification to avoid duplicates
                    
                    else:
                        # Use regular TP (existing logic)
                        tp_resp = self.order_manager.place_tp_limit(symbol, safe_qty, tp_price)
                        if not tp_resp:
                            try:
                                if getattr(self.config, "cancel_orders_enabled", True):
                                    self.order_manager.close_position_market(symbol, safe_qty)
                            except Exception:
                                pass
                            self.notifier.send_telegram(f"EMERGENCY CLOSE: {symbol} - failed to place TP")
                            return
                        # Create trade only after TP placed successfully
                        try:
                            tp_id = str(tp_resp.get("orderId") or tp_resp.get("result", {}).get("orderId") ) if tp_resp else None
                            self.db.insert_trade(symbol_id, order_id, "Buy", safe_qty, avg, tp_price, "open", stop_loss_price=None, tp_order_id=tp_id)
                            self.logger.info(f"TRADE SAVED TO DB: {symbol} order_id={order_id} entry={avg:.6f} qty={safe_qty} tp_id={tp_id}")
                        except Exception as e:
                            tp_id = None
                            self.logger.error(f"FAILED TO SAVE TRADE TO DB: {symbol} order_id={order_id} error={e}")
                    # update entry qty/fees
                    try:
                        self.db.update_trade_entry_qty(order_id, avg, safe_qty)
                        if fee_entry is not None:
                            self.db.update_trade_fees(order_id, fee_entry=fee_entry)
                        self.logger.info(f"TRADE UPDATED IN DB: {symbol} order_id={order_id} entry={avg:.6f} qty={safe_qty}")
                    except Exception as e:
                        self.logger.error(f"FAILED TO UPDATE TRADE IN DB: {symbol} order_id={order_id} error={e}")
                    # Добавить Software SL (если не отключен STOP_SL)
                    try:
                        if self.config.place_exchange_sl and not getattr(self.config, "stop_sl", False):
                            r = float(self.config.fee_rate)
                            be_price = avg * (1.0 + r) / max(1e-12, (1.0 - r))
                            self.software_sl.add_sl_position(
                                trade_id=order_id,
                                symbol=symbol,
                                quantity=safe_qty,
                                entry_price=avg,
                                sl_price=be_price,
                            )
                            delay_s = float(getattr(self.config, "software_sl_activation_delay_seconds", 0.0))
                            if delay_s > 0:
                                self.notifier.send_telegram(
                                    f"SOFTWARE SL SCHEDULED: {symbol} trigger={be_price:.6f} starts_in={int(delay_s)}s"
                                )
                            else:
                                self.notifier.send_telegram(f"SOFTWARE SL ADDED: {symbol} trigger={be_price:.6f}")
                        elif getattr(self.config, "stop_sl", False):
                            self.logger.info(f"STOP_SL active: Software SL not added for {symbol}")
                    except Exception as e:
                        self.logger.debug(f"Add Software SL error {symbol}: {e}")
                    self.db.insert_signal(symbol_id, price_change, oi_change, action_taken="bought")
                    # expected PnL: gross and net (with fees)
                    try:
                        expected_gross = (tp_price - avg) * safe_qty
                        fee_rate = float(self.config.fee_rate)
                        expected_fees = (avg * safe_qty + tp_price * safe_qty) * fee_rate
                        _expected_net = expected_gross - expected_fees
                    except Exception:
                        expected_gross = None
                    # Do not send extra ORDER FILLED telegram; OCO flow will notify on TP/SL
                    # Announce panic-monitor parameters immediately after fill
                    try:
                        panic_enabled = getattr(self.config, "panic_enabled", True)
                        panic_sell_enabled = getattr(self.config, "panic_sell_enabled", False)
                        if panic_enabled and panic_sell_enabled:
                            ps = float(getattr(self.config, "panic_sell_drop_pct", 2.0))
                            trigger = avg * (1.0 - abs(ps) / 100.0)
                            # Add order to notified set to prevent duplicate notifications
                            self._panic_notified_orders.add(order_id)
                            self.notifier.send_telegram(
                                f"PANIC MONITOR ENABLED: {symbol} entry={avg:.6f} drop={ps:.2f}% trigger={trigger:.6f} — market sell will execute even if TP is open"
                            )
                            self.logger.info(f"PANIC MONITOR ENABLED: {symbol} entry={avg:.6f} drop={ps:.2f}% trigger={trigger:.6f}")
                    except Exception:
                        pass
                    self.logger.info(f"ORDER FILLED: {symbol} qty={safe_qty} avg={avg}")
                    return
            avail_qty = self.order_manager.get_available_base_qty(symbol, max_wait_s=6.0)
            if avail_qty <= 0.0:
                avail_qty = self.order_manager.get_filled_base_qty(symbol, order_id, max_wait_s=6.0)
            if avail_qty <= 0.0:
                avail_qty = qty
            safe_qty = self.order_manager.adjust_qty_for_safety(symbol, avail_qty)
            tp_resp = self.order_manager.place_tp_limit(symbol, safe_qty, tp_price)
            if not tp_resp:
                self.notifier.send_telegram(f"TP PLACE FAILED: {symbol}")
            self.db.insert_trade(symbol_id, order_id, "Buy", safe_qty, last_price, tp_price, "open", stop_loss_price=None, tp_order_id=(tp_resp or {}).get("orderId") if tp_resp else None)
            self.logger.info(f"TRADE SAVED TO DB (alt): {symbol} order_id={order_id} entry={last_price:.6f} qty={safe_qty}")
            self.db.insert_signal(symbol_id, price_change, oi_change, action_taken="bought")
            try:
                # remaining open order slots
                max_pos = int(self.config.max_simultaneous_positions)
                open_now = int(self.db.count_open_positions())
                remaining = max(0, max_pos - open_now)
            except Exception:
                remaining = None
            extra = f" | slots_left={remaining}" if remaining is not None else ""
            self.notifier.send_telegram(f"ORDER PLACED: {symbol} qty={safe_qty} entry={last_price:.6f} tp={tp_price:.6f}{extra}")
            # Announce panic-monitor parameters after placement when fill wasn't confirmed
            try:
                panic_enabled = getattr(self.config, "panic_enabled", True)
                panic_sell_enabled = getattr(self.config, "panic_sell_enabled", False)
                if panic_enabled and panic_sell_enabled:
                    ps = float(getattr(self.config, "panic_sell_drop_pct", 2.0))
                    trigger = last_price * (1.0 - abs(ps) / 100.0)
                    # Add order to notified set to prevent duplicate notifications
                    self._panic_notified_orders.add(order_id)
                    self.notifier.send_telegram(
                        f"PANIC MONITOR ENABLED: {symbol} entry≈{last_price:.6f} drop={ps:.2f}% trigger≈{trigger:.6f} — market sell will execute even if TP is open"
                    )
                    self.logger.info(f"PANIC MONITOR ENABLED: {symbol} entry≈{last_price:.6f} drop={ps:.2f}% trigger≈{trigger:.6f}")
            except Exception:
                pass
            self.logger.info(f"✅ ORDER PLACED: {symbol} - Qty: {safe_qty}, TP: {tp_price}")
        except Exception as e:
            self.logger.error(f"Failed to execute trade signal for {symbol_id}: {e}")

    # ---- Callbacks ----
    def _on_spot_ticker(self, symbol: str, price: float, ts_epoch: float) -> None:
        try:
            records = self.db.get_active_symbols()
            rec = next((r for r in records if r.spot_symbol == symbol), None)
            if rec:
                self.db.insert_price(rec.id, price)
        except Exception as e:
            self.logger.debug(f"Failed to persist price for {symbol}: {e}")

    def _on_trade_tick(self, symbol: str, price: float, qty: float, ts: float) -> None:
        """Aggregate per-trade data into 1m quote-volume buckets in memory."""
        try:
            # ts may be in ms; normalize to seconds then to minute index
            tsec = ts / 1000.0 if ts > 1e11 else ts
            minute = int(tsec // 60)
            quote_vol = max(0.0, float(price)) * max(0.0, float(qty))
            buckets = self._minute_volumes.setdefault(symbol, {})
            buckets[minute] = buckets.get(minute, 0.0) + quote_vol
            # trim old buckets beyond max horizon (keep last 120 minutes)
            if len(buckets) > 200:
                threshold = minute - 200
                for k in list(buckets.keys()):
                    if k < threshold:
                        buckets.pop(k, None)
        except Exception:
            pass

    def _on_orderbook_quote(self, symbol: str, best_bid: float, best_ask: float, _bids, _asks) -> None:
        try:
            self._best_quotes[symbol] = (float(best_bid), float(best_ask))
        except Exception:
            pass

    def _on_public_trade(self, symbol: str, price: float, qty: float, ts: float, side: str | None) -> None:
        try:
            arr = self._recent_trades.setdefault(symbol, [])
            s_out = side if isinstance(side, str) else None
            if s_out is None:
                # infer side from orderbook snapshot if available
                qb = self._best_quotes.get(symbol)
                if qb:
                    bid, ask = qb
                    try:
                        if price >= ask:
                            s_out = 'Buy'
                        elif price <= bid:
                            s_out = 'Sell'
                    except Exception:
                        s_out = None
            arr.append((float(price), float(qty), float(ts), s_out))
            if len(arr) > 500:
                del arr[: len(arr) - 500]
        except Exception:
            pass

    def _compute_rvol(self, symbol: str, period: int) -> float | None:
        """Return relative volume ratio = current_minute / avg(previous period minutes)."""
        try:
            buckets = self._minute_volumes.get(symbol)
            if not buckets:
                return None
            minutes_sorted = sorted(buckets.keys())
            if len(minutes_sorted) < period + 1:
                return None
            last_min = minutes_sorted[-1]
            # Collect last period+1 volumes (including current minute)
            vols: List[float] = []
            for m in range(last_min - period, last_min + 1):
                vols.append(float(buckets.get(m, 0.0)))
            base = vols[:-1]
            cur = vols[-1]
            avg = sum(base) / max(1, len(base))
            if avg <= 0:
                return None
            return cur / avg
        except Exception:
            return None

    def _sum_quote_volume(self, symbol: str, minutes_back: int) -> float:
        try:
            buckets = self._minute_volumes.get(symbol)
            if not buckets:
                return 0.0
            minutes_sorted = sorted(buckets.keys())
            if not minutes_sorted:
                return 0.0
            last_min = minutes_sorted[-1]
            total = 0.0
            for m in range(last_min - max(1, minutes_back) + 1, last_min + 1):
                total += float(buckets.get(m, 0.0))
            return total
        except Exception:
            return 0.0

    # ---- Internal workers ----
    def _run_spot_ws(self) -> None:
        self.logger.info("Spot WS worker started")
        while not self._stop_event.is_set():
            time.sleep(1.0)
        self.logger.info("Spot WS worker stopped")

    def _split_loop(self) -> None:
        self.logger.info("Split mode loop started")
        while not self._stop_event.is_set():
            try:
                time.sleep(0.2)
            except Exception:
                time.sleep(0.5)
        self.logger.info("Split mode loop stopped")

    def _on_split_ticker(self, symbol: str, price: float, ts_epoch: float) -> None:
        try:
            det = self._split_detectors.get(symbol)
            if det is None:
                det = SpikeDetector(self.config, order_manager=self.order_manager)
                self._split_detectors[symbol] = det
            prev = self._split_last_price.get(symbol)
            pseudo_vol = 0.0
            if prev and prev > 0:
                try:
                    pseudo_vol = abs(price - prev) / prev
                except Exception:
                    pseudo_vol = 0.0
            self._split_last_price[symbol] = price
            det.update_market_data(price, max(pseudo_vol, 0.0), datetime.utcnow())
            sig = det.generate_trading_signal(symbol)
            if sig and sig.strength >= float(self.config.split_min_signal_strength):
                # Enforce split max open orders
                try:
                    if self.db.count_open_sp_orders() >= int(self.config.split_max_open_orders):
                        return
                except Exception:
                    pass
                # cooldown
                last_sp = self.db.get_last_sp_signal_time(symbol)
                if last_sp is not None and (datetime.utcnow() - last_sp) < timedelta(minutes=self.config.cooldown_minutes_split):
                    return
                signal_id = self.db.insert_sp_signal(
                    timestamp=sig.timestamp,
                    symbol=symbol,
                    signal_type="volume_spike",
                    signal_strength=sig.strength,
                    price=sig.price,
                    volume=sig.volume,
                )
                order_id = det.execute_signal(sig)
                if order_id:
                    self.db.insert_sp_order(signal_id, order_id, symbol, "Buy", (self.config.trade_notional_usdt or 0.0) / max(price, 1e-9), price * (1.0 - float(self.config.limit_order_offset)), status="placed")
                    self.notifier.send_telegram(f"SPLIT ORDER: {symbol} placed limit with TP {self.config.target_profit_pct*100:.2f}%")
        except Exception as e:
            self.logger.error(f"Split ticker handler error for {symbol}: {e}")

    def _on_split_orderbook(self, symbol: str, best_bid: float, best_ask: float, bids: list, asks: list) -> None:
        try:
            det = self._split_detectors.get(symbol)
            if det is None:
                det = SpikeDetector(self.config, order_manager=self.order_manager)
                self._split_detectors[symbol] = det
            det.update_orderbook(best_bid, best_ask, bids=bids, asks=asks, levels=5)
        except Exception as e:
            self.logger.debug(f"Split orderbook error {symbol}: {e}")

    def _on_split_trade(self, symbol: str, price: float, qty: float, ts: float) -> None:
        try:
            # Use trade volumes to enrich volume buffer
            det = self._split_detectors.get(symbol)
            if det is None:
                det = SpikeDetector(self.config, order_manager=self.order_manager)
                self._split_detectors[symbol] = det
            # approximate: push price and qty as volume increment
            det.update_market_data(price, max(qty, 0.0), datetime.utcnow())
        except Exception as e:
            self.logger.debug(f"Split trade error {symbol}: {e}")

    def _run_oi_poll(self) -> None:
        self.logger.info("OI polling worker started")
        interval = max(1, int(self.config.monitoring_interval_seconds))
        while not self._stop_event.is_set():
            try:
                for rec in self.db.get_active_symbols():
                    # Monetary OI
                    oi_val = self.futures.get_open_interest(rec.futures_symbol)
                    # Contracts (tokens) directly from Bybit
                    oi_contracts = self.futures.get_open_interest_contracts(rec.futures_symbol)
                    # Tokens derived from markPrice of the same moment
                    mark = self.futures.get_mark_price(rec.futures_symbol)
                    oi_tokens_mark = None
                    if oi_val is not None and mark is not None and mark > 0:
                        try:
                            oi_tokens_mark = float(oi_val) / float(mark)
                        except Exception:
                            oi_tokens_mark = None
                    # Compute nOI against window
                    noi_percent = None
                    try:
                        series = self.db.get_recent_oi_series(rec.id, minutes=self.config.signal_window_minutes)
                        vals = [v for (_, v) in series]
                        if oi_val is not None:
                            vals.append(float(oi_val))
                        if vals:
                            vmin = min(vals)
                            vmax = max(vals)
                            if vmax > vmin and oi_val is not None:
                                noi_percent = (float(oi_val) - vmin) / (vmax - vmin) * 100.0
                    except Exception:
                        noi_percent = None
                    if oi_val is not None:
                        self.db.insert_oi(
                            rec.id,
                            oi_val,
                            oi_value=oi_val,
                            oi_tokens=oi_contracts,
                            oi_tokens_mark=oi_tokens_mark,
                            noi_percent=noi_percent,
                        )
            except Exception as e:
                self.logger.error(f"OI polling error: {e}")
            time.sleep(interval)
        self.logger.info("OI polling worker stopped")

    def _check_loss_streak_and_stop(self) -> None:
        try:
            # Check if safety50 is enabled
            if not getattr(self.config, "safety50_enabled", True):
                return
            
            # Check 10-consecutive losing
            pnls10 = self.db.get_last_closed_pnls(10)
            if len(pnls10) == 10 and all(p < 0 for p in pnls10):
                if not self._is_emergency():
                    self.logger.error("Emergency stop: 10 consecutive losing trades detected. Trading paused.")
                    if not getattr(self, "_loss_notify_sent", False):
                        self.notifier.send_telegram("EMERGENCY STOP: 10 consecutive losing trades. Trading paused.")
                        self._loss_notify_sent = True
                    self._runtime_emergency_stop = True
                    return
            # Check >50% losing among last 50
            pnls50 = self.db.get_last_closed_pnls(50)
            if len(pnls50) >= 10:  # ensure at least 10 trades before applying ratio rule
                losses = sum(1 for p in pnls50 if p < 0)
                if losses / len(pnls50) > 0.5:
                    if not self._is_emergency():
                        self.logger.error(
                            f"Emergency stop: losing rate {losses}/{len(pnls50)} (>50%). Trading paused."
                        )
                        if not getattr(self, "_loss_notify_sent", False):
                            self.notifier.send_telegram(
                                f"EMERGENCY STOP: Losing rate {losses}/{len(pnls50)} (>50%). Trading paused."
                            )
                            self._loss_notify_sent = True
                        self._runtime_emergency_stop = True
                        return
        except Exception as e:
            self.logger.error(f"Loss-streak/rate check error: {e}")

    def _run_analyzer(self) -> None:
        self.logger.info("Analyzer worker started")
        while not self._stop_event.is_set():
            try:
                if self.config.switch_mode == "tg":
                    time.sleep(1.0)
                    continue
                symbols = self.db.get_active_symbols()
                ok_found = 0
                if self._is_emergency():
                    time.sleep(2.0)
                    continue
                for rec in symbols:
                    ok, pchg, oichg = self.check_trading_conditions(rec.id)
                    # lightweight observability: log when near thresholds
                    if not ok and (pchg >= self.config.price_change_threshold * 0.8 or oichg >= self.config.oi_change_threshold * 0.8):
                        self.logger.debug(f"Near thresholds for {rec.spot_symbol}: price {pchg:.2f}% oi {oichg:.2f}%")
                    # Spike up/down detection and suppression interval (N bars)
                    n_bars = max(1, int(self.config.signal_window_minutes) // 5)
                    # Suppress if last signal < N bars (use timestamps)
                    last_sig_ts = self.db.get_last_signal_time(rec.id)
                    if last_sig_ts is not None:
                        # Convert bars to minutes horizon
                        min_gap = timedelta(minutes=n_bars * 5)
                        if datetime.utcnow() - last_sig_ts < min_gap:
                            continue
                    # В trade-режиме ставим в очередь только валидные положительные сигналы (ok)
                    if ok:
                        with self._signals_lock:
                            self._pending_signals.append(
                                Signal(symbol_id=rec.id, price_change_percent=pchg, oi_change_percent=oichg)
                            )
                        self.db.insert_signal(rec.id, pchg, oichg, action_taken="queued")
                        ok_found += 1
                now = time.time()
                if now - self._last_analyzer_summary >= 30.0:
                    self.logger.info(f"Analyzer summary: scanned={len(symbols)} signals_found={ok_found}")
                    self._last_analyzer_summary = now
                # Periodically check loss streak
                self._check_loss_streak_and_stop()
            except Exception as e:
                self.logger.error(f"Analyzer error: {e}")
            time.sleep(2.0)
        self.logger.info("Analyzer worker stopped")

    def _is_emergency(self) -> bool:
        return self.config.emergency_stop or getattr(self, "_runtime_emergency_stop", False)

    def _monitor_take_profit_once(self) -> None:
        try:
            open_trades = self.db.get_open_trades()
            if not open_trades:
                return
            symbols_by_id = {rec.id: rec.spot_symbol for rec in self.db.get_active_symbols()}
            for tr in open_trades:
                last_price = self.db.get_last_price(tr.symbol_id)
                if last_price is None:
                    continue
                # TP check
                if last_price >= tr.take_profit_price:
                    symbol = symbols_by_id.get(tr.symbol_id, "")
                    if symbol:
                        # Prefer sync with existing TP order if it's already filled
                        try:
                            if getattr(tr, "tp_order_id", None):
                                # For OCO orders we do not rely on legacy order history sync to close
                                if getattr(tr, "is_oco_order", False):
                                    continue
                                row = self.order_manager.get_order_fill_row(str(tr.tp_order_id))
                                if row and str(row.get("orderStatus", "")).lower() in {"filled", "partiallyfilled", "partially_filled"}:
                                    filled_qty = float(row.get("cumExecQty") or row.get("qty") or tr.quantity)
                                    close_price = float(row.get("avgPrice") or row.get("price") or tr.take_profit_price)
                                    # If partial fill: close entire position in one go to avoid double-close on the same trade record
                                    # But skip this for OCO orders as they handle TP/SL automatically
                                    is_oco = getattr(tr, "is_oco_order", False)
                                    self.logger.info(f"TP HIT (SYNC) CHECK: {symbol} filled_qty={filled_qty} quantity={tr.quantity} is_oco_order={is_oco}")
                                    if filled_qty < tr.quantity and not is_oco:
                                        remaining = max(0.0, tr.quantity - filled_qty)
                                        if remaining > 0:
                                            if getattr(self.config, "cancel_orders_enabled", True):
                                                self.logger.info(f"TP HIT (SYNC): Closing remaining position for {symbol} qty={remaining}")
                                                self.order_manager.close_position_market(symbol, remaining)
                                            else:
                                                self.logger.info("TP HIT (SYNC): Close disabled; skipping residual close")
                                    elif is_oco:
                                        self.logger.info(f"TP HIT (SYNC): Skipping auto-close for OCO order {symbol}")
                                    self._close_position_with_pnl(tr, symbol, close_price, "TP HIT (SYNC)", str(row.get("orderId")) if row.get("orderId") else None)
                                    try:
                                        self.software_sl.remove_sl_position(tr.order_id)
                                    except Exception:
                                        pass
                                    continue
                        except Exception:
                            pass
                        resp = None
                        if getattr(self.config, "cancel_orders_enabled", True):
                            resp = self.order_manager.close_position_market(symbol, tr.quantity)
                        if resp:
                            try:
                                close_id = str(resp.get("orderId") or resp.get("result", {}).get("orderId")) if isinstance(resp, dict) else None
                            except Exception:
                                close_id = None
                            # try fetch avgPrice for accurate close
                            close_price = last_price
                            if close_id:
                                row = self.order_manager.wait_for_filled(close_id, timeout_s=5.0)
                                try:
                                    if isinstance(row, dict):
                                        cp = row.get("avgPrice") or row.get("price")
                                        if cp is not None:
                                            close_price = float(cp)
                                except Exception:
                                    pass
                            self._close_position_with_pnl(tr, symbol, close_price, "TP HIT", close_id)
                            try:
                                self.software_sl.remove_sl_position(tr.order_id)
                            except Exception:
                                pass
                        else:
                            # OCO deals: suppress legacy TP close retry noise
                            if not getattr(tr, "is_oco_order", False):
                                self.logger.error(f"TP close failed for {symbol} (order_id={tr.order_id}) - will retry")
                    continue
                # Ensure BE SL exists for filled orders even if initial fill was slow (delayed placement)
                # Legacy exchange SL placement removed

                # Panic sell: fast exit on small drop from entry (e.g., 2%)
                try:
                    panic_enabled = getattr(self.config, "panic_enabled", True)
                    panic_sell_enabled = getattr(self.config, "panic_sell_enabled", False)
                    self.logger.debug(f"PANIC SELL CONFIG: panic_enabled={panic_enabled}, panic_sell_enabled={panic_sell_enabled}, entry_price={tr.entry_price}")
                    if panic_enabled and panic_sell_enabled and tr.entry_price > 0:
                        ps_thresh = float(getattr(self.config, "panic_sell_drop_pct", 2.0))
                        drop_pct_ps = (last_price - tr.entry_price) / tr.entry_price * 100.0
                        # Debug logging
                        symbol_name = symbols_by_id.get(tr.symbol_id, 'UNKNOWN')
                        self.logger.debug(f"PANIC SELL CHECK: {symbol_name} entry={tr.entry_price:.6f} last={last_price:.6f} drop={drop_pct_ps:.2f}% threshold={ps_thresh:.2f}%")
                        # Check if price dropped below threshold (negative drop percentage)
                        if drop_pct_ps <= -abs(ps_thresh):
                            self.logger.warning(f"PANIC SELL TRIGGERED: {symbol_name} drop={drop_pct_ps:.2f}% <= -{ps_thresh:.2f}%")
                            symbol = symbols_by_id.get(tr.symbol_id, "")
                            if symbol:
                                # Notify immediately about panic sell trigger
                                try:
                                    self.notifier.send_telegram(
                                        f"PANIC SELL TRIGGERED: {symbol} drop={drop_pct_ps:.2f}% entry={tr.entry_price:.6f} last={last_price:.6f}. Cancelling TP and selling at market."
                                    )
                                except Exception:
                                    pass
                                self.logger.warning(
                                    f"PANIC SELL TRIGGERED for {symbol}: drop={drop_pct_ps:.2f}% entry={tr.entry_price:.6f} last={last_price:.6f}"
                                )
                                resp = None
                                if getattr(self.config, "cancel_orders_enabled", True):
                                    resp = self.order_manager.close_position_market(symbol, tr.quantity)
                                if resp:
                                    try:
                                        close_id = str(resp.get("orderId") or resp.get("result", {}).get("orderId")) if isinstance(resp, dict) else None
                                    except Exception:
                                        close_id = None
                                    close_price = last_price
                                    if close_id:
                                        row = self.order_manager.wait_for_filled(close_id, timeout_s=5.0)
                                        try:
                                            if isinstance(row, dict):
                                                cp = row.get("avgPrice") or row.get("price")
                                                if cp is not None:
                                                    close_price = float(cp)
                                        except Exception:
                                            pass
                                    self._close_position_with_pnl(tr, symbol, close_price, f"PANIC SELL (drop={drop_pct_ps:.2f}%)", close_id)
                                    try:
                                        self.software_sl.remove_sl_position(tr.order_id)
                                    except Exception:
                                        pass
                                else:
                                    self.logger.error(f"Panic sell failed for {symbol} (order_id={tr.order_id}) - will retry")
                                    try:
                                        self.notifier.send_telegram(f"PANIC SELL FAILED: {symbol} could not close position immediately. Will retry.")
                                    except Exception:
                                        pass
                            continue
                except Exception as e:
                    self.logger.debug(f"Panic sell error: {e}")

                # Emergency drawdown SL: if price falls >= configured threshold from entry, market exit
                try:
                    threshold_pct = float(self.config.drawdown_exit_threshold_pct)
                    if tr.entry_price > 0:
                        drop_pct = (last_price - tr.entry_price) / tr.entry_price * 100.0
                        if drop_pct <= -abs(threshold_pct):
                            symbol = symbols_by_id.get(tr.symbol_id, "")
                            if symbol:
                                resp = None
                                if getattr(self.config, "cancel_orders_enabled", True):
                                    resp = self.order_manager.close_position_market(symbol, tr.quantity)
                                if resp:
                                    try:
                                        close_id = str(resp.get("orderId") or resp.get("result", {}).get("orderId")) if isinstance(resp, dict) else None
                                    except Exception:
                                        close_id = None
                                    close_price = last_price
                                    if close_id:
                                        row = self.order_manager.wait_for_filled(close_id, timeout_s=5.0)
                                        try:
                                            if isinstance(row, dict):
                                                cp = row.get("avgPrice") or row.get("price")
                                                if cp is not None:
                                                    close_price = float(cp)
                                        except Exception:
                                            pass
                                    self._close_position_with_pnl(tr, symbol, close_price, f"DRAWDOWN EXIT (drop={drop_pct:.2f}%)", close_id)
                                    try:
                                        self.software_sl.remove_sl_position(tr.order_id)
                                    except Exception:
                                        pass
                                else:
                                    self.logger.error(f"Drawdown close failed for {symbol} (order_id={tr.order_id}) - will retry")
                            continue
                except Exception as e:
                    self.logger.debug(f"Drawdown SL error: {e}")

                # If we have protective SL/TP orders and they filled outside our polling windows, detect via order history
                try:
                    if getattr(tr, "tp_order_id", None):
                        # Skip legacy sync for OCO trades
                        if getattr(tr, "is_oco_order", False):
                            continue
                        row = self.order_manager.get_order_fill_row(str(tr.tp_order_id))
                        if row and str(row.get("orderStatus", "")).lower() == "filled":
                            symbol = symbols_by_id.get(tr.symbol_id, "")
                            close_price = float(row.get("avgPrice") or row.get("price") or tr.take_profit_price)
                            close_id = str(row.get("orderId")) if row.get("orderId") else None
                            self._close_position_with_pnl(tr, symbol, close_price, "TP HIT (SYNC)", close_id)
                            try:
                                self.software_sl.remove_sl_position(tr.order_id)
                            except Exception:
                                pass
                            continue
                    # Legacy SL order sync removed
                except Exception as e:
                    self.logger.debug(f"Order history sync error: {e}")
        except Exception as e:
            self.logger.error(f"TP/SL monitor error: {e}")

    def _run_executor(self) -> None:
        self.logger.info("Executor worker started")
        last_panic_check = 0.0
        panic_check_interval = 10.0  # Check panic sell every 10 seconds
        
        while not self._stop_event.is_set():
            try:
                current_time = time.time()
                
                if self._is_emergency():
                    with self._signals_lock:
                        self._pending_signals.clear()
                    self._monitor_take_profit_once()
                    now = time.time()
                    if getattr(self.config, "cancel_orders_enabled", True):
                        if now - self._last_sync_time >= 20.0:
                            self.logger.debug(f"SYNC: Starting sync_cancellations (emergency) at {now}")
                            self.order_manager.sync_cancellations()
                            self._last_sync_time = now
                    time.sleep(1.0)
                    continue
                    
                if self.config.switch_mode == "tg":
                    # In tg mode ignore Bybit signals, but still monitor TP/SL
                    # Check panic sell every 10 seconds
                    if current_time - last_panic_check >= panic_check_interval:
                        self._monitor_take_profit_once()
                        self._monitor_panic_sell_from_api()
                        last_panic_check = current_time
                    time.sleep(1.0)
                    continue
                    
                signal_obj: Signal | None = None
                with self._signals_lock:
                    if self._pending_signals:
                        signal_obj = self._pending_signals.pop(0)
                        
                if signal_obj:
                    self.execute_trade_signal(
                        symbol_id=signal_obj.symbol_id,
                        price_change=signal_obj.price_change_percent,
                        oi_change=signal_obj.oi_change_percent,
                    )
                
                # Check panic sell every 10 seconds
                if current_time - last_panic_check >= panic_check_interval:
                    self._monitor_take_profit_once()
                    self._monitor_panic_sell_from_api()
                    last_panic_check = current_time
                    
                now = time.time()
                if getattr(self.config, "cancel_orders_enabled", True):
                    if now - self._last_sync_time >= 20.0:
                        self.logger.debug(f"SYNC: Starting sync_cancellations at {now}")
                        self.order_manager.sync_cancellations()
                        self._last_sync_time = now
                    
                # Periodically check loss streak
                self._check_loss_streak_and_stop()
            except Exception as e:
                self.logger.error(f"Executor error: {e}")
            time.sleep(1.0)
        self.logger.info("Executor worker stopped")

    def _run_api_sync(self) -> None:
        """API synchronization worker - syncs all orders every 10 seconds."""
        self.logger.info("API Sync worker started")
        
        # Initial sync immediately after startup
        try:
            self._sync_all_orders_from_api()
            # Check for panic monitoring after initial sync
            self._check_and_start_panic_monitoring()
        except Exception as e:
            self.logger.error(f"Initial API sync failed: {e}")
        
        while not self._stop_event.is_set():
            try:
                current_time = time.time()
                
                # Sync every 10 seconds
                if current_time - self._last_api_sync_time >= 10.0:
                    self._sync_all_orders_from_api()
                    self._last_api_sync_time = current_time
                
                time.sleep(1.0)
            except Exception as e:
                self.logger.error(f"API sync worker error: {e}")
                time.sleep(5.0)  # Wait longer on error
        
        self.logger.info("API Sync worker stopped")

    def _sync_all_orders_from_api(self) -> None:
        """Sync all orders from API to database."""
        try:
            if not self.order_manager or not hasattr(self.order_manager, '_http') or not self.order_manager._http:
                return
            
            # Get all open spot orders from API
            resp = self.order_manager._http.request("get_open_orders", category="spot")
            if int(resp.get("retCode", -1)) != 0:
                self.logger.warning(f"API sync failed: {resp.get('retCode')} {resp.get('retMsg')}")
                return
            
            open_orders = (resp.get("result", {}) or {}).get("list", [])
            self.logger.debug(f"API SYNC: Found {len(open_orders)} open orders on exchange")
            
            # Get all trades from database
            all_trades = []
            try:
                conn = self.db._get_conn()
                cur = conn.cursor()
                cur.execute("SELECT order_id, symbol_id, status FROM trades ORDER BY created_at DESC")
                all_trades = cur.fetchall()
            except Exception as e:
                self.logger.error(f"API SYNC: Failed to get trades from DB: {e}")
                return
            
            # Create sets for comparison
            api_order_ids = {str(o.get("orderId", "")) for o in open_orders if o.get("orderId")}
            db_order_ids = {str(t["order_id"]) for t in all_trades}
            
            # Find orders that exist in DB but not in API (should be marked as cancelled/filled)
            missing_in_api = db_order_ids - api_order_ids
            
            # Find orders that exist in API but not in DB (should be added to DB)
            missing_in_db = api_order_ids - db_order_ids
            
            # Update status for orders missing in API
            for order_id in missing_in_api:
                try:
                    # Check if it's an open trade
                    conn = self.db._get_conn()
                    cur = conn.cursor()
                    cur.execute("SELECT status, side FROM trades WHERE order_id = ?", (order_id,))
                    row = cur.fetchone()
                    if row and row["status"] == "open":
                        # Don't mark buy orders as cancelled - they might be filled
                        # Only mark sell orders (TP/SL) as cancelled
                        if row["side"] and row["side"].upper() == "BUY":
                            self.logger.debug(f"API SYNC: Buy order {order_id} not found in API - likely filled, keeping status")
                        else:
                            self.logger.info(f"API SYNC: Marking sell order {order_id} as cancelled (not found in API)")
                            self.db.set_trade_status(order_id, "cancelled")
                except Exception as e:
                    self.logger.error(f"API SYNC: Failed to update status for {order_id}: {e}")
            
            # Add new orders from API to DB
            for order in open_orders:
                order_id = str(order.get("orderId", ""))
                if order_id in missing_in_db:
                    try:
                        symbol = str(order.get("symbol", ""))
                        side = str(order.get("side", ""))
                        qty = float(order.get("qty", 0))
                        price = float(order.get("price", 0))
                        avg_price = float(order.get("avgPrice", 0))
                        _ = str(order.get("orderStatus", ""))
                        
                        # Get symbol_id
                        symbol_id = self.db.get_symbol_id(symbol)
                        if symbol_id is None:
                            self.logger.warning(f"API SYNC: Unknown symbol {symbol}, skipping order {order_id}")
                            continue
                        
                        # For sell orders (TP/SL), try to link to existing buy order
                        if side.upper() == "SELL":
                            # Look for existing buy order for this symbol
                            conn = self.db._get_conn()
                            cur = conn.cursor()
                            cur.execute(
                                "SELECT order_id, entry_price, quantity FROM trades WHERE symbol_id = ? AND side = 'Buy' AND status = 'open' ORDER BY created_at DESC LIMIT 1",
                                (symbol_id,)
                            )
                            buy_row = cur.fetchone()
                            
                            if buy_row:
                                # Link this sell order to the buy order
                                buy_order_id = buy_row["order_id"]
                                _ = buy_row["entry_price"]
                                
                                # Update the buy order with TP order ID
                                cur.execute(
                                    "UPDATE trades SET tp_order_id = ? WHERE order_id = ?",
                                    (order_id, buy_order_id)
                                )
                                conn.commit()
                                
                                self.logger.info(f"API SYNC: Linked TP order {order_id} to buy order {buy_order_id} for {symbol}")
                                continue  # Don't create separate trade record for TP
                        
                        # Use average price if available, otherwise use order price
                        entry_price = avg_price if avg_price > 0 else price
                        
                        # Insert new trade record (for buy orders or unlinked sell orders)
                        self.db.insert_trade(
                            symbol_id=symbol_id,
                            order_id=order_id,
                            side=side,
                            quantity=qty,
                            entry_price=entry_price,
                            take_profit_price=0.0,  # Will be updated when TP is placed
                            status="open"
                        )
                        
                        self.logger.info(f"API SYNC: Added new order {order_id} for {symbol} to DB")
                        
                    except Exception as e:
                        self.logger.error(f"API SYNC: Failed to add order {order_id} to DB: {e}")
            
            # Update existing orders with current data from API
            for order in open_orders:
                order_id = str(order.get("orderId", ""))
                if order_id in db_order_ids:
                    try:
                        # Update order data if needed
                        avg_price = float(order.get("avgPrice", 0))
                        qty = float(order.get("qty", 0))
                        _ = str(order.get("orderStatus", ""))
                        
                        if avg_price > 0:
                            # Update entry price if we have average price
                            conn = self.db._get_conn()
                            cur = conn.cursor()
                            cur.execute(
                                "UPDATE trades SET entry_price = ?, quantity = ? WHERE order_id = ?",
                                (avg_price, qty, order_id)
                            )
                            conn.commit()
                            
                    except Exception as e:
                        self.logger.error(f"API SYNC: Failed to update order {order_id}: {e}")
            
            self.logger.debug(f"API SYNC: Completed - {len(missing_in_api)} marked as cancelled, {len(missing_in_db)} added to DB")
            
            # Check if we have open orders and need to start panic monitoring
            self._check_and_start_panic_monitoring()
            
        except Exception as e:
            self.logger.error(f"API sync error: {e}")

    def _check_and_start_panic_monitoring(self) -> None:
        """Check if we have open orders and start panic monitoring if needed."""
        try:
            # Check if panic is enabled
            panic_enabled = getattr(self.config, "panic_enabled", True)
            if not panic_enabled:
                return
            
            # Check if panic sell is enabled
            panic_sell_enabled = getattr(self.config, "panic_sell_enabled", False)
            if not panic_sell_enabled:
                return
            
            # Get count of open orders
            open_trades = self.db.get_open_trades()
            open_count = len(open_trades)
            
            if open_count > 0:
                current_time = time.time()
                
                # Check if we need to start panic monitoring (every 30 seconds or first time)
                if current_time - self._last_panic_check_time >= 30.0:
                    self.logger.info(f"PANIC MONITOR: Found {open_count} open orders, starting panic monitoring")
                    
                    # Start panic monitoring for all open orders
                    for tr in open_trades:
                        try:
                            symbol = self.db._get_spot_symbol_by_id(tr.symbol_id)
                            if symbol and tr.entry_price > 0:
                                # Only monitor buy orders (positions), not sell orders (TP/SL)
                                if tr.side and tr.side.upper() == "BUY":
                                    panic_threshold = float(getattr(self.config, "panic_sell_drop_pct", 2.0))
                                    trigger_price = tr.entry_price * (1.0 - panic_threshold / 100.0)
                                    
                                    self.logger.info(f"PANIC MONITOR: {symbol} entry={tr.entry_price:.6f} drop={panic_threshold:.2f}% trigger={trigger_price:.6f}")
                                    
                                    # Send notification about panic monitoring only once per order
                                    if tr.order_id not in self._panic_notified_orders:
                                        try:
                                            self.notifier.send_telegram(
                                                f"🔍 PANIC MONITOR STARTED: {symbol} entry={tr.entry_price:.6f} drop={panic_threshold:.2f}% trigger={trigger_price:.6f}"
                                            )
                                            self._panic_notified_orders.add(tr.order_id)
                                        except Exception:
                                            pass
                                else:
                                    self.logger.debug(f"PANIC MONITOR: Skipping {symbol} - not a buy order (side={tr.side})")
                                    
                        except Exception as e:
                            self.logger.error(f"Error starting panic monitor for trade {tr.order_id}: {e}")
                    
                    self._last_panic_check_time = current_time
                    
        except Exception as e:
            self.logger.error(f"Error in panic monitoring check: {e}")

    def _on_tg_signal(self, text: str) -> None:
        """Parse PumpScreener-like message and place a spot market buy on Bybit.

        Expected format:
        EDU-USDT | BINANCE_FUTURES\n price: 5.6%\n open interest: 10.37%\n signals (24h): 4
        """
        if self.config.switch_mode != "tg":
            return
        try:
            line0 = text.splitlines()[0].strip()
            left = line0.split("|")[0].strip() if "|" in line0 else line0
            spot_symbol = left.replace("-", "")
            # Find our symbol id by spot symbol mapping
            recs = self.db.get_active_symbols()
            rec = next((r for r in recs if r.spot_symbol.upper() == spot_symbol.upper()), None)
            if not rec:
                msg = f"TG signal symbol not found in mapping: {spot_symbol}"
                self.logger.warning(msg)
                try:
                    self.notifier.send_telegram(msg)
                except Exception:
                    pass
                return
            last_price = self.db.get_last_price(rec.id) or 0.0
            if last_price <= 0.0:
                self.logger.warning(f"No recent price for {spot_symbol}")
                return
            qty = self.order_manager.calculate_position_size(spot_symbol, last_price, self.config.account_equity_usdt)
            if qty <= 0.0:
                self.logger.warning(f"Calculated qty is zero for {spot_symbol}")
                return
            # API guard: don't place if an open order already exists
            try:
                if self.order_manager.has_open_spot_order(spot_symbol):
                    self.logger.warning(f"TG: Skip placing order for {spot_symbol}: open order exists (API)")
                    try:
                        self.notifier.send_telegram(f"TG SKIP: {spot_symbol} has an existing open order (API)")
                    except Exception:
                        pass
                    return
            except Exception:
                pass
            tp_price = last_price * (1.0 + self.config.take_profit_percent / 100.0)
            order = self.order_manager.place_spot_order(spot_symbol, "Buy", qty, tp_price, reference_price=last_price)
            if not order:
                self.notifier.send_telegram(f"ORDER FAILED: {spot_symbol}")
                return
            order_id = str(order.get("orderId") or order.get("result", {}).get("orderId") or f"UNKWN-{spot_symbol}")
            avail_qty = self.order_manager.get_available_base_qty(spot_symbol, max_wait_s=6.0)
            if avail_qty <= 0.0:
                avail_qty = self.order_manager.get_filled_base_qty(spot_symbol, order_id, max_wait_s=6.0)
            if avail_qty <= 0.0:
                avail_qty = qty
            safe_qty = self.order_manager.adjust_qty_for_safety(spot_symbol, avail_qty)
            tp_resp = self.order_manager.place_tp_limit(spot_symbol, safe_qty, tp_price)
            if not tp_resp:
                self.notifier.send_telegram(f"TP PLACE FAILED: {spot_symbol}")
            self.db.insert_trade(rec.id, order_id, "Buy", safe_qty, last_price, tp_price, "open", stop_loss_price=None)
            self.db.insert_signal(rec.id, 0.0, 0.0, action_taken="bought")
            self.notifier.send_telegram(f"TG ORDER: {spot_symbol} qty={safe_qty} entry={last_price:.6f} tp={tp_price:.6f}")
            self.logger.info(f"TG ORDER PLACED: {spot_symbol} - Qty: {safe_qty}, TP: {tp_price}")
        except Exception as e:
            self.logger.error(f"Telegram signal handling error: {e}")

    def _maybe_sync_orders_periodically(self) -> None:
        now = time.time()
        if now - self._last_sync_time >= 30.0:  # every 30s
            try:
                self.order_manager.sync_recent_orders()
            except Exception as e:
                self.logger.error(f"Periodic order sync failed: {e}")
            self._last_sync_time = now 
            # Monitor for breakeven exits: if a trade is closed but TP did not trigger, consider SL/breakeven
            try:
                # For simplicity: check open trades list shrink; notify on close without TP hit
                pass
            except Exception as e:
                self.logger.debug(f"Breakeven monitor error: {e}")

    def _activate_runtime_emergency(self) -> None:
        self._runtime_emergency_stop = True
        # Red text with cross
        try:
            print("\x1b[31m✖ Emergency stop activated by Telegram command\x1b[0m")
        except Exception:
            pass
        self.logger.error("Emergency stop activated by Telegram command")
        self.notifier.send_telegram("EMERGENCY STOP: Activated by command") 

    def _deactivate_runtime_emergency(self) -> None:
        self._runtime_emergency_stop = False
        try:
            print("\x1b[32m✔ Trading resumed by Telegram command\x1b[0m")
        except Exception:
            pass
        self.logger.info("Trading resumed by Telegram command")
        self.notifier.send_telegram("RESUME: Trading resumed by command") 