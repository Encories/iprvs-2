from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.notifier import Notifier


@dataclass
class SoftwareSLPosition:
    trade_id: str
    symbol: str
    quantity: float
    entry_price: float
    sl_price: float
    created_at: datetime
    last_check: datetime
    activate_at: Optional[datetime] = None


class SoftwareSLManager:
    """Программный менеджер Stop Loss позиций"""

    def __init__(self, config: Config, db: DBManager, notifier: Notifier):
        self.config = config
        self.db = db
        self.notifier = notifier
        self.logger = get_logger(self.__class__.__name__)

        # Активные SL позиции
        self._positions: Dict[str, SoftwareSLPosition] = {}
        self._positions_lock = threading.Lock()

        # Контроль работы
        self._stop_event = threading.Event()
        self._worker_thread: Optional[threading.Thread] = None
        self._is_running = False

        # Кэш последних цен для оптимизации
        self._price_cache: Dict[str, tuple[float, datetime]] = {}

        # Retry state per trade
        self._retry_state: Dict[str, int] = {}

    def start(self) -> None:
        """Запуск мониторинга SL позиций"""
        if self._is_running:
            return

        self.logger.info("Starting Software SL Manager")
        self._stop_event.clear()
        self._worker_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._worker_thread.start()
        self._is_running = True
        # Сбросить метрики heartbeat
        self._last_heartbeat: float = time.time()
        self._last_heartbeat_notify: float = 0.0
        self._consecutive_price_failures: Dict[str, int] = {}

    def stop(self) -> None:
        """Остановка мониторинга"""
        if not self._is_running:
            return

        self.logger.info("Stopping Software SL Manager")
        self._stop_event.set()

        if self._worker_thread and self._worker_thread.is_alive():
            self._worker_thread.join(timeout=5.0)

        self._is_running = False

    def add_sl_position(self, trade_id: str, symbol: str, quantity: float,
                        entry_price: float, sl_price: float) -> None:
        """Добавить позицию для мониторинга SL"""
        with self._positions_lock:
            position = SoftwareSLPosition(
                trade_id=trade_id,
                symbol=symbol,
                quantity=float(quantity),
                entry_price=float(entry_price),
                sl_price=float(sl_price),
                created_at=datetime.utcnow(),
                last_check=datetime.utcnow(),
                activate_at=(
                    datetime.utcnow() + 
                    timedelta(seconds=max(0.0, float(getattr(self.config, "software_sl_activation_delay_seconds", 0.0))))
                ),
            )
            self._positions[trade_id] = position
            self._retry_state[trade_id] = 0

        delay_s = float(getattr(self.config, "software_sl_activation_delay_seconds", 0.0))
        if delay_s > 0:
            self.logger.info(
                f"Added SL position (delayed): {symbol} trade_id={trade_id} sl_price={sl_price} activates_in={int(delay_s)}s"
            )
        else:
            self.logger.info(f"Added SL position: {symbol} trade_id={trade_id} sl_price={sl_price}")

    def remove_sl_position(self, trade_id: str) -> None:
        """Удалить позицию из мониторинга"""
        with self._positions_lock:
            if trade_id in self._positions:
                pos = self._positions.pop(trade_id)
                self._retry_state.pop(trade_id, None)
                self.logger.info(f"Removed SL position: {pos.symbol} trade_id={trade_id}")

    def get_current_price(self, symbol: str) -> Optional[float]:
        """Получить текущую цену с кэшированием"""
        now = datetime.utcnow()

        # Проверить кэш
        try:
            ttl = float(getattr(self.config, "software_sl_price_cache_ttl", 5.0))
        except Exception:
            ttl = 5.0
        if symbol in self._price_cache:
            cached_price, cached_time = self._price_cache[symbol]
            if (now - cached_time).total_seconds() < ttl:
                return cached_price

        # Получить из БД
        try:
            records = self.db.get_active_symbols()
            symbol_rec = next((r for r in records if r.spot_symbol == symbol), None)
            if symbol_rec:
                price = self.db.get_last_price(symbol_rec.id)
                if price and price > 0:
                    self._price_cache[symbol] = (price, now)
                    return price
        except Exception as e:
            self.logger.debug(f"Failed to get price for {symbol}: {e}")

        return None

    def _monitor_loop(self) -> None:
        """Основной цикл мониторинга SL позиций"""
        self.logger.info("Software SL monitoring loop started")

        while not self._stop_event.is_set():
            try:
                self._check_sl_positions()
                self._maybe_send_heartbeat()
                try:
                    interval = float(getattr(self.config, "software_sl_check_interval", 1.0))
                except Exception:
                    interval = 1.0
                time.sleep(max(0.05, interval))
            except Exception as e:
                self.logger.error(f"SL monitoring error: {e}")
                time.sleep(2.0)

        self.logger.info("Software SL monitoring loop stopped")

    def _check_sl_positions(self) -> None:
        """Проверить все SL позиции"""
        with self._positions_lock:
            positions_to_check = list(self._positions.values())

        for position in positions_to_check:
            try:
                self._check_single_position(position)
            except Exception as e:
                self.logger.error(f"Error checking SL position {position.trade_id}: {e}")

    def _check_single_position(self, position: SoftwareSLPosition) -> None:
        """Проверить одну SL позицию"""
        # Отложенная активация
        if position.activate_at:
            now = datetime.utcnow()
            if now < position.activate_at:
                return
            # активация наступила — сбросить флаг и залогировать один раз
            position.activate_at = None
            self.logger.info(f"Software SL activated: {position.symbol} trade_id={position.trade_id}")

        current_price = self.get_current_price(position.symbol)
        if current_price is None:
            # Учёт неудачных попыток получения цены
            cnt = self._consecutive_price_failures.get(position.symbol, 0) + 1
            self._consecutive_price_failures[position.symbol] = cnt
            try:
                threshold = int(getattr(self.config, "software_sl_price_fail_threshold", 5))
            except Exception:
                threshold = 5
            if cnt >= max(1, threshold):
                msg = f"ALERT: Price unavailable for {position.symbol} ({cnt} consecutive fails)"
                try:
                    self.notifier.send_telegram(msg)
                except Exception:
                    pass
                self.logger.warning(msg)
            return
        else:
            # Сброс счётчика при успехе
            if position.symbol in self._consecutive_price_failures:
                self._consecutive_price_failures[position.symbol] = 0

        # Обновить время последней проверки
        position.last_check = datetime.utcnow()

        # Гистерезис триггера
        try:
            hyst = float(getattr(self.config, "software_sl_hysteresis_pct", 0.05))
        except Exception:
            hyst = 0.05
        trigger_price = position.sl_price * (1.0 - abs(hyst) / 100.0)

        # Проверить условие SL (цена упала ниже SL с гистерезисом)
        if current_price <= trigger_price:
            self.logger.warning(
                f"SL TRIGGERED: {position.symbol} price={current_price} <= sl_price={position.sl_price} (trigger={trigger_price})"
            )
            self._execute_sl_exit(position, current_price)

    def _execute_sl_exit(self, position: SoftwareSLPosition, current_price: float) -> None:
        """Исполнить выход по SL"""
        try:
            # Импорт здесь чтобы избежать циклических зависимостей
            from bybit_trading_bot.core.order_manager import OrderManager

            # Временное создание экземпляра (в идеале через DI)
            order_manager = OrderManager(self.config, self.db)

            # Попытки закрытия с backoff
            max_retries = int(getattr(self.config, "software_sl_max_retries", 3))
            base_delay = float(getattr(self.config, "software_sl_retry_backoff_base", 0.5))
            attempt = self._retry_state.get(position.trade_id, 0)

            while attempt <= max(0, max_retries):
                close_resp = order_manager.close_position_market(position.symbol, position.quantity)
                if close_resp:
                    # Вычисляем PnL с комиссиями
                    try:
                        fee_rate = float(self.config.fee_rate)
                    except Exception:
                        fee_rate = 0.001

                    fee_entry = position.entry_price * position.quantity * fee_rate
                    fee_exit = current_price * position.quantity * fee_rate
                    pnl_net = (current_price - position.entry_price) * position.quantity - (fee_entry + fee_exit)

                    # Закрываем трейд в БД
                    self.db.close_trade(position.trade_id, pnl_net)

                    # Сохраняем информацию о закрытии
                    try:
                        close_id = (
                            str(close_resp.get("orderId") or close_resp.get("result", {}).get("orderId"))
                            if isinstance(close_resp, dict)
                            else None
                        )
                        if close_id:
                            self.db.set_trade_close_info(position.trade_id, close_id, current_price)
                            self.db.update_trade_fees(position.trade_id, fee_exit=fee_exit)
                    except Exception:
                        pass

                    # Уведомления
                    notional = position.entry_price * position.quantity
                    pct = (pnl_net / notional * 100.0) if notional > 0 else 0.0
                    msg = (
                        f"SOFTWARE SL HIT: {position.symbol} qty={position.quantity} "
                        f"close={current_price:.6f} pnl_net={pnl_net:.4f} ({pct:.2f}%)"
                    )

                    try:
                        self.notifier.send_telegram(msg)
                    except Exception:
                        pass

                    self.logger.info(msg)

                    # Удаляем позицию из мониторинга
                    self.remove_sl_position(position.trade_id)
                    self._retry_state.pop(position.trade_id, None)
                    return

                # Ошибка — подождать и повторить
                attempt += 1
                self._retry_state[position.trade_id] = attempt
                if attempt > max_retries:
                    break
                delay = base_delay * (2 ** (attempt - 1))
                self.logger.warning(
                    f"SL close retry {attempt}/{max_retries} for {position.symbol} in {delay:.2f}s"
                )
                time.sleep(delay)

            # После исчерпания попыток
            err_msg = (
                f"ALERT: Failed to execute SL close for {position.symbol} trade_id={position.trade_id} "
                f"after {attempt} attempts"
            )
            self.logger.error(err_msg)
            try:
                self.notifier.send_telegram(err_msg)
            except Exception:
                pass

        except Exception as e:
            err = f"ALERT: SL execution error for {position.symbol}: {e}"
            self.logger.error(err)
            try:
                self.notifier.send_telegram(err)
            except Exception:
                pass

    def get_active_positions_count(self) -> int:
        """Получить количество активных SL позиций"""
        with self._positions_lock:
            return len(self._positions)

    def get_positions_info(self) -> List[Dict]:
        """Получить информацию о всех SL позициях"""
        positions_info: List[Dict] = []
        with self._positions_lock:
            for pos in self._positions.values():
                positions_info.append(
                    {
                        "trade_id": pos.trade_id,
                        "symbol": pos.symbol,
                        "quantity": pos.quantity,
                        "entry_price": pos.entry_price,
                        "sl_price": pos.sl_price,
                        "created_at": pos.created_at.isoformat(),
                        "last_check": pos.last_check.isoformat(),
                    }
                )
        return positions_info

    # --- Heartbeat ---
    def _maybe_send_heartbeat(self) -> None:
        """Периодически отправлять heartbeat о количестве позиций и состоянии менеджера."""
        try:
            hb_interval = float(getattr(self.config, "software_sl_heartbeat_interval", 60.0))
        except Exception:
            hb_interval = 60.0
        now = time.time()
        if now - getattr(self, "_last_heartbeat", 0.0) < max(5.0, hb_interval):
            return
        self._last_heartbeat = now
        # Условная отправка по флагу
        enabled = bool(getattr(self.config, "software_sl_heartbeat_enabled", True))
        if not enabled:
            return
        with self._positions_lock:
            n = len(self._positions)
            syms = [p.symbol for p in self._positions.values()]
        msg = f"SL-HEARTBEAT: running={self._is_running} positions={n} symbols={','.join(syms) if syms else '-'}"
        self.logger.info(msg)


