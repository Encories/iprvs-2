from __future__ import annotations

import json
import threading
import time
from typing import Callable, Optional, Tuple

import requests

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger


class Notifier:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self._lock = threading.Lock()

    def send_telegram(self, text: str) -> None:
        if not self.config.telegram_enabled:
            return
        token = self.config.telegram_bot_token
        chat_id = self.config.telegram_chat_id
        if not token or not chat_id:
            self.logger.warning("Telegram enabled but bot token or chat id is missing")
            return
        try:
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            payload = {"chat_id": chat_id, "text": text}
            with self._lock:
                resp = requests.post(url, json=payload, timeout=10)
            if resp.status_code != 200:
                self.logger.warning(f"Telegram send failed: {resp.status_code} {resp.text}")
        except Exception as e:
            self.logger.error(f"Telegram send error: {e}")


class TelegramCommandListener:
    def __init__(self, config: Config, on_stop: Callable[[], None], on_rate: Optional[Callable[[], str]] = None, on_start: Optional[Callable[[], None]] = None, on_signal: Optional[Callable[[str], None]] = None, on_orders: Optional[Callable[[], str]] = None, on_panics: Optional[Callable[[], str]] = None, on_oco: Optional[Callable[[], str]] = None) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._offset: Optional[int] = None
        self._on_stop = on_stop
        self._on_rate = on_rate
        self._on_start = on_start
        self._on_signal = on_signal
        self._on_orders = on_orders
        self._on_panics = on_panics
        self._on_oco = on_oco

    def start(self) -> None:
        if not (self.config.telegram_enabled and self.config.telegram_commands_enabled):
            return
        if not self.config.telegram_bot_token:
            self.logger.warning("Telegram commands enabled but bot token missing")
            return
        if self._thread and self._thread.is_alive():
            return
        # Drain old updates so previous /stop is not reprocessed on restart
        try:
            base = f"https://api.telegram.org/bot{self.config.telegram_bot_token}"
            r = requests.get(f"{base}/getUpdates", params={"timeout": 0}, timeout=5)
            if r.status_code == 200:
                data = r.json()
                updates = data.get("result", [])
                if updates:
                    last_id = updates[-1]["update_id"]
                    self._offset = int(last_id) + 1
        except Exception as e:
            self.logger.debug(f"Telegram pre-drain failed: {e}")
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, name="TGCmd", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        try:
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=2)
        except Exception:
            pass

    def _reply(self, chat_id: str, text: str) -> None:
        try:
            url = f"https://api.telegram.org/bot{self.config.telegram_bot_token}/sendMessage"
            payload = {"chat_id": chat_id, "text": text}
            requests.post(url, json=payload, timeout=10)
        except Exception as e:
            self.logger.error(f"Telegram reply error: {e}")

    def _loop(self) -> None:
        token = self.config.telegram_bot_token
        allowed_chat = self.config.telegram_chat_id
        allowed_user = self.config.telegram_allowed_user_id
        base = f"https://api.telegram.org/bot{token}"
        while not self._stop_event.is_set():
            try:
                params = {"timeout": 20}
                if self._offset is not None:
                    params["offset"] = self._offset
                r = requests.get(f"{base}/getUpdates", params=params, timeout=30)
                if r.status_code != 200:
                    time.sleep(1)
                    continue
                data = r.json()
                for upd in data.get("result", []):
                    self._offset = upd["update_id"] + 1
                    msg = upd.get("message") or {}
                    text = (msg.get("text") or "").strip()
                    chat = msg.get("chat", {})
                    chat_id = str(chat.get("id")) if chat.get("id") is not None else None
                    from_user = msg.get("from", {})
                    from_id = str(from_user.get("id")) if from_user.get("id") is not None else None
                    if not text:
                        continue
                    if allowed_chat and chat_id != str(allowed_chat):
                        continue
                    if allowed_user and from_id != str(allowed_user):
                        continue
                    low = text.lower()
                    if low in {"/stop", "stop", "/pause"}:
                        self.logger.warning("Telegram /stop received; activating emergency stop")
                        try:
                            self._on_stop()
                            self._reply(chat_id, "Stop command accepted")
                        except Exception as e:
                            self.logger.error(f"on_stop error: {e}")
                    elif low in {"/start", "start", "/resume", "resume"} and self._on_start:
                        try:
                            self._on_start()
                            self._reply(chat_id, "Resume command accepted")
                        except Exception as e:
                            self.logger.error(f"on_start error: {e}")
                    elif low in {"/rateb", "rateb"} and self._on_rate:
                        try:
                            report = self._on_rate() or "No data"
                            self._reply(chat_id, report)
                        except Exception as e:
                            self.logger.error(f"on_rate error: {e}")
                    elif low in {"/orders", "orders"} and self._on_orders:
                        try:
                            report = self._on_orders() or "No open orders"
                            self._reply(chat_id, report)
                        except Exception as e:
                            self.logger.error(f"on_orders error: {e}")
                    elif low in {"/panics", "panics"} and self._on_panics:
                        try:
                            report = self._on_panics() or "No panic sell monitoring active"
                            self._reply(chat_id, report)
                        except Exception as e:
                            self.logger.error(f"on_panics error: {e}")
                    elif low in {"/oco", "oco"} and self._on_oco:
                        try:
                            report = self._on_oco() or "No OCO orders active"
                            self._reply(chat_id, report)
                        except Exception as e:
                            self.logger.error(f"on_oco error: {e}")
                    elif self._on_signal and "|" in text and "open interest" in low:
                        # Raw forward from PumpScreener-like text
                        try:
                            self._on_signal(text)
                            self._reply(chat_id, "Signal received")
                        except Exception as e:
                            self.logger.error(f"on_signal error: {e}")
            except Exception as e:
                self.logger.error(f"Telegram command loop error: {e}")
                time.sleep(2) 