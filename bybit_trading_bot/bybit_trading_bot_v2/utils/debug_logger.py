from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Dict, Any


class DebugLogger:
    def __init__(self, enabled: bool = True) -> None:
        self.enabled = bool(enabled)
        self.log_dir = os.path.join("bybit_trading_bot", "storage", "debug")
        try:
            os.makedirs(self.log_dir, exist_ok=True)
        except Exception:
            pass

    def log_pullback_state(self, symbol: str, state_info: Dict, market_data: Dict) -> None:
        if not self.enabled:
            return
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "pullback_state": state_info,
            "market_data": market_data,
        }
        log_file = os.path.join(self.log_dir, f"pullback_states_{symbol}.jsonl")
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception:
            pass

    def log_order_validation(self, symbol: str, original_qty: float, adjusted_qty: float, symbol_info: Dict) -> None:
        if not self.enabled:
            return
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "symbol": symbol,
            "original_qty": float(original_qty),
            "adjusted_qty": float(adjusted_qty),
            "symbol_info": symbol_info,
        }
        log_file = os.path.join(self.log_dir, "order_validation.jsonl")
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry) + "\n")
        except Exception:
            pass


