from __future__ import annotations

import os
import sys
from dataclasses import asdict

# Ensure package import works when run directly
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from bybit_trading_bot.config.settings import load_settings


def _mask(name: str, value: object) -> object:
    if value is None:
        return None
    s = str(value)
    lname = name.lower()
    if any(k in lname for k in ("secret", "key", "token")) and s:
        if len(s) <= 8:
            return "***" + ("*" * max(0, len(s) - 3))
        return s[:4] + "***" + s[-4:]
    return value


def main() -> None:
    cfg = load_settings()
    data = asdict(cfg)
    # Sort keys for readability
    keys = sorted(data.keys())
    print("Config (.env -> settings) in effect:\n")
    for k in keys:
        print(f"{k} = {_mask(k, data[k])}")


if __name__ == "__main__":
    main()


