from __future__ import annotations

import logging
import os
import sys
from typing import Optional


_LOGGER_CONFIGURED = False


def _configure_root_logger(level: int = logging.INFO) -> None:
    global _LOGGER_CONFIGURED
    if _LOGGER_CONFIGURED:
        return

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)

    # Optional file logging for analytics
    log_path = os.getenv("ANALYTICS_LOG_PATH")
    file_handler = None
    if log_path:
        try:
            os.makedirs(os.path.dirname(log_path), exist_ok=True) if os.path.dirname(log_path) else None
            file_handler = logging.FileHandler(log_path, mode="a", encoding="utf-8")
            file_handler.setFormatter(formatter)
        except Exception:
            file_handler = None

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(handler)
    if file_handler is not None:
        root_logger.addHandler(file_handler)

    _LOGGER_CONFIGURED = True


def get_logger(name: Optional[str] = None, level: int = logging.INFO) -> logging.Logger:
    """Return a configured logger.

    The first call configures the root logger with a stdout handler.
    Subsequent calls return child loggers.
    """
    _configure_root_logger(level)
    # Late-enable analytics file handler if env var appears after initial config
    try:
        log_path = os.getenv("ANALYTICS_LOG_PATH")
        if log_path:
            root_logger = logging.getLogger()
            has_file = False
            for h in root_logger.handlers:
                try:
                    if isinstance(h, logging.FileHandler):
                        # type: ignore[attr-defined]
                        if getattr(h, "baseFilename", None) == log_path:
                            has_file = True
                            break
                except Exception:
                    continue
            if not has_file:
                try:
                    os.makedirs(os.path.dirname(log_path), exist_ok=True) if os.path.dirname(log_path) else None
                    fh = logging.FileHandler(log_path, mode="a", encoding="utf-8")
                    fh.setFormatter(next((x.formatter for x in root_logger.handlers if hasattr(x, "formatter")), None))
                    root_logger.addHandler(fh)
                except Exception:
                    pass
    except Exception:
        pass
    return logging.getLogger(name or "bybit_bot")