from __future__ import annotations

import signal
import sys
import time

from bybit_trading_bot.config.settings import load_settings
from bybit_trading_bot.core.market_monitor import MarketMonitor
from bybit_trading_bot.utils.logger import get_logger


logger = get_logger(__name__)


def main() -> None:
    config = load_settings()
    try:
        logger.info("TEST FLAGS: TEST_SCALP=%s TEST_SCALP_ALL=%s", getattr(config, "test_scalp", False), getattr(config, "test_scalp_all", False))
    except Exception:
        pass
    # Single-instance guard (best-effort): create lock file exclusively; if exists, exit
    try:
        import os
        from pathlib import Path
        lock_dir = Path(__file__).resolve().parent / "storage"
        try:
            lock_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass
        lock_path = lock_dir / ".scalp_lock"
        try:
            fd = os.open(lock_path.as_posix(), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                os.write(fd, str(os.getpid()).encode("utf-8"))
            finally:
                os.close(fd)
        except FileExistsError:
            logger.error("Another instance appears to be running (lock file exists at %s). Exiting.", lock_path)
            return
        except Exception as e:
            logger.debug("Lock create error: %s", e)
        # Ensure lock removal on exit
        try:
            import atexit
            atexit.register(lambda: (lock_path.unlink(missing_ok=True)))  # type: ignore[arg-type]
        except Exception:
            pass
    except Exception:
        pass
    monitor = MarketMonitor(config)

    def _shutdown(signum: int, frame) -> None:  # type: ignore[override]
        logger.info("Received shutdown signal. Stopping monitor...")
        monitor.stop_monitoring()
        try:
            # Remove lock file if present
            from pathlib import Path
            lock_path = Path(__file__).resolve().parent / "storage" / ".scalp_lock"
            lock_path.unlink(missing_ok=True)  # type: ignore[arg-type]
        except Exception:
            pass
        sys.exit(0)

    # Handle Ctrl+C gracefully
    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    monitor.start_monitoring()

    # Keep the main thread alive while background threads run
    try:
        while monitor.is_running:
            time.sleep(0.5)
    except KeyboardInterrupt:
        _shutdown(signal.SIGINT, None)


if __name__ == "__main__":
    main() 