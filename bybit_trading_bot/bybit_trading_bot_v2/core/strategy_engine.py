from __future__ import annotations

import asyncio
from dataclasses import dataclass
import os

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.notifier import Notifier
from bybit_trading_bot.utils.db_manager import DBManager

from ..utils.data_fetcher import DataFetcher
from ..utils.performance_tracker import PerformanceTracker
from ..strategies.volume_breakout import VolumeBreakoutStrategy
from .signal_generator import SignalGenerator
from .risk_manager import RiskManager
from .position_manager import PositionManager
from .execution_engine import ExecutionEngine


@dataclass
class StrategyContext:
    config: Config
    db: DBManager
    notifier: Notifier
    data: DataFetcher
    tracker: PerformanceTracker
    risk: RiskManager
    positions: PositionManager
    exec: ExecutionEngine
    signal_gen: SignalGenerator


class StrategyEngine:
    """High-level async orchestration for v2 scalp_5m strategies."""

    def __init__(self, config: Config, db: DBManager, notifier: Notifier) -> None:
        self.config = config
        self.db = db
        self.notifier = notifier
        self.logger = get_logger(self.__class__.__name__)

        # Compose modules
        self.data = DataFetcher(config)
        self.tracker = PerformanceTracker(config, db)
        self.strategy = VolumeBreakoutStrategy(config)
        self.risk = RiskManager(config, self.tracker)
        self.positions = PositionManager(config, db)
        self.exec = ExecutionEngine(config, db, notifier)
        self.signal_gen = SignalGenerator(config, self.data, self.strategy, db)

        self._stop = asyncio.Event()

    async def _loop_signals(self) -> None:
        symbols = await self.data.get_symbols()
        poll_interval = 1.0
        while not self._stop.is_set():
            try:
                # Enforce daily DD guard
                if not self.risk.can_trade_today(self.db):
                    await asyncio.sleep(5.0)
                    continue
                # Loss streak guard (soft): if recent performance is too poor, pause briefly
                if not self.risk.allow_trade_after_losses():
                    await asyncio.sleep(10.0)
                    continue
                # Commission impact monitor (best-effort)
                try:
                    perf = self.tracker.validate_strategy_profitability()
                    if bool(getattr(self.config, "commission_validation", True)):
                        if not bool(perf.get("profitable_after_fees", True)):
                            # Pause briefly if after-fee loss reported
                            await asyncio.sleep(15.0)
                            continue
                except Exception:
                    pass

                # Iterate symbols concurrently for speed
                await asyncio.gather(*(self._process_symbol(sym) for sym in symbols))
            except Exception as e:
                self.logger.error(f"signal loop error: {e}")
            await asyncio.sleep(poll_interval)

    async def _process_symbol(self, symbol: str) -> None:
        # Skip if market filters fail
        if not await self.data.is_min_volatility_met(symbol):
            return

        sig = await self.signal_gen.generate(symbol)
        if sig is None:
            return

        # Position sizing (adaptive)
        qty, sl_price, tp1, tp2, parts = self.risk.size_and_targets(symbol, sig)
        if qty <= 0:
            return

        # Live execution
        entry = await self.exec.enter_market(symbol, sig.side, qty)
        if entry is None:
            return

        # Register position and place exits
        self.positions.on_open(symbol, sig.side, qty, entry)
        await self.exec.plan_exits(symbol, sig.side, qty, entry, sl_price, tp1, tp2, parts)

    async def run(self) -> None:
        try:
            # Ensure DB has active futures symbols for subscription
            try:
                await self._ensure_symbols()
            except Exception as e:
                self.logger.error(f"ensure symbols failed: {e}")
            await self.data.start()
            await self.tracker.start()
            # Wire basic Telegram commands for status
            try:
                from bybit_trading_bot.utils.notifier import TelegramCommandListener
                def _status() -> str:
                    try:
                        return (
                            f"v2 status: open_positions={self.positions.open_count()} "
                        )
                    except Exception:
                        return "v2 status: n/a"
                self.tg_listener = TelegramCommandListener(
                    self.config,
                    on_stop=(lambda: None),
                    on_rate=_status,
                )
                self.tg_listener.start()
            except Exception:
                pass
            try:
                self.notifier.send_telegram("v2 engine started: scalp_5m")
            except Exception:
                pass
        except Exception:
            pass

        # Run concurrent loops
        tasks = [
            asyncio.create_task(self._loop_signals(), name="signals"),
            asyncio.create_task(self.exec.watch_fills(self.positions, self.tracker), name="fills"),
        ]
        try:
            await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in tasks:
                t.cancel()
            await self.data.stop()
            await self.tracker.stop()
            try:
                getattr(self, "tg_listener", None) and self.tg_listener.stop()
            except Exception:
                pass
            try:
                self.notifier.send_telegram("v2 engine stopped")
            except Exception:
                pass

    async def _ensure_symbols(self) -> None:
        # If symbols table already has active entries, nothing to do
        try:
            existing = self.db.get_active_symbols()
            if existing:
                return
        except Exception:
            pass
        # Try to use existing SymbolMapper to populate from exchange
        populated = 0
        try:
            from bybit_trading_bot.core.symbol_mapper import SymbolMapper
            mapper = SymbolMapper(self.config, self.db)
            populated = mapper.create_symbol_mapping()
        except Exception:
            populated = 0
        if populated and populated > 0:
            return
        # Fallback: seed with a curated majors list or env SCALP_SYMBOLS
        raw = os.getenv("SCALP_SYMBOLS") or "BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,DOGEUSDT,ADAUSDT,AVAXUSDT,LINKUSDT,TONUSDT"
        symbols = [s.strip().upper() for s in raw.split(",") if s.strip()]
        for sym in symbols:
            try:
                self.db.upsert_symbol(spot_symbol=sym, futures_symbol=sym, is_active=True)
            except Exception:
                continue


