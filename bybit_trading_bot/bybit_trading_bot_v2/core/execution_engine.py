from __future__ import annotations

import asyncio
from typing import Optional, Tuple

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.notifier import Notifier
from bybit_trading_bot.core.order_manager_futures import FuturesOrderManager
from ..utils.commission_calculator import CommissionCalculator


class ExecutionEngine:
    def __init__(self, config: Config, db: DBManager, notifier: Notifier) -> None:
        self.config = config
        self.db = db
        self.notifier = notifier
        self.logger = get_logger(self.__class__.__name__)
        self._om = FuturesOrderManager(config)
        # Commission model (can be overridden by env later)
        self._fees = CommissionCalculator(
            maker_fee=float(getattr(self.config, "bybit_maker_fee", 0.0002)),
            taker_fee=float(getattr(self.config, "bybit_taker_fee", 0.00055)),
        )
        # Track runner orders for optional trailing
        self._runner_orders: dict[str, str] = {}
        self._trailing_tasks: dict[str, asyncio.Task] = {}

    async def enter_market(self, symbol: str, side: str, qty: float) -> Optional[float]:
        if qty <= 0:
            return None
        try:
            # Place market order live only
            resp = self._om.place_market(symbol, side=side, qty=qty)
            if not resp:
                return None
            # Approx entry price via mark price
            mp = self._om.get_mark_price_safe(symbol)
            await self._notify(f"ENTRY {symbol} {side} qty={qty}")
            return mp or None
        except Exception as e:
            self.logger.error(f"enter_market error: {e}")
            return None

    async def plan_exits(
        self,
        symbol: str,
        side: str,
        qty: float,
        entry: float,
        sl_price: float,
        tp1: float,
        tp2: float,
        parts: Tuple[float, float],
    ) -> None:
        try:
            qty1 = qty * parts[0]
            qty2 = qty - qty1
            # Place reduce-only limit exits
            side_close = "Sell" if side == "Buy" else "Buy"
            if qty1 > 0:
                # Commission validation: only place TP1 if net expected profit > 0
                _ok1, _net1 = self._fees.validate_trade_profitability(entry, tp1, "long" if side == "Buy" else "short")
                if _ok1:
                    self._om.place_reduce_only_limit(symbol, side=side_close, qty=self._om.snap_down_to_step(symbol, qty1), price=tp1)
            if qty2 > 0:
                _ok2, _net2 = self._fees.validate_trade_profitability(entry, tp2, "long" if side == "Buy" else "short")
                if not _ok2:
                    # Adjust runner target to min profitable with buffer
                    target = self._fees.calculate_min_profitable_price(
                        entry, "long" if side == "Buy" else "short", buffer=float(getattr(self.config, "min_profit_buffer", 0.001))
                    )
                    tp2 = target
                resp = self._om.place_reduce_only_limit(symbol, side=side_close, qty=self._om.snap_down_to_step(symbol, qty2), price=tp2)
                try:
                    if isinstance(resp, dict):
                        oid = str(resp.get("orderId") or resp.get("result", {}).get("orderId") or "")
                        if oid:
                            self._runner_orders[symbol] = oid
                except Exception:
                    pass
            # Register Software SL monitoring
            try:
                from bybit_trading_bot.core.software_sl_manager import SoftwareSLManager
                slm = SoftwareSLManager(self.config, self.db, self.notifier)
                # Ensure the manager is running (idempotent)
                try:
                    slm.start()
                except Exception:
                    pass
                trade_id = f"{symbol}-{int(entry*1e6)}"
                slm.add_sl_position(trade_id=trade_id, symbol=symbol, quantity=qty, entry_price=entry, sl_price=sl_price)
            except Exception as e:
                self.logger.debug(f"Software SL registration failed: {e}")
            await self._notify(f"EXITS {symbol} tp1={tp1:.6f} tp2={tp2:.6f} sl={sl_price:.6f}")

            # Optional trailing on runner
            try:
                trailing_pct = float(getattr(self.config, "scalp_trailing_pct", 0.0) or 0.0)
            except Exception:
                trailing_pct = 0.0
            if trailing_pct > 0.0 and qty2 > 0:
                # Launch trailing task (replaces runner limit as price moves)
                if symbol in self._trailing_tasks and not self._trailing_tasks[symbol].done():
                    try:
                        self._trailing_tasks[symbol].cancel()
                    except Exception:
                        pass
                self._trailing_tasks[symbol] = asyncio.create_task(
                    self._trail_runner(symbol, side_close, qty2, entry, trailing_pct), name=f"trail:{symbol}"
                )
        except Exception as e:
            self.logger.error(f"plan_exits error: {e}")

    async def watch_fills(self, positions, tracker) -> None:
        # Placeholder: could poll order status and update position manager
        while True:
            await asyncio.sleep(2.0)

    async def _trail_runner(self, symbol: str, close_side: str, qty: float, entry: float, trailing_pct: float) -> None:
        """Move runner limit in direction of profit by trailing_pct from recent extremum.

        For long (close_side=Sell): increase TP as price rises; for short (close_side=Buy): decrease TP as price falls.
        """
        try:
            last_placed_price: float | None = None
            while True:
                await asyncio.sleep(2.0)
                mp = self._om.get_mark_price_safe(symbol)
                if mp is None or mp <= 0:
                    continue
                try:
                    trail = (trailing_pct / 100.0)
                except Exception:
                    trail = 0.0
                if trail <= 0:
                    return
                if close_side == "Sell":
                    target = mp * (1.0 - max(0.0, trail))
                    better = (last_placed_price is None) or (target > float(last_placed_price))
                else:
                    target = mp * (1.0 + max(0.0, trail))
                    better = (last_placed_price is None) or (target < float(last_placed_price))
                if not better:
                    continue
                # Cancel previous runner order if we have id
                oid = self._runner_orders.get(symbol)
                if oid:
                    try:
                        self._om.cancel_order(symbol, oid)
                    except Exception:
                        pass
                # Place new reduce-only limit at target
                resp = self._om.place_reduce_only_limit(symbol, side=close_side, qty=self._om.snap_down_to_step(symbol, qty), price=target)
                try:
                    if isinstance(resp, dict):
                        new_id = str(resp.get("orderId") or resp.get("result", {}).get("orderId") or "")
                        if new_id:
                            self._runner_orders[symbol] = new_id
                except Exception:
                    pass
                last_placed_price = target
        except asyncio.CancelledError:
            return
        except Exception as e:
            self.logger.debug(f"_trail_runner error {symbol}: {e}")

    async def _notify(self, text: str) -> None:
        try:
            self.notifier.send_telegram(text)
        except Exception:
            pass


