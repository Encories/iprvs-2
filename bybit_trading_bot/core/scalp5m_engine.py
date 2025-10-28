from __future__ import annotations

from typing import Dict, List
from dataclasses import dataclass
import time
import threading

from ..config.settings import Config
from ..utils.logger import get_logger
from ..utils.db_manager import DBManager
from ..utils.notifier import Notifier
from ..handlers.futures_handler import FuturesHandler
from ..handlers.futures_ws import FuturesWS
from .symbol_mapper import SymbolMapper
from .order_manager_futures import FuturesOrderManager
from ..indicators.technical import (
    calculate_rsi,
    calculate_macd,
    calculate_ema,
    calculate_vwap,
    calculate_adx,
)


@dataclass
class Candle:
    ts: float
    o: float
    h: float
    low: float
    c: float
    v: float
    closed: bool


class Scalp5mEngine:
    """5m futures scalping engine with EMA/VWAP/MACD/RSI conditions and risk sizing."""

    def __init__(self, config: Config, db: DBManager, notifier: Notifier) -> None:
        self.config = config
        self.db = db
        self.notifier = notifier
        self.logger = get_logger(self.__class__.__name__)
        self._fh = FuturesHandler(testnet=self.config.bybit_testnet)
        self._ws = FuturesWS(testnet=self.config.bybit_testnet, config=self.config)
        self._symbols: List[str] = []
        self._buffers: Dict[str, List[Candle]] = {}
        self._positions: Dict[str, str] = {}  # symbol -> side ("Long"/"Short")
        self._om = FuturesOrderManager(self.config)
        self._stop = False
        # simple JSONL log path
        self._jsonl_path = "bybit_trading_bot/storage/scalp_signals.jsonl"
        # position state for trailing stop updates
        self._pos_state: Dict[str, Dict[str, float | str]] = {}
        # heartbeat counters
        self._hb_last_log_ts: float = 0.0
        self._hb_last_tg_ts: float = 0.0
        self._hb_scanned: int = 0
        self._hb_presignals: int = 0
        self._hb_scanned_accum: int = 0
        self._hb_presignals_accum: int = 0
        self._hb_touched_symbols: set[str] = set()
        self._hb_thread: threading.Thread | None = None
        self._consecutive_losses: int = 0
        self._circuit_triggered_until_ts: float = 0.0
        # US session end alert removed per request
        # Last scanned symbol metrics (for per-minute console report)
        self._last_scan_info: dict[str, float | str] = {}
        self._last_scan_ts: float = 0.0
        self._hb_rr_idx: int = 0
        # rejection counters (per minute and accumulated)
        self._rej_minute: dict[str, int] = {
            "base": 0, "volsig": 0, "htf": 0, "adx": 0, "spread": 0, "dd": 0, "poslimit": 0,
        }
        self._rej_accum: dict[str, int] = {
            "base": 0, "volsig": 0, "htf": 0, "adx": 0, "spread": 0, "dd": 0, "poslimit": 0,
        }
        # early guards: insufficient history, circuit, session, liquidity
        self._rej_early_min: dict[str, int] = {"hist": 0, "circuit": 0, "session": 0, "liq": 0}
        self._rej_early_acc: dict[str, int] = {"hist": 0, "circuit": 0, "session": 0, "liq": 0}
        # base breakdown: which condition failed most
        self._rej_base_min: dict[str, int] = {"ema50": 0, "ema_cross": 0, "macd": 0, "rsi": 0, "vwap": 0, "vol0": 0}
        self._rej_base_acc: dict[str, int] = {"ema50": 0, "ema_cross": 0, "macd": 0, "rsi": 0, "vwap": 0, "vol0": 0}
        # Higher timeframe (15m) closes buffer per symbol
        self._htf15_closes: Dict[str, List[float]] = {}
        # Mapper to resolve symbol ids
        self._sm = SymbolMapper(self.config, self.db)
        # Backfill gating state
        self._backfill_complete: bool = False
        self._backfill_total: int = 0
        self._min_needed_5m: int = 60
        self._warmed_set: set[str] = set()

    # ---- lifecycle ----
    def run(self) -> None:
        try:
            self._initialize_symbols()
            if not self._symbols:
                self.logger.warning("No futures symbols available for scalp_5m mode")
                return
            # SCALP_TEST: run one synthetic test trade and exit
            if getattr(self.config, "scalp_test", False):
                self._run_one_test_trade()
                return
            # Start WS before heavy backfill to begin warming buffers in parallel
            self._ws.subscribe_kline_5m(self._symbols, self._on_kline)
            # Backfill in background if enabled
            if getattr(self.config, "scalp_backfill_async", False):
                try:
                    threading.Thread(target=self._backfill_recent_klines, name="ScalpBackfill", daemon=True).start()
                except Exception:
                    self._backfill_recent_klines()
            else:
                self._backfill_recent_klines()
            # Subscribe 15m for higher-timeframe trend filter
            try:
                if getattr(self.config, "scalp_wss_enable_htf15", True):
                    self._ws.subscribe_kline(self._symbols, interval="15", on_kline=self._on_kline_htf)
            except Exception:
                pass
            self.logger.info(f"Scalp5mEngine started for {len(self._symbols)} symbols")
            # Start heartbeat thread (logs every minute regardless of signals)
            try:
                if self._hb_thread is None or not self._hb_thread.is_alive():
                    self._hb_thread = threading.Thread(target=self._heartbeat_loop, name="ScalpHB", daemon=True)
                    self._hb_thread.start()
            except Exception:
                pass
            while not self._stop:
                time.sleep(0.5)
        except Exception as e:
            self.logger.error(f"Scalp5mEngine stopped with error: {e}")

    def stop(self) -> None:
        """Stop engine and close subscriptions."""
        try:
            self._stop = True
            try:
                self._ws.stop()
            except Exception:
                pass
        except Exception:
            pass

    # ---- data ----
    def _initialize_symbols(self) -> None:
        try:
            sm = SymbolMapper(self.config, self.db)
            common = sm.get_common_symbols()
            # filter for USDT pairs commonly used on USDT-M
            self._symbols = [s for s in common if s.upper().endswith("USDT")]
            # optional: liquidity filter by recent 5m quote volume threshold via config
            # Backfill now handled in run() (sync or async)
        except Exception as e:
            self.logger.error(f"Failed to initialize symbols: {e}")

    def _backfill_recent_klines(self) -> None:
        try:
            limit = max(60, int(getattr(self.config, "scalp_backfill_bars", 200)))
            # Cap to keep startup time reasonable
            limit = min(limit, 500)
            # HTF (15m) backfill: ensure enough bars for EMA50_15m (~>=60)
            limit15_cfg = int(getattr(self.config, "scalp_backfill_bars_htf", getattr(self.config, "scalp_backfill_bars", 200)))
            limit15 = max(60, min(limit15_cfg, 300))
            min_needed_5m = 60
            # init backfill gating counters
            try:
                self._min_needed_5m = min_needed_5m
                self._backfill_total = len(self._symbols)
                self._warmed_set.clear()
                self._backfill_complete = False
            except Exception:
                pass
            total = len(self._symbols)
            if total == 0:
                return
            # Backfill in batches to be gentle on API; simple progress to console
            batch_size = max(5, int(getattr(self.config, "scalp_backfill_batch_size", 15)))
            soft_fail_symbols: List[str] = []
            ok_symbols = 0
            import sys as _sys
            def _progress(i: int, warmed: int) -> None:
                try:
                    pct = (i * 100) // total
                    _sys.stdout.write(f"\rBackfill 5m/15m: {i}/{total} ({pct}%) | warmed>={min_needed_5m}: {warmed}")
                    _sys.stdout.flush()
                except Exception:
                    pass
            _progress(0, 0)
            for idx, sym in enumerate(self._symbols, start=1):
                # Paged fetch to guarantee minimum bars if exchange provides history
                rows = self._fh.get_klines_paged(sym, interval="5", min_bars=min_needed_5m, window_bars=500, max_windows=8)
                # Retry best-effort if insufficient history
                if not rows or len(rows) < min_needed_5m:
                    try:
                        time.sleep(0.05)
                    except Exception:
                        pass
                    rows = self._fh.get_klines(sym, interval="5", limit=500)
                buf = self._buffers.setdefault(sym, [])
                buf.clear()
                if rows:
                    for r in rows:
                        try:
                            buf.append(Candle(
                                ts=float(r.get("start") or 0.0),
                                o=float(r.get("open") or 0.0),
                                h=float(r.get("high") or 0.0),
                                low=float(r.get("low") or 0.0),
                                c=float(r.get("close") or 0.0),
                                v=float(r.get("volume") or 0.0),
                                closed=bool(r.get("confirm") or True),
                            ))
                        except Exception:
                            continue
                    if len(buf) > 500:
                        del buf[:-500]
                if len(buf) >= min_needed_5m:
                    ok_symbols += 1
                    try:
                        self._warmed_set.add(sym)
                    except Exception:
                        pass
                else:
                    soft_fail_symbols.append(sym)
                # Backfill 15m closes buffer for HTF trend filter (best-effort) with minimal paging
                try:
                    rows15 = self._fh.get_klines_paged(sym, interval="15", min_bars=60, window_bars=500, max_windows=6)
                    if rows15:
                        cl = self._htf15_closes.setdefault(sym, [])
                        cl.clear()
                        for r15 in rows15:
                            try:
                                cl.append(float(r15.get("close") or 0.0))
                            except Exception:
                                continue
                        if len(cl) > 300:
                            del cl[:-300]
                except Exception:
                    pass
                # Progress & batch pacing
                _progress(idx, ok_symbols)
                if (idx % batch_size) == 0:
                    time.sleep(0.5)
            try:
                _sys.stdout.write("\n")
            except Exception:
                pass
            # Summary
            if soft_fail_symbols:
                self.logger.warning(
                    f"Backfill: {len(soft_fail_symbols)} symbols have <{min_needed_5m} 5m bars; will rely on WS to warm up."
                )
            self.logger.info(
                f"Backfilled 5m/15m for {ok_symbols}/{total} symbols (5m limit={limit}, 15m limit={limit15})"
            )
            try:
                self._backfill_complete = (ok_symbols >= total and total > 0)
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"Backfill failed: {e}")

    def _on_kline(self, symbol: str, k: Dict[str, float | bool | None]) -> None:
        try:
            start = float(k.get("start") or 0.0)
            close = float(k.get("close") or 0.0)
            high = float(k.get("high") or 0.0)
            low = float(k.get("low") or 0.0)
            open_ = float(k.get("open") or 0.0)
            vol = float(k.get("volume") or 0.0)
            closed = bool(k.get("confirm") or False)
        except Exception:
            return
        buf = self._buffers.setdefault(symbol, [])
        if buf and abs(buf[-1].ts - start) < 1.0:
            buf[-1] = Candle(ts=start, o=open_, h=high, low=low, c=close, v=vol, closed=closed)
        else:
            buf.append(Candle(ts=start, o=open_, h=high, low=low, c=close, v=vol, closed=closed))
            if len(buf) > 500:
                del buf[:-500]
        if closed or bool(getattr(self.config, "scalp_diag_each_tick", False)):
            # mark warmed if buffer enough
            try:
                buf2 = self._buffers.get(symbol) or []
                if len(buf2) >= int(self._min_needed_5m):
                    self._warmed_set.add(symbol)
                    if not self._backfill_complete and self._backfill_total > 0 and len(self._warmed_set) >= self._backfill_total:
                        self._backfill_complete = True
            except Exception:
                pass
            self._maybe_signal(symbol)
            # try trailing update on closed bar
            try:
                self._try_trailing(symbol, price=close)
            except Exception:
                pass
            # lifecycle polling hint on bar close
            try:
                self._poll_positions()
            except Exception:
                pass
        # Heartbeat: count any kline update as a scan event
        try:
            self._hb_scanned += 1
            self._hb_scanned_accum += 1
            self._hb_touched_symbols.add(symbol)
            # track last WS activity to gate fallback polling
            self._last_scan_ts = time.time()
        except Exception:
            pass

    # ---- indicators and signals ----
    def _maybe_signal(self, symbol: str) -> None:
        # Gate all signals until backfill+warmup complete for all symbols
        try:
            if not getattr(self, "_backfill_complete", False):
                # count early reject for hist to make it visible in HB
                try:
                    self._rej_early_min["hist"] += 1
                    self._rej_early_acc["hist"] += 1
                except Exception:
                    pass
                return
        except Exception:
            pass
        buf = self._buffers.get(symbol) or []
        # Allow smaller warmup in TEST_SCALP_ALL mode
        _t_all = bool(getattr(self.config, "test_scalp_all", False))
        _min_bars = 30 if _t_all else 60
        if len(buf) < _min_bars:  # need enough bars for indicators
            try:
                self._rej_early_min["hist"] += 1
                self._rej_early_acc["hist"] += 1
            except Exception:
                pass
            return
        # Circuit breaker: evaluate consecutive losses before checks
        try:
            self._check_circuit_consecutive_losses()
        except Exception:
            pass
        # Circuit breaker cooldown
        if self._is_circuit_active():
            try:
                self._rej_early_min["circuit"] += 1
                self._rej_early_acc["circuit"] += 1
            except Exception:
                pass
            return
        # trading sessions guard
        if not self._is_in_active_session():
            return
        # Liquidity filter: 5m quote volume threshold
        closes = [b.c for b in buf]
        highs = [b.h for b in buf]
        lows = [b.low for b in buf]
        vols = [b.v for b in buf]
        try:
            quote_vol_last = closes[-1] * vols[-1]
            min_qv = float(self.config.scalp_min_quote_volume_5m_usdt)
            if min_qv > 0 and quote_vol_last < min_qv:
                try:
                    self._rej_early_min["liq"] += 1
                    self._rej_early_acc["liq"] += 1
                except Exception:
                    pass
                return
        except Exception:
            pass
        ema9 = calculate_ema(closes, 9)
        ema21 = calculate_ema(closes, 21)
        ema50 = calculate_ema(closes, 50)
        macd_line, signal_line = calculate_macd(closes, fast=12, slow=26, signal=9)
        rsi = calculate_rsi(closes, period=14)
        vwap = calculate_vwap(highs, lows, closes, vols)
        if not (ema9 and ema21 and ema50 and macd_line and signal_line and rsi and vwap):
            return
        p = closes[-1]
        e9, e21, e50 = ema9[-1], ema21[-1], ema50[-1]
        macd_last, sig_last = macd_line[-1], signal_line[-1]
        rsi_last = rsi[-1]
        vwap_last = vwap[-1]
        # Store last scan info for per-minute logging
        # placeholders for ok flags (set below)
        ok_l = False
        ok_s = False

        # slopes
        def rising(series: List[float], n: int = 2) -> bool:
            if len(series) < (n + 1):
                return False
            return series[-1] > series[-2] and series[-2] >= series[-3] if n >= 2 else series[-1] > series[-2]

        test_mode = bool(getattr(self.config, "test_scalp", False))
        test_mode_all = bool(getattr(self.config, "test_scalp_all", False))
        # In test mode, relax VWAP condition
        vwap_long_ok = (p > vwap_last) or test_mode
        vwap_short_ok = (p < vwap_last) or test_mode
        if test_mode_all:
            # Extra‑relaxed:
            # - ignore EMA slope and MACD sign
            # - keep EMA50 direction and VWAP alignment
            # - require BOTH EMA cross and MACD vs signal (non-strict ≥ / ≤)
            # - keep RSI threshold
            ema_cond_long = (p > e50)
            ema_cond_short = (p < e50)
            cross_long_ok = (e9 >= e21) and (macd_last >= sig_last)
            cross_short_ok = (e9 <= e21) and (macd_last <= sig_last)
            rsi_ok_long = (rsi_last > 50)
            rsi_ok_short = (rsi_last < 50)
            vol_ok_last = (vols[-1] > 0.0)
            long_ok = (
                ema_cond_long and
                cross_long_ok and
                rsi_ok_long and
                vwap_long_ok and
                vol_ok_last
            )
            short_ok = (
                ema_cond_short and
                cross_short_ok and
                rsi_ok_short and
                vwap_short_ok and
                vol_ok_last
            )
        else:
            long_ok = (
                p > e50 and
                e9 > e21 and
                rising(ema9) and rising(ema21) and
                macd_last > sig_last and macd_last > 0 and
                rsi_last > 50 and
                vwap_long_ok and
                vols[-1] > 0.0
            )
            short_ok = (
                p < e50 and
                e9 < e21 and
                not rising(ema9) and not rising(ema21) and
                macd_last < sig_last and macd_last < 0 and
                rsi_last < 50 and
                vwap_short_ok and
                vols[-1] > 0.0
            )
        ok_l = bool(long_ok)
        ok_s = bool(short_ok)

        # Store last scan info for per-minute logging
        try:
            self._last_scan_info = {
                "symbol": symbol,
                "price": float(p),
                "ema9": float(e9),
                "ema21": float(e21),
                "ema50": float(e50),
                "macd": float(macd_last),
                "signal": float(sig_last),
                "rsi": float(rsi_last),
                "vwap": float(vwap_last),
                "ok_l": ok_l,
                "ok_s": ok_s,
                "test_all": bool(getattr(self.config, "test_scalp_all", False)),
                "test": bool(getattr(self.config, "test_scalp", False)),
            }
            self._last_scan_ts = time.time()
        except Exception:
            pass

        # RVOL + Z-score approximation: compare last volume vs mean and std over last N same-session bars
        vol_ok = self._volume_significance_filter(vols)
        # heartbeat: count whether raw pre-signal existed on this bar
        try:
            if long_ok or short_ok:
                self._hb_presignals += 1
                self._hb_presignals_accum += 1
        except Exception:
            pass
        # If no base pre-signal, count rejection and exit early
        if not (long_ok or short_ok):
            try:
                self._rej_minute["base"] += 1
                self._rej_accum["base"] += 1
                # breakdown counters for failed components (evaluate both sides)
                # compute component flags
                cond_ema50_l = (p > e50)
                cond_ema50_s = (p < e50)
                cond_cross_l = (e9 >= e21) if test_mode_all else (e9 > e21)
                cond_cross_s = (e9 <= e21) if test_mode_all else (e9 < e21)
                cond_macd_l = (macd_last >= sig_last) if test_mode_all else (macd_last > sig_last)
                cond_macd_s = (macd_last <= sig_last) if test_mode_all else (macd_last < sig_last)
                cond_rsi_l = (rsi_last > 50)
                cond_rsi_s = (rsi_last < 50)
                cond_vwap_l = vwap_long_ok
                cond_vwap_s = vwap_short_ok
                cond_vol = (vols[-1] > 0.0)
                # increment for any component that failed on either side
                if not (cond_ema50_l or cond_ema50_s):
                    self._rej_base_min["ema50"] += 1; self._rej_base_acc["ema50"] += 1
                if not (cond_cross_l or cond_cross_s):
                    self._rej_base_min["ema_cross"] += 1; self._rej_base_acc["ema_cross"] += 1
                if not (cond_macd_l or cond_macd_s):
                    self._rej_base_min["macd"] += 1; self._rej_base_acc["macd"] += 1
                if not (cond_rsi_l or cond_rsi_s):
                    self._rej_base_min["rsi"] += 1; self._rej_base_acc["rsi"] += 1
                if not (cond_vwap_l or cond_vwap_s):
                    self._rej_base_min["vwap"] += 1; self._rej_base_acc["vwap"] += 1
                if not cond_vol:
                    self._rej_base_min["vol0"] += 1; self._rej_base_acc["vol0"] += 1
            except Exception:
                pass
            # maybe emit heartbeat
            try:
                self._maybe_emit_heartbeat()
            except Exception:
                pass
            return
        # Volume significance rejection
        if not vol_ok:
            try:
                self._rej_minute["volsig"] += 1
                self._rej_accum["volsig"] += 1
            except Exception:
                pass
            return
        # Daily drawdown guard
        if not self._check_daily_drawdown_guard():
            try:
                self._rej_minute["dd"] += 1
                self._rej_accum["dd"] += 1
            except Exception:
                pass
            return
        # Higher timeframe (15m) filter
        if not self._check_htf_trend(symbol, want_long=long_ok, want_short=short_ok):
            try:
                self._rej_minute["htf"] += 1
                self._rej_accum["htf"] += 1
            except Exception:
                pass
            return
        # ADX regime filter (trend strength)
        if not self._check_adx_regime(highs, lows, closes):
            try:
                self._rej_minute["adx"] += 1
                self._rej_accum["adx"] += 1
            except Exception:
                pass
            return
        # Spread filter
        if not self._check_spread(symbol):
            try:
                self._rej_minute["spread"] += 1
                self._rej_accum["spread"] += 1
            except Exception:
                pass
            return
        # Global concurrent positions limit
        if not self._can_open_new_position():
            try:
                self._rej_minute["poslimit"] += 1
                self._rej_accum["poslimit"] += 1
            except Exception:
                pass
            return

        # Passed all filters → execute
        self._log_jsonl(symbol, {
                "ts": int(time.time()),
                "side": ("Buy" if long_ok else "Sell"),
                "price": closes[-1],
                "ema9": e9, "ema21": e21, "ema50": e50,
                "macd": macd_last, "signal": sig_last, "rsi": rsi_last, "vwap": vwap_last,
                "volume": vols[-1]
            })
        self._execute_signal(symbol, side=("Buy" if long_ok else "Sell"))
        # maybe emit heartbeat once per period
        try:
            self._maybe_emit_heartbeat()
        except Exception:
            pass

    # ---- execution ----
    def _execute_signal(self, symbol: str, side: str) -> None:
        # prevent duplicate concurrent opens for the same symbol
        if self._positions.get(symbol):
            return  # one position per symbol
        buf = self._buffers.get(symbol) or []
        if len(buf) < 5:
            return
        last = buf[-1]
        # stop level: swing low/high of last N bars or 0.5%
        n = 3
        if side == "Buy":
            swing = min(b.low for b in buf[-n:])
            max_stop = last.c * 0.995
            stop = max(swing, max_stop)
        else:
            swing = max(b.h for b in buf[-n:])
            min_stop = last.c * 1.005
            stop = min(swing, min_stop)
        entry = last.c
        # Sizing: prefer fixed notional * leverage; optional ATR-based scaling; fallback to risk-based sizing
        qty: float
        if getattr(self.config, "scalp_notional_usdt", 0.0) and float(self.config.scalp_notional_usdt) > 0.0:
            effective_notional = float(self.config.scalp_notional_usdt) * max(1.0, float(self.config.scalp_leverage))
            qty = effective_notional / max(1e-12, entry)
            # ATR adjustment: target volatility sizing
            if getattr(self.config, "scalp_atr_sizing_enabled", False):
                buf = self._buffers.get(symbol) or []
                if len(buf) >= max(16, int(self.config.scalp_atr_period) + 2):
                    highs = [b.h for b in buf]
                    lows = [b.low for b in buf]
                    closes = [b.c for b in buf]
                    from ..indicators.technical import calculate_atr
                    atr_vals = calculate_atr(highs, lows, closes, period=int(self.config.scalp_atr_period))
                    if atr_vals and atr_vals[-1] and atr_vals[-1] > 0:
                        atr_pct = float(atr_vals[-1] / entry) * 100.0
                        target = max(0.01, float(self.config.scalp_target_atr_pct))
                        mult = target / max(0.0001, atr_pct)
                        # clamp multiplier
                        mult = max(float(self.config.scalp_atr_size_min_mult), min(mult, float(self.config.scalp_atr_size_max_mult)))
                        qty = qty * mult
        else:
            qty = self._om.calc_risk_based_qty(symbol, entry_price=entry, stop_price=stop, equity_usdt=self.config.account_equity_usdt)
        if qty <= 0.0:
            return
        # Ensure meets exchange minimum notional at current entry
        adj_qty = self._om.ensure_min_notional(symbol, qty, entry)

        # Dry run: emulate fill and proceed to TP/SL planning without placing real orders
        if self.config.scalp_dry_run:
            filled_qty = adj_qty
            fill_price = entry
            order_id = f"DRY-{symbol}-{side}-{int(time.time())}"
        else:
            resp = None
            order_id = ""
            filled_qty = 0.0
            fill_price = 0.0
            # Aggressive limit entry with polling for fill; fallback to market
            if getattr(self.config, "scalp_use_limit_entry", False):
                bbba = self._fh.get_best_bid_ask(symbol)
                if bbba is not None:
                    bid, ask = bbba
                    offset = float(self.config.scalp_limit_offset_pct) / 100.0
                    price = (ask * (1.0 + offset)) if side == "Buy" else (bid * (1.0 - offset))
                    resp = self._om.place_limit(symbol, side=side, qty=adj_qty, price=price)
                    order_id = self._extract_order_id(resp)
                    filled, ap, fq = self._wait_for_limit_fill(symbol, order_id, side, timeout_sec=int(self.config.scalp_limit_timeout_sec)) if order_id else (False, 0.0, 0.0)
                    if not filled and order_id:
                        try:
                            self._om.cancel_order(symbol, order_id)
                        except Exception:
                            pass
                    if filled and fq > 0.0:
                        fill_price, filled_qty = ap, fq
            if filled_qty <= 0.0:
                # market fallback
                resp = self._om.place_market(symbol, side=side, qty=adj_qty)
                if resp is None:
                    return
                order_id = self._extract_order_id(resp)
                # Best-effort read back position for avg price/qty
                pos = self._om.get_position(symbol)
                if pos and abs(float(pos.get("size") or 0.0)) > 0:
                    filled_qty = abs(float(pos.get("size") or 0.0))
                    fill_price = float(pos.get("avgPrice") or entry)
                else:
                    filled_qty = adj_qty
                    fill_price = entry

        # Use filled quantity for TP sizing
        eff_qty = max(0.0, float(filled_qty))
        if eff_qty <= 0.0:
            return
        # Multi-TP: TP1 and TP2 with partial reduce-only limits, plus protective SL
        dist = abs(entry - stop)
        tp1_rr = max(0.1, float(self.config.scalp_tp1_rr))
        tp2_rr = max(tp1_rr, float(self.config.scalp_tp2_rr))
        part1 = min(1.0, max(0.0, float(self.config.scalp_tp1_part)))
        part2 = min(1.0, max(0.0, float(self.config.scalp_tp2_part)))
        # normalize parts
        if (part1 + part2) <= 0.0:
            part1, part2 = 0.5, 0.5
        if (part1 + part2) > 1.0:
            s = (part1 + part2)
            part1, part2 = part1 / s, part2 / s
        # prices
        if side == "Buy":
            tp1 = entry + tp1_rr * dist
            tp2 = entry + tp2_rr * dist
        else:
            tp1 = entry - tp1_rr * dist
            tp2 = entry - tp2_rr * dist
        # place SL first (protective) — only after confirmed fill (dry run counts as filled)
        if not self.config.scalp_dry_run:
            ok = self._om.place_tp_sl(symbol, position_side=("Long" if side == "Buy" else "Short"), take_profit=None, stop_loss=stop)
            if not ok:
                return
        # reduce-only partials with step-aware adjustment; merge if too small
        ro_side = ("Sell" if side == "Buy" else "Buy")
        step = self._om.get_qty_step(symbol)
        q1_raw = eff_qty * part1
        q2_raw = eff_qty * part2
        q1 = self._om.snap_down_to_step(symbol, q1_raw)
        q2 = self._om.snap_down_to_step(symbol, q2_raw)
        min_step = max(step, 0.0)
        placed_any = False
        # if both below step after snap, place single TP with full qty
        if (q1 <= 0.0) and (q2 <= 0.0):
            if not self.config.scalp_dry_run:
                self._om.place_reduce_only_limit(symbol, ro_side, self._om.snap_down_to_step(symbol, eff_qty), tp2)
            placed_any = True
        else:
            if q1 > 0.0:
                if not self.config.scalp_dry_run:
                    self._om.place_reduce_only_limit(symbol, ro_side, q1, tp1)
                placed_any = True
            if q2 > 0.0 and (eff_qty - q1) >= min_step:
                if not self.config.scalp_dry_run:
                    self._om.place_reduce_only_limit(symbol, ro_side, q2, tp2)
                placed_any = True
            # if second couldn't be placed, try merge residual into first target
            if not placed_any:
                if not self.config.scalp_dry_run:
                    self._om.place_reduce_only_limit(symbol, ro_side, self._om.snap_down_to_step(symbol, eff_qty), tp1)
        # optional trailing: convert SL to trailing if configured (>0)
        self._pos_state[symbol] = {
            "side": ("Long" if side == "Buy" else "Short"),
            "entry": fill_price or entry,
            "sl": stop,
            "qty": eff_qty,
            "order_id": order_id,
        }
        self._positions[symbol] = "Long" if side == "Buy" else "Short"
        # Build info line with actual placed quantities
        info_parts = [f"Opened {symbol} {side} qty={eff_qty:.6f} sl={stop:.6f}"]
        if q1 > 0.0:
            info_parts.append(f"TP1 {q1:.6f}@{tp1:.6f}")
        if q2 > 0.0 and (eff_qty - q1) >= min_step:
            info_parts.append(f"TP2 {q2:.6f}@{tp2:.6f}")
        self.logger.info(" | ".join(info_parts))
        # Telegram notify
        try:
            tl = [f"Opened {symbol} {side} qty={eff_qty:.6f}", f"Entry={fill_price or entry:.6f} SL={stop:.6f}"]
            if q1 > 0.0:
                tl.append(f"TP1={q1:.6f}@{tp1:.6f}")
            if q2 > 0.0 and (eff_qty - q1) >= min_step:
                tl.append(f"TP2={q2:.6f}@{tp2:.6f}")
            self.notifier.send_telegram("\n".join(tl))
        except Exception:
            pass

        # Breakeven stop: if price reaches 1R (configurable), move SL to entry
        try:
            be_rr = max(0.1, float(self.config.scalp_breakeven_rr))
            # Monitor asynchronously would be ideal; here we approximate on subsequent _try_trailing calls.
            self._pos_state[symbol]["be_rr"] = be_rr
            self._pos_state[symbol]["entry"] = fill_price or entry
            self._pos_state[symbol]["init_sl"] = stop
        except Exception:
            pass

        # Record trade open in DB (best-effort)
        try:
            symbol_id = self._get_symbol_id_by_futures(symbol)
            if symbol_id is not None and not self.config.scalp_dry_run:
                order_id_eff = order_id or f"{symbol}-{int(time.time())}"
                if not self.db.trade_exists(order_id_eff):
                    self.db.insert_trade(
                        symbol_id=symbol_id,
                        order_id=order_id_eff,
                        side=("long" if side == "Buy" else "short"),
                        quantity=eff_qty,
                        entry_price=(fill_price or entry),
                        take_profit_price=tp1,
                        status="open",
                        stop_loss_price=stop,
                    )
        except Exception:
            pass
        

    # ---- HTF/Regime helpers ----
    def _run_one_test_trade(self) -> None:
        try:
            # Pick first available symbol or fallback
            preferred = "BTCUSDT"
            symbol = preferred if preferred in self._symbols else (self._symbols[0] if self._symbols else preferred)
            # Use real mark price if available to reflect true min notional
            mp = self._fh.get_mark_price(symbol) or 100.0
            entry = float(mp)
            # 0.5% stop distance
            stop = entry * 0.995
            # Qty by env: notional * leverage
            notional = float(getattr(self.config, "scalp_notional_usdt", 0.0) or 0.0)
            lev = float(getattr(self.config, "scalp_leverage", 1.0) or 1.0)
            if notional <= 0.0:
                notional = 10.0
            if lev <= 0.0:
                lev = 1.0
            qty = (notional * lev) / max(1e-12, entry)
            self.logger.info(f"[SCALP_TEST] Initiating real test trade on {symbol}")
            adj_qty = self._om.ensure_min_notional(symbol, qty, entry)
            if getattr(self.config, "scalp_dry_run", True):
                self.logger.info(f"[SCALP_TEST] BUY (DRY) {symbol} qty={adj_qty:.6f} entry={entry}")
            else:
                resp = self._om.place_market(symbol, side="Buy", qty=adj_qty)
                if resp is None:
                    self.logger.error(f"[SCALP_TEST] Failed to place market order for {symbol}")
                    return
            # Simulate SL and two TPs
            dist = abs(entry - stop)
            tp1 = entry + float(self.config.scalp_tp1_rr) * dist
            tp2 = entry + float(self.config.scalp_tp2_rr) * dist
            self.logger.info(f"[SCALP_TEST] SL={stop} TP1={tp1} TP2={tp2}")
            if not getattr(self.config, "scalp_dry_run", True):
                # Set protective SL and two reduce-only TPs
                ok = self._om.place_tp_sl(symbol, position_side="Long", take_profit=None, stop_loss=stop)
                if not ok:
                    self.logger.error(f"[SCALP_TEST] Failed to set SL for {symbol}")
                part1 = min(1.0, max(0.0, float(self.config.scalp_tp1_part)))
                part2 = min(1.0, max(0.0, float(self.config.scalp_tp2_part)))
                if (part1 + part2) <= 0.0:
                    part1, part2 = 0.5, 0.5
                if (part1 + part2) > 1.0:
                    s = part1 + part2
                    part1, part2 = part1 / s, part2 / s
                q1 = adj_qty * part1
                q2 = adj_qty * part2
                self._om.place_reduce_only_limit(symbol, "Sell", q1, tp1)
                self._om.place_reduce_only_limit(symbol, "Sell", q2, tp2)
            # Short sleep to show full cycle in logs (without real fills simulation)
            time.sleep(1)
            try:
                self.notifier.send_telegram(
                    f"[SCALP_TEST] Test trade {symbol}\nEntry={entry} SL={stop}\nTP1={tp1}\nTP2={tp2}"
                )
            except Exception:
                pass
            self.logger.info("[SCALP_TEST] Completed")
        except Exception as e:
            self.logger.error(f"[SCALP_TEST] Failed: {e}")
    def _can_open_new_position(self) -> bool:
        try:
            active = sum(1 for v in self._positions.values() if v)
            return active < int(self.config.scalp_max_concurrent_positions)
        except Exception:
            return True

    def _is_circuit_active(self) -> bool:
        if not getattr(self.config, "scalp_circuit_enabled", True):
            return False
        import time as _t
        return _t.time() < float(self._circuit_triggered_until_ts)

    def _maybe_trigger_circuit(self, reason: str) -> None:
        if not getattr(self.config, "scalp_circuit_enabled", True):
            return
        import time as _t
        cooldown = max(0, int(self.config.scalp_circuit_cooldown_min)) * 60
        self._circuit_triggered_until_ts = _t.time() + cooldown
        try:
            self.logger.critical(f"CIRCUIT BREAKER: {reason}. Cooldown {cooldown//60}m")
            self.notifier.send_telegram(f"🚨 Circuit breaker: {reason}. Cooling {cooldown//60}m")
        except Exception:
            pass

    def _check_circuit_consecutive_losses(self) -> None:
        if not getattr(self.config, "scalp_circuit_enabled", True):
            return
        try:
            max_losses = max(1, int(self.config.scalp_circuit_max_consecutive_losses))
            pnls = self.db.get_last_closed_pnls(limit=max_losses)
            if not pnls:
                return
            consec = 0
            for p in pnls:
                if p is not None and float(p) < 0.0:
                    consec += 1
                else:
                    break
            if consec >= max_losses:
                self._maybe_trigger_circuit(f"{consec} consecutive losses")
        except Exception:
            pass
    def _on_kline_htf(self, symbol: str, k: Dict[str, float | bool | None], interval: str) -> None:
        # Maintain a compact buffer of confirmed 15m closes for HTF filters
        try:
            if interval != "15":
                return
            closed = bool(k.get("confirm") or False)
            if not closed:
                return
            close = k.get("close")
            if close is None:
                return
            lst = self._htf15_closes.setdefault(symbol, [])
            lst.append(float(close))
            if len(lst) > 300:
                del lst[:-300]
        except Exception:
            pass

    def _check_htf_trend(self, symbol: str, want_long: bool, want_short: bool) -> bool:
        try:
            closes_15 = self._htf15_closes.get(symbol)
            if not closes_15 or len(closes_15) < 60:
                # fallback: approximate 15m using sampled 5m closes
                buf = self._buffers.get(symbol) or []
                if len(buf) < 200:
                    return True
                closes_15 = [buf[i].c for i in range(0, len(buf), 3)]
                if len(closes_15) < 60:
                    return True
            ema50_15 = calculate_ema(closes_15, 50)
            if not ema50_15:
                return True
            price_15 = closes_15[-1]
            trend_long_ok = price_15 > ema50_15[-1]
            trend_short_ok = price_15 < ema50_15[-1]
            if want_long and not trend_long_ok:
                return False
            if want_short and not trend_short_ok:
                return False
            return True
        except Exception:
            return True

    def _check_adx_regime(self, highs: List[float], lows: List[float], closes: List[float]) -> bool:
        try:
            adx = calculate_adx(highs, lows, closes, period=14)
            if not adx:
                return True
            last = adx[-1]
            try:
                import math as _m
                if _m.isnan(float(last)):
                    return True
            except Exception:
                pass
            return float(last) >= 20.0
        except Exception:
            return True

    def _try_trailing(self, symbol: str, price: float) -> None:
        trail_pct = float(self.config.scalp_trailing_pct)
        trail_atr_mult = float(self.config.scalp_trailing_atr_mult)
        if trail_pct <= 0.0 and trail_atr_mult <= 0.0:
            return
        st = self._pos_state.get(symbol)
        if not st:
            return
        side = str(st.get("side"))
        curr_sl = float(st.get("sl", 0.0))
        # ATR-based trailing if requested
        atr_sl = None
        buf = self._buffers.get(symbol) or []
        if trail_atr_mult > 0.0 and len(buf) >= max(16, int(self.config.scalp_atr_period) + 2):
            highs = [b.h for b in buf]
            lows = [b.low for b in buf]
            closes = [b.c for b in buf]
            from ..indicators.technical import calculate_atr
            atr_vals = calculate_atr(highs, lows, closes, period=int(self.config.scalp_atr_period))
            if atr_vals and atr_vals[-1] and atr_vals[-1] > 0:
                d = float(atr_vals[-1]) * float(trail_atr_mult)
                atr_sl = (price - d) if side == "Long" else (price + d)
        # Breakeven: if current price >= entry + R (for Long) or <= entry - R (for Short)
        try:
            be_rr = float(self._pos_state.get(symbol, {}).get("be_rr") or 0.0)
            entry = float(self._pos_state.get(symbol, {}).get("entry") or 0.0)
            if be_rr > 0.0 and entry > 0.0:
                # risk per unit = |entry - initial SL|
                init_sl = float(self._pos_state.get(symbol, {}).get("init_sl") or curr_sl)
                r = abs(entry - init_sl)
                if r > 0:
                    if side == "Long" and price >= (entry + be_rr * r) and curr_sl < entry:
                        ok = self._om.place_tp_sl(symbol, position_side="Long", take_profit=None, stop_loss=entry)
                        if ok:
                            self._pos_state[symbol]["sl"] = entry
                            try:
                                self.notifier.send_telegram(f"Break-even SL moved {symbol} Long -> {entry:.6f}")
                            except Exception:
                                pass
                    if side == "Short" and price <= (entry - be_rr * r) and (curr_sl == 0 or curr_sl > entry):
                        ok = self._om.place_tp_sl(symbol, position_side="Short", take_profit=None, stop_loss=entry)
                        if ok:
                            self._pos_state[symbol]["sl"] = entry
                            try:
                                self.notifier.send_telegram(f"Break-even SL moved {symbol} Short -> {entry:.6f}")
                            except Exception:
                                pass
        except Exception:
            pass
        if side == "Long":
            base_sl = price * (1.0 - trail_pct / 100.0) if trail_pct > 0 else curr_sl
            target_sl = max(curr_sl, base_sl)
            if atr_sl is not None:
                target_sl = max(target_sl, atr_sl)
            # ensure SL below price
            if target_sl > curr_sl and target_sl < price:
                ok = self._om.place_tp_sl(symbol, position_side="Long", take_profit=None, stop_loss=target_sl)
                if ok:
                    st["sl"] = target_sl
                    try:
                        self.notifier.send_telegram(f"Trailing SL moved {symbol} Long -> {target_sl:.6f}")
                    except Exception:
                        pass
        else:
            # Short: SL above price
            base_sl = price * (1.0 + trail_pct / 100.0) if trail_pct > 0 else curr_sl
            target_sl = min(curr_sl if curr_sl > 0 else base_sl, base_sl)
            if atr_sl is not None:
                target_sl = min(target_sl, atr_sl)
            if target_sl < curr_sl and target_sl > price:
                ok = self._om.place_tp_sl(symbol, position_side="Short", take_profit=None, stop_loss=target_sl)
                if ok:
                    st["sl"] = target_sl
                    try:
                        self.notifier.send_telegram(f"Trailing SL moved {symbol} Short -> {target_sl:.6f}")
                    except Exception:
                        pass

    def _check_spread(self, symbol: str) -> bool:
        try:
            bbba = self._fh.get_best_bid_ask(symbol)
            if bbba is None:
                return True
            bid, ask = bbba
            if bid <= 0 or ask <= 0 or ask <= bid:
                return False
            spread_pct = (ask - bid) / bid * 100.0
            return spread_pct <= float(self.config.scalp_spread_max_pct)
        except Exception:
            return True

    # ---- helpers ----
    def _is_in_active_session(self) -> bool:
        return True

    def _check_daily_drawdown_guard(self) -> bool:
        try:
            max_dd_pct = float(self.config.scalp_max_daily_dd_pct)
            if max_dd_pct <= 0:
                return True
            today_pnl = self.db.get_today_pnl()
            # stop if loss >= limit
            return today_pnl >= -abs((self.config.account_equity_usdt * max_dd_pct) / 100.0)
        except Exception:
            return True

    def _log_jsonl(self, symbol: str, payload: dict) -> None:
        try:
            import json
            import os
            os.makedirs("bybit_trading_bot/storage", exist_ok=True)
            with open(self._jsonl_path, "a", encoding="utf-8") as f:
                payload = dict({"symbol": symbol}, **payload)
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            pass

    def _maybe_emit_heartbeat(self) -> None:
        now = time.time()
        # Log once per minute
        if (now - self._hb_last_log_ts) >= 60.0:
            scanned = self._hb_scanned
            presignals = self._hb_presignals
            self._hb_scanned = 0
            self._hb_presignals = 0
            self._hb_last_log_ts = now
            touched = len(self._hb_touched_symbols)
            total = len(self._symbols)
            self._hb_touched_symbols.clear()
            # If нет WS-активности за минуту, считаем RR-метрику как минимум одной активности
            if scanned == 0:
                scanned = 1
            if touched == 0:
                touched = 1
            msg = (
                f"HB: scanned={scanned} touched={touched}/{total} pre_signals={presignals} "
                f"circuit={'ON' if self._is_circuit_active() else 'OFF'}"
            )
            # Append last scanned symbol metrics (compact)
            try:
                if self._last_scan_info:
                    si = self._last_scan_info
                    msg += (
                        f" | TEST_ALL={bool(getattr(self.config,'test_scalp_all',False))} TEST={bool(getattr(self.config,'test_scalp',False))} "
                        f"ok_l={si.get('ok_l')} ok_s={si.get('ok_s')} "
                        f"e9={si.get('ema9'):.6f} e21={si.get('ema21'):.6f} "
                        f"macd={si.get('macd'):.6f}/{si.get('signal'):.6f} rsi={si.get('rsi'):.2f}"
                    )
                else:
                    msg += " | no_scan_info_yet"
            except Exception:
                pass
            # Append rejection summary
            try:
                rc = self._rej_minute; ra = self._rej_accum
                msg += (
                    f" | rejects[min]: base={rc['base']} vol={rc['volsig']} htf={rc['htf']} adx={rc['adx']} spd={rc['spread']} dd={rc['dd']} pos={rc['poslimit']}"
                    f" | rejects[acc]: base={ra['base']} vol={ra['volsig']} htf={ra['htf']} adx={ra['adx']} spd={ra['spread']} dd={ra['dd']} pos={ra['poslimit']}"
                )
                # reset minute counters
                self._rej_minute = {k: 0 for k in self._rej_minute.keys()}
                # append early guards and base breakdown
                re = self._rej_early_min; rbe = self._rej_base_min
                msg += (
                    f" | early[min]: hist={re['hist']} circuit={re['circuit']} liq={re['liq']}"
                    f" | base[min]: ema50={rbe['ema50']} ema_cross={rbe['ema_cross']} macd={rbe['macd']} rsi={rbe['rsi']} vwap={rbe['vwap']} vol0={rbe['vol0']}"
                )
                self._rej_early_min = {k: 0 for k in self._rej_early_min.keys()}
                self._rej_base_min = {k: 0 for k in self._rej_base_min.keys()}
            except Exception:
                pass
            self.logger.info(msg)
        # Telegram on configured period
        minutes = getattr(self.config, "scalp_heartbeat_minutes", 10)
        try:
            minutes = int(minutes)
        except Exception:
            minutes = 10
        minutes = max(1, minutes)
        period_tg = float(minutes) * 60.0
        if getattr(self.config, "scalp_heartbeat_telegram", True) and (now - self._hb_last_tg_ts) >= period_tg:
            self._hb_last_tg_ts = now
            scanned_tg = self._hb_scanned_accum
            presignals_tg = self._hb_presignals_accum
            self._hb_scanned_accum = 0
            self._hb_presignals_accum = 0
            msg_tg = (
                f"HB: scanned={scanned_tg} pre_signals={presignals_tg} "
                f"circuit={'ON' if self._is_circuit_active() else 'OFF'}"
            )
            try:
                self.notifier.send_telegram(msg_tg)
            except Exception:
                pass

    def _heartbeat_loop(self) -> None:
        while not self._stop:
            try:
                self._maybe_emit_heartbeat()
                # Fallback: poll one symbol's last closed 5m kline to keep scanning active
                # Gate fallback polling to run only if нет WS активности > 10s
                try:
                    if (time.time() - float(self._last_scan_ts)) > 10.0:
                        self._fallback_poll_one_symbol()
                except Exception:
                    self._fallback_poll_one_symbol()
                # Poll positions lifecycle periodically
                self._poll_positions()
            except Exception:
                pass
            time.sleep(5.0)


    def _fallback_poll_one_symbol(self) -> None:
        """Rotate through symbols and fetch last closed 5m kline via REST to update buffers and signals.

        This serves as a safety net if WS klines are sparse or delayed. Runs lightweight: one symbol per 5s.
        """
        try:
            if not self._symbols:
                return
            idx = self._hb_rr_idx % len(self._symbols)
            self._hb_rr_idx += 1
            sym = self._symbols[idx]
            rows = self._fh.get_klines(sym, interval="5", limit=1)
            if not rows:
                return
            r = rows[-1]
            # Only accept confirmed/closed bars
            if not bool(r.get("confirm") or True):
                return
            cdl = Candle(
                ts=float(r.get("start") or 0.0),
                o=float(r.get("open") or 0.0),
                h=float(r.get("high") or 0.0),
                low=float(r.get("low") or 0.0),
                c=float(r.get("close") or 0.0),
                v=float(r.get("volume") or 0.0),
                closed=True,
            )
            buf = self._buffers.setdefault(sym, [])
            # Append if newer than last
            if not buf or cdl.ts > buf[-1].ts:
                buf.append(cdl)
                if len(buf) > 500:
                    del buf[:-500]
                # Trigger analysis path
                self._maybe_signal(sym)
                # mark as activity
                try:
                    self._hb_scanned += 1
                    self._hb_scanned_accum += 1
                    self._hb_touched_symbols.add(sym)
                except Exception:
                    pass
        except Exception:
            pass

    def _volume_significance_filter(self, vols: List[float], lookback_bars: int = 60, rvol_threshold: float = 1.5, z_threshold: float = 1.5) -> bool:
        try:
            if len(vols) < max(10, lookback_bars):
                return True  # not enough data
            recent = vols[-lookback_bars:]
            current = float(vols[-1])
            mean_v = sum(recent[:-1]) / max(1, len(recent) - 1)
            # Simple RVOL proxy: current vs mean of recent bars (time-of-day true RVOL requires daily archives)
            rvol = (current / mean_v) if mean_v > 0 else 1.0
            # Z-score proxy
            import math
            m2 = sum((x - mean_v) ** 2 for x in recent[:-1]) / max(1, len(recent) - 2)
            std_v = math.sqrt(m2) if m2 > 0 else 0.0
            z = (current - mean_v) / std_v if std_v > 0 else 0.0
            return (rvol >= rvol_threshold) and (z >= z_threshold)
        except Exception:
            return True


    def _poll_positions(self) -> None:
        """Best-effort lifecycle sync: if position closed on exchange, clear local state and compute PnL.

        This is a lightweight poll based on get_position() and mark price; for production, prefer WS private fills.
        """
        try:
            if not self._positions:
                return
            for sym, side in list(self._positions.items()):
                pos = self._om.get_position(sym)
                size = 0.0 if not pos else abs(float(pos.get("size") or 0.0))
                if size > 0:
                    continue
                # position is closed — compute PnL best-effort from entry and current mark
                st = self._pos_state.get(sym) or {}
                entry = float(st.get("entry") or 0.0)
                qty = float(st.get("qty") or 0.0)
                if entry > 0 and qty > 0:
                    mp = self._om.get_mark_price_safe(sym) or entry
                    if side == "Long":
                        pnl = (mp - entry) * qty
                    else:
                        pnl = (entry - mp) * qty
                    try:
                        symbol_id = self._get_symbol_id_by_futures(sym)
                        if symbol_id is not None:
                            # Find trade by stored order_id
                            order_id_eff = str(st.get("order_id") or f"{sym}-{int(st.get('entry',0))}")
                            if order_id_eff:
                                self.db.set_trade_close_info(order_id_eff, close_order_id="", close_price=mp)
                                self.db.close_trade(order_id_eff, pnl)
                    except Exception:
                        pass
                # clear state
                self._positions.pop(sym, None)
                self._pos_state.pop(sym, None)
        except Exception:
            pass

    # ---- entry and lifecycle helpers ----
    def _extract_order_id(self, resp: Dict | None) -> str:
        try:
            if not resp:
                return ""
            rid = (resp.get("result", {}) or {}).get("orderId") if isinstance(resp.get("result"), dict) else None
            if rid:
                return str(rid)
            rid2 = resp.get("orderId")
            return str(rid2) if rid2 else ""
        except Exception:
            return ""

    def _wait_for_limit_fill(self, symbol: str, order_id: str, side: str, timeout_sec: int) -> tuple[bool, float, float]:
        """Poll order/position until filled or timeout.

        Returns (filled, avg_price, filled_qty).
        """
        deadline = time.time() + max(1, int(timeout_sec))
        last_avg = 0.0
        last_qty = 0.0
        while time.time() < deadline and not self._stop:
            try:
                pos = self._om.get_position(symbol)
                if pos and abs(float(pos.get("size") or 0.0)) > 0.0:
                    # consider position opened
                    sz = abs(float(pos.get("size") or 0.0))
                    ap = float(pos.get("avgPrice") or 0.0)
                    return True, ap, sz
                # fallback: check order history status if available
                hist = self._om.get_order_history(symbol, order_id)
                if isinstance(hist, dict):
                    res = hist.get("result", {}) if isinstance(hist.get("result"), dict) else {}
                    lst = res.get("list", []) if isinstance(res, dict) else []
                    if isinstance(lst, list) and lst:
                        row = lst[0]
                        st = str(row.get("orderStatus") or row.get("status") or "")
                        if st.lower() in {"filled", "partiallyfilled"}:
                            try:
                                ap = float(row.get("avgPrice") or row.get("cumExecAvgPrice") or 0.0)
                            except Exception:
                                ap = 0.0
                            try:
                                qty = float(row.get("cumExecQty") or row.get("qty") or 0.0)
                            except Exception:
                                qty = 0.0
                            last_avg, last_qty = ap, qty
                            if st.lower() == "filled" and qty > 0:
                                return True, ap, qty
            except Exception:
                pass
            time.sleep(0.5)
        return False, last_avg, last_qty

    def _get_symbol_id_by_futures(self, futures_symbol: str) -> int | None:
        try:
            recs = self.db.get_active_symbols()
            for r in recs:
                if str(r.futures_symbol).upper() == futures_symbol.upper():
                    return int(r.id)
        except Exception:
            return None
        return None

