from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from typing import List, Tuple

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from bybit_trading_bot.config.settings import load_settings
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.momentum_enhancements.momentum_gradient import GradientMomentumDetector
from bybit_trading_bot.momentum_enhancements.momentum_exhaustion import MomentumExhaustionDetector
from bybit_trading_bot.indicators.technical import calculate_rsi


def analyze_now(minutes: int = 10, top_n: int = 12) -> None:
    cfg = load_settings()
    db = DBManager(cfg.database_path)
    det = GradientMomentumDetector(cfg)
    ex = MomentumExhaustionDetector(cfg)

    now = datetime.utcnow()
    since = now - timedelta(minutes=max(5, minutes))

    rows = []
    try:
        symbols = db.get_active_symbols()
    except Exception:
        symbols = []

    for rec in symbols:
        try:
            ps: List[Tuple[str, float]] = db.get_recent_price_data(rec.id, since)
            if len(ps) < 6:
                continue
            # series as tuples (ts_iso, price)
            prices = [p for (_, p) in ps]
            grad, acc = det.calculate_momentum_gradient([(i, p) for i, p in enumerate(prices)])
            # exhaustion inputs
            rsi_val = None
            rsi_block = False
            if len(prices) >= max(16, int(cfg.rsi_period) + 1):
                rsi_vals = calculate_rsi(prices, period=int(cfg.rsi_period))
                if rsi_vals:
                    rsi_val = rsi_vals[-1]
                    rsi_block = ex.detect_rsi_exhaustion(rsi_val, +1)
            candle_block = False
            body_ratio = None
            bodies: List[float] = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
            if len(bodies) >= 6:
                body = bodies[-1]
                lookback = max(5, int(getattr(cfg, "volume_lookback_periods", 20)))
                ref = bodies[-(lookback+1):-1] if len(bodies) > lookback else bodies[:-1]
                if ref:
                    avg_body = sum(ref) / float(len(ref)) if ref else 0.0
                    body_ratio = (body / avg_body) if avg_body > 0 else None
                    candle_block = ex.detect_candle_exhaustion(body, avg_body)

            early_ok = det.validate_early_signal(grad, None)
            if early_ok and acc is not None and acc <= 0:
                early_ok = False
            reasons = []
            if rsi_block:
                reasons.append("RSI")
            if candle_block:
                reasons.append("Candle")
            if len(reasons) >= 2:
                early_ok = False

            rows.append({
                "symbol": rec.spot_symbol,
                "grad": grad,
                "acc": acc,
                "rsi": rsi_val,
                "body_ratio": body_ratio,
                "rsi_block": rsi_block,
                "candle_block": candle_block,
                "early_ok": early_ok,
            })
        except Exception:
            continue

    rows.sort(key=lambda r: (r["early_ok"], r["grad"] if r["grad"] is not None else 0.0), reverse=True)

    allowed = [r for r in rows if r["early_ok"]]
    blocked = [r for r in rows if not r["early_ok"]]

    print(f"Snapshot @ {now.isoformat(sep=' ', timespec='seconds')} | window={minutes}m | candidates={len(rows)} | allowed={len(allowed)} | blocked={len(blocked)}")
    print("Top candidates:")
    for r in rows[:top_n]:
        print(
            f"{r['symbol']:<12} grad={r['grad']:.4f} acc={r['acc'] if r['acc'] is not None else 'NA'} "
            f"rsi={r['rsi'] if r['rsi'] is not None else 'NA'} bodyx={r['body_ratio']:.2f if r['body_ratio'] else 'NA'} "
            f"blocks={'/'.join([k for k,v in [('RSI',r['rsi_block']),('Candle',r['candle_block'])] if v]) or '-'} "
            f"early_ok={r['early_ok']}"
        )


if __name__ == "__main__":
    minutes = int(os.environ.get("SNAP_MINUTES", "10"))
    top_n = int(os.environ.get("SNAP_TOP", "12"))
    analyze_now(minutes=minutes, top_n=top_n)


