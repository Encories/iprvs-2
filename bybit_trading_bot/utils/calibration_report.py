from __future__ import annotations

import argparse
import json
import math
from typing import Iterable, List, Tuple

from bybit_trading_bot.config.settings import load_settings
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.notifier import Notifier
from bybit_trading_bot.utils.logger import get_logger


logger = get_logger(__name__)


def _q(vals: List[float], q: float) -> float:
    if not vals:
        return float("nan")
    v = sorted(vals)
    if len(v) == 1:
        return float(v[0])
    idx = q * (len(v) - 1)
    lo = int(math.floor(idx))
    hi = int(math.ceil(idx))
    if lo == hi:
        return float(v[lo])
    frac = idx - lo
    return float(v[lo] * (1.0 - frac) + v[hi] * frac)


def _fmt(x: float, digits: int = 2) -> str:
    if x != x:  # NaN
        return "NaN"
    return f"{x:.{digits}f}"


def build_report(lookback_days: int = 30) -> str:
    cfg = load_settings()
    db = DBManager(cfg.database_path)
    rows = db.get_predictions_with_outcomes(lookback_days=lookback_days)
    if not rows:
        return "No prediction outcomes found in the selected lookback window."

    total = len(rows)
    wins = sum(1 for r in rows if (r.get("pnl") or 0) > 0)
    hit_rate = wins / total if total > 0 else 0.0

    def collect(vals_key: str, winners_only: bool | None = None) -> List[float]:
        out: List[float] = []
        for r in rows:
            if winners_only is True and not ((r.get("pnl") or 0) > 0):
                continue
            if winners_only is False and ((r.get("pnl") or 0) > 0):
                continue
            f = r.get("factors") or {}
            if vals_key in f:
                try:
                    out.append(float(f[vals_key]))
                except Exception:
                    continue
        return out

    rsi_w = collect("rsi", winners_only=True)
    macd_w = collect("macd_hist", winners_only=True)
    slope_w = collect("slope_pct", winners_only=True)
    conf_w = [float(r.get("confidence") or 0.0) for r in rows if (r.get("pnl") or 0) > 0]

    rsi_l = collect("rsi", winners_only=False)
    macd_l = collect("macd_hist", winners_only=False)
    slope_l = collect("slope_pct", winners_only=False)
    conf_l = [float(r.get("confidence") or 0.0) for r in rows if not ((r.get("pnl") or 0) > 0)]

    # Recommendations: 25-й перцентиль среди победителей
    rec_rsi = _q(rsi_w, 0.25) if rsi_w else float("nan")
    rec_macd = _q(macd_w, 0.25) if macd_w else float("nan")
    rec_slope = _q(slope_w, 0.25) if slope_w else float("nan")
    rec_conf = _q(conf_w, 0.25) if conf_w else float("nan")

    lines: List[str] = []
    lines.append(f"Calibration window: last {lookback_days} days")
    lines.append(f"Samples: {total}, Wins: {wins}, Hit-rate: {_fmt(hit_rate*100, 2)}%")
    lines.append("")
    lines.append("Winners (Q25/Median/Q75):")
    lines.append(
        f"  RSI: {_fmt(_q(rsi_w,0.25))} / {_fmt(_q(rsi_w,0.5))} / {_fmt(_q(rsi_w,0.75))}"
    )
    lines.append(
        f"  MACD_HIST: {_fmt(_q(macd_w,0.25),4)} / {_fmt(_q(macd_w,0.5),4)} / {_fmt(_q(macd_w,0.75),4)}"
    )
    lines.append(
        f"  SLOPE_%: {_fmt(_q(slope_w,0.25))} / {_fmt(_q(slope_w,0.5))} / {_fmt(_q(slope_w,0.75))}"
    )
    lines.append(
        f"  CONF: {_fmt(_q(conf_w,0.25),2)} / {_fmt(_q(conf_w,0.5),2)} / {_fmt(_q(conf_w,0.75),2)}"
    )
    lines.append("")
    lines.append("Losers (Q25/Median/Q75):")
    lines.append(
        f"  RSI: {_fmt(_q(rsi_l,0.25))} / {_fmt(_q(rsi_l,0.5))} / {_fmt(_q(rsi_l,0.75))}"
    )
    lines.append(
        f"  MACD_HIST: {_fmt(_q(macd_l,0.25),4)} / {_fmt(_q(macd_l,0.5),4)} / {_fmt(_q(macd_l,0.75),4)}"
    )
    lines.append(
        f"  SLOPE_%: {_fmt(_q(slope_l,0.25))} / {_fmt(_q(slope_l,0.5))} / {_fmt(_q(slope_l,0.75))}"
    )
    lines.append(
        f"  CONF: {_fmt(_q(conf_l,0.25),2)} / {_fmt(_q(conf_l,0.5),2)} / {_fmt(_q(conf_l,0.75),2)}"
    )
    lines.append("")
    lines.append("Suggested thresholds (~Q25 winners):")
    lines.append(f"  TRADE_MODE_RSI_MIN={_fmt(rec_rsi)}")
    lines.append(f"  TRADE_MODE_MACD_HIST_MIN={_fmt(rec_macd,4)}")
    lines.append(f"  TRADE_MODE_SLOPE_MIN={_fmt(rec_slope)}")
    lines.append(f"  TRADE_MODE_CONFIDENCE_MIN={_fmt(rec_conf,2)}")
    lines.append("")
    lines.append("Env block (copy/paste):")
    lines.append("```env")
    lines.append(f"TRADE_MODE_RSI_MIN={_fmt(rec_rsi)}")
    lines.append(f"TRADE_MODE_MACD_HIST_MIN={_fmt(rec_macd,4)}")
    lines.append(f"TRADE_MODE_SLOPE_MIN={_fmt(rec_slope)}")
    lines.append(f"TRADE_MODE_CONFIDENCE_MIN={_fmt(rec_conf,2)}")
    lines.append("```")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Build calibration report from DB predictions and trades")
    parser.add_argument("--days", type=int, default=30, help="Lookback window in days (default: 30)")
    parser.add_argument("--telegram", action="store_true", help="Also send report to Telegram if configured")
    args = parser.parse_args()

    report = build_report(lookback_days=args.days)
    print(report)
    if args.telegram:
        try:
            cfg = load_settings()
            Notifier(cfg).send_telegram(report)
        except Exception:
            pass


if __name__ == "__main__":
    main()


