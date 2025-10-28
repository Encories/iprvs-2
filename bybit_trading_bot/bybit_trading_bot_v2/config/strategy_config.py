from __future__ import annotations

# This module documents env-to-attr expectations on Config.
# Existing Config already loads many of these via settings.py with names:
# - vb_ema_period, vb_trend_ema_h1, vb_min_rvol, vb_confidence_threshold
# - adaptive_risk, base_position_size, adaptive_leverage_max, confidence_multiplier
# - win_rate_adaptation, daily_loss_limit
# - execution_type (implicit market), limit_exit_only
# - quick_tp_pct, runner_tp_pct, quick_tp_ratio, runner_tp_ratio, tight_sl_ratio
# - scalp_active_sessions, eu/us session strings, min_market_volatility


