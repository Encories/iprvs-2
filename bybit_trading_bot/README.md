# Bybit Dual-Market Trading Bot

Python bot that monitors Bybit spot prices via WebSocket and futures Open Interest via HTTP, generates signals (+5% price and +5% OI over 5 minutes), and executes spot trades with a +2% take-profit.

## Setup

1. Create and activate a virtual environment (recommended).
2. Install dependencies:

```bash
pip install -r bybit_trading_bot/requirements.txt
```

3. Create `.env` in project root or set env vars:

```
BYBIT_API_KEY=your_api_key_here
BYBIT_API_SECRET=your_api_secret_here
BYBIT_TESTNET=True
MAX_POSITION_SIZE_PERCENT=1.0
MAX_SIMULTANEOUS_POSITIONS=5
PRICE_CHANGE_THRESHOLD=5.0
OI_CHANGE_THRESHOLD=5.0
TAKE_PROFIT_PERCENT=2.0
MONITORING_INTERVAL=60
ACCOUNT_EQUITY_USDT=10000
# Prefer fixed notional per trade. If > 0, overrides percent sizing
TRADE_NOTIONAL_USDT=50
```

Sizing: if `TRADE_NOTIONAL_USDT > 0`, qty = TRADE_NOTIONAL_USDT / last_price. Otherwise qty = (ACCOUNT_EQUITY_USDT * MAX_POSITION_SIZE_PERCENT/100) / last_price.

## Run

```bash
python -m bybit_trading_bot.main
```

The current version includes real-time storage, signal checks, order placement (testnet-ready), and TP monitoring with simple protections. 

## Modes

- ordinary
- tg
- split
- trade
- scalp_5m (USDT-M futures 5m scalping)

### Scalp 5m mode

Set `SWITCH_MODE=scalp_5m` and configure:

Example env:

```
SCALP_TIMEFRAME=5m
SCALP_RISK_PCT=1.0
SCALP_MAX_DAILY_DD_PCT=3.0
SCALP_ACTIVE_SESSIONS=eu,us
SCALP_MIN_QUOTE_VOLUME_5M_USDT=15000
SCALP_LEVERAGE=10
SCALP_DRY_RUN=true
SCALP_NOTIONAL_USDT=10
SCALP_SESSION_EU_UTC=07:00-16:00
SCALP_SESSION_US_UTC=13:30-20:00
SCALP_TP1_RR=1.0
SCALP_TP1_PART=0.5
SCALP_TP2_RR=1.5
SCALP_TP2_PART=0.5
SCALP_TRAILING_PCT=0.0
```

Sizing rules:
- If `SCALP_NOTIONAL_USDT > 0`: qty = (`SCALP_NOTIONAL_USDT` × `SCALP_LEVERAGE`) / price
- Else: risk-based sizing from `SCALP_RISK_PCT` and stop distance

The engine subscribes to USDT-M futures 5m klines, computes EMA(9,21,50), VWAP, MACD(12,26,9), RSI(14), and opens one position per symbol when all LONG/SHORT conditions match. Two take-profits supported via reduce-only partials; optional trailing placeholder (future enhancement). Set `SCALP_DRY_RUN=false` to enable live orders.