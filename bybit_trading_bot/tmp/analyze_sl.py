import os, sqlite3, json, argparse
from datetime import datetime, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB = os.path.abspath(os.path.join(BASE_DIR, '..', 'storage', 'database.sqlite'))
res = {'error': None, 'rows': []}

parser = argparse.ArgumentParser()
parser.add_argument('--id', type=int, dest='trade_id', default=None)
args, _ = parser.parse_known_args()

def iso(ts):
    try:
        return datetime.fromisoformat(ts) if ts else None
    except Exception:
        return None

try:
    if not os.path.exists(DB):
        res['error'] = {'code': 'db_not_found', 'path': DB}
    else:
        conn = sqlite3.connect(DB)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        trades: list[dict] = []
        if args.trade_id is not None:
            cur.execute("""
SELECT t.id, t.order_id, t.symbol_id,
       (SELECT spot_symbol FROM symbols s WHERE s.id = t.symbol_id) AS spot_symbol,
       t.side, t.quantity, t.entry_price, t.take_profit_price, t.sl_price, t.status, t.pnl,
       t.created_at, t.closed_at, t.sl_order_id, t.close_order_id, t.close_price, t.is_oco_order
FROM trades t
WHERE t.id = ?
LIMIT 1
            """, (int(args.trade_id),))
            row = cur.fetchone()
            if row:
                trades = [dict(row)]
        if not trades:
            cur.execute("""
SELECT t.id, t.order_id, t.symbol_id,
       (SELECT spot_symbol FROM symbols s WHERE s.id = t.symbol_id) AS spot_symbol,
       t.side, t.quantity, t.entry_price, t.take_profit_price, t.sl_price, t.status, t.pnl,
       t.created_at, t.closed_at, t.sl_order_id, t.close_order_id, t.close_price, t.is_oco_order
FROM trades t
WHERE t.sl_order_id IS NOT NULL AND t.status = 'closed'
ORDER BY datetime(COALESCE(t.closed_at, t.created_at)) DESC
LIMIT 2
            """)
            trades = [dict(r) for r in cur.fetchall()]
        if not trades:
            # Fallback: последние 2 закрытые сделки
            cur.execute("""
SELECT t.id, t.order_id, t.symbol_id,
       (SELECT spot_symbol FROM symbols s WHERE s.id = t.symbol_id) AS spot_symbol,
       t.side, t.quantity, t.entry_price, t.take_profit_price, t.sl_price, t.status, t.pnl,
       t.created_at, t.closed_at, t.sl_order_id, t.close_order_id, t.close_price, t.is_oco_order
FROM trades t
WHERE t.status = 'closed'
ORDER BY datetime(COALESCE(t.closed_at, t.created_at)) DESC
LIMIT 2
            """)
            trades = [dict(r) for r in cur.fetchall()]

        for tr in trades:
            symbol_id = tr['symbol_id']
            created = iso(tr.get('created_at'))
            closed = iso(tr.get('closed_at')) or created

            # Предыдущий сигнал до сделки
            cur.execute("""
SELECT id, price_change_percent AS price_chg, oi_change_percent AS oi_chg, action_taken, timestamp
FROM signals
WHERE symbol_id = ? AND datetime(timestamp) <= datetime(?)
ORDER BY datetime(timestamp) DESC LIMIT 1
            """, (symbol_id, tr.get('created_at')))
            sig = cur.fetchone()
            sig = dict(sig) if sig else None

            # Окно для выборки цен и OI: -20м до +10м вокруг сделки
            start = (created - timedelta(minutes=20)).isoformat() if created else None
            end = (closed + timedelta(minutes=10)).isoformat() if closed else None

            cur.execute("""
SELECT timestamp, price FROM price_data
WHERE symbol_id = ? AND timestamp BETWEEN ? AND ?
ORDER BY timestamp ASC
            """, (symbol_id, start, end))
            prices = [(r['timestamp'], float(r['price'])) for r in cur.fetchall()]

            cur.execute("""
SELECT timestamp, COALESCE(oi_value, open_interest) AS oi
FROM oi_data
WHERE symbol_id = ? AND timestamp BETWEEN ? AND ?
ORDER BY timestamp ASC
            """, (symbol_id, start, end))
            ois = [(r['timestamp'], float(r['oi'])) for r in cur.fetchall()]

            def pct(a, b):
                try:
                    return (b / a - 1.0) * 100.0 if a > 0 else None
                except Exception:
                    return None

            entry = tr.get('entry_price') or (prices[0][1] if prices else None)
            last = prices[-1][1] if prices else None
            move_pct = pct(entry, last) if (entry is not None and last is not None) else None

            # Compute MFE/MAE between entry and close
            mfe = None
            mae = None
            t_mfe = None
            t_mae = None
            if entry is not None and prices:
                # find extrema after created
                min_p = min(prices, key=lambda x: x[1])
                max_p = max(prices, key=lambda x: x[1])
                if tr.get('side','Buy').lower() == 'buy':
                    mae = pct(entry, min_p[1])
                    mfe = pct(entry, max_p[1])
                else:
                    # for sells reverse logic
                    mae = (entry / max_p[1] - 1.0) * 100.0 if max_p[1] > 0 else None
                    mfe = (entry / min_p[1] - 1.0) * 100.0 if min_p[1] > 0 else None
                t_mae = min_p[0]
                t_mfe = max_p[0]

            # Determine close reason
            reason = 'sl' if (tr.get('sl_order_id') or (tr.get('sl_price') and tr.get('close_price') and tr['close_price'] <= tr['sl_price'])) else 'tp_or_manual'

            # Find immediate 5m pre-entry deltas
            pre_start = (created - timedelta(minutes=5)).isoformat() if created else None
            pre_end = created.isoformat() if created else None
            cur.execute("""
SELECT timestamp, price FROM price_data WHERE symbol_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC
            """, (symbol_id, pre_start, pre_end))
            pre_prices = [float(r['price']) for r in cur.fetchall()]
            pre_price_chg = None
            if len(pre_prices) >= 2 and pre_prices[0] > 0:
                pre_price_chg = (pre_prices[-1]/pre_prices[0]-1.0)*100.0
            cur.execute("""
SELECT timestamp, COALESCE(oi_value, open_interest) AS oi FROM oi_data WHERE symbol_id = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp ASC
            """, (symbol_id, pre_start, pre_end))
            pre_ois = [float(r['oi']) for r in cur.fetchall() if r['oi'] is not None]
            pre_oi_chg = None
            if len(pre_ois) >= 2 and pre_ois[0] != 0:
                pre_oi_chg = (pre_ois[-1]/pre_ois[0]-1.0)*100.0

            res['rows'].append({
                'trade': tr,
                'signal': sig,
                'inferred_close_reason': reason,
                'samples': {'price_points': len(prices), 'oi_points': len(ois)},
                'entry_vs_last_pct': move_pct,
                'mfe_pct': mfe,
                'mae_pct': mae,
                't_mfe': t_mfe,
                't_mae': t_mae,
                'pre5m': {'price_change_pct': pre_price_chg, 'oi_change_pct': pre_oi_chg},
                'window': {'start': start, 'end': end}
            })
except Exception as e:
    res['error'] = {'code': 'exception', 'msg': str(e)}

print(json.dumps(res, ensure_ascii=False, indent=2))