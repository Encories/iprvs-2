from __future__ import annotations

import os
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, List, Optional, Sequence, Tuple

from bybit_trading_bot.utils.logger import get_logger


@dataclass(frozen=True)
class SymbolRecord:
    id: int
    spot_symbol: str
    futures_symbol: str
    is_active: bool


@dataclass(frozen=True)
class TradeRecord:
    order_id: str
    symbol_id: int
    side: str
    quantity: float
    entry_price: float
    take_profit_price: float
    status: str
    tp_order_id: Optional[str] = None
    close_order_id: Optional[str] = None
    close_price: Optional[float] = None
    fee_entry: Optional[float] = None
    fee_exit: Optional[float] = None
    sl_order_id: Optional[str] = None
    sl_price: Optional[float] = None
    is_oco_order: bool = False


class DBManager:
    """SQLite database manager for the trading bot.

    - Creates schema on first use
    - Provides thread-safe operations using a connection-per-thread model
    - Offers helpers to store and query data required by the bot
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self.logger = get_logger(self.__class__.__name__)
        self._local = threading.local()
        self._signals_has_symbol: bool = False
        self._trades_has_symbol: bool = False

        self._ensure_parent_directory()
        self._initialize_schema()

    # ---- Connection management ----
    def _get_conn(self) -> sqlite3.Connection:
        conn: Optional[sqlite3.Connection] = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self._configure_connection(conn)
            self._local.conn = conn
        return conn

    def _configure_connection(self, conn: sqlite3.Connection) -> None:
        cur = conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA foreign_keys=ON;")
        conn.commit()

    def _ensure_parent_directory(self) -> None:
        parent = os.path.dirname(os.path.abspath(self.db_path))
        os.makedirs(parent, exist_ok=True)

    # ---- Schema ----
    def _initialize_schema(self) -> None:
        conn = self._get_conn()
        cur = conn.cursor()

        cur.executescript(
            """
            CREATE TABLE IF NOT EXISTS symbols (
                id INTEGER PRIMARY KEY,
                spot_symbol TEXT NOT NULL,
                futures_symbol TEXT NOT NULL,
                is_active BOOLEAN DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS price_data (
                id INTEGER PRIMARY KEY,
                symbol_id INTEGER,
                price REAL NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                FOREIGN KEY (symbol_id) REFERENCES symbols (id)
            );

            CREATE TABLE IF NOT EXISTS oi_data (
                id INTEGER PRIMARY KEY,
                symbol_id INTEGER,
                open_interest REAL NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                bar_ts BIGINT,
                FOREIGN KEY (symbol_id) REFERENCES symbols (id)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY,
                symbol_id INTEGER,
                order_id TEXT,
                tp_order_id TEXT,
                close_order_id TEXT,
                side TEXT,
                quantity REAL,
                entry_price REAL,
                take_profit_price REAL,
                close_price REAL,
                fee_entry REAL,
                fee_exit REAL,
                sl_order_id TEXT,
                sl_price REAL,
                status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                pnl REAL,
                FOREIGN KEY (symbol_id) REFERENCES symbols (id)
            );

            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY,
                symbol_id INTEGER,
                price_change_percent REAL,
                oi_change_percent REAL,
                action_taken TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (symbol_id) REFERENCES symbols (id)
            );

            CREATE INDEX IF NOT EXISTS idx_price_data_symbol_time ON price_data(symbol_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_oi_data_symbol_time ON oi_data(symbol_id, timestamp);
            CREATE INDEX IF NOT EXISTS idx_trades_symbol_status ON trades(symbol_id, status);
            CREATE INDEX IF NOT EXISTS idx_trades_order_id ON trades(order_id);

            -- Split mode tables (spike detector)
            CREATE TABLE IF NOT EXISTS sp_signals (
                id INTEGER PRIMARY KEY,
                timestamp DATETIME,
                symbol TEXT,
                signal_type TEXT,
                signal_strength REAL,
                price REAL,
                volume REAL,
                rsi REAL,
                macd REAL,
                volume_spike_ratio REAL,
                orderbook_imbalance REAL,
                action_taken TEXT
            );

            CREATE TABLE IF NOT EXISTS sp_orders (
                id INTEGER PRIMARY KEY,
                signal_id INTEGER,
                order_id TEXT,
                symbol TEXT,
                side TEXT,
                quantity REAL,
                price REAL,
                status TEXT,
                created_at DATETIME,
                filled_at DATETIME,
                profit_loss REAL,
                FOREIGN KEY (signal_id) REFERENCES sp_signals (id)
            );

            CREATE TABLE IF NOT EXISTS sp_daily_stats (
                date DATE PRIMARY KEY,
                total_signals INTEGER,
                orders_placed INTEGER,
                profitable_trades INTEGER,
                total_profit_loss REAL,
                win_rate REAL
            );

            -- OCO orders table
            CREATE TABLE IF NOT EXISTS oco_orders (
                id INTEGER PRIMARY KEY,
                symbol_id INTEGER,
                symbol TEXT NOT NULL,
                quantity REAL NOT NULL,
                entry_price REAL NOT NULL,
                tp_order_id TEXT NOT NULL,
                sl_order_id TEXT NOT NULL,
                tp_price REAL NOT NULL,
                sl_price REAL NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                closed_at TIMESTAMP,
                FOREIGN KEY (symbol_id) REFERENCES symbols (id)
            );

            CREATE INDEX IF NOT EXISTS idx_oco_orders_symbol_status ON oco_orders(symbol, status);
            CREATE INDEX IF NOT EXISTS idx_oco_orders_tp_order_id ON oco_orders(tp_order_id);
            CREATE INDEX IF NOT EXISTS idx_oco_orders_sl_order_id ON oco_orders(sl_order_id);
            """
        )
        conn.commit()

        # Migration: add spot_symbol to signals if missing
        try:
            cur.execute("PRAGMA table_info(signals)")
            cols = [row[1] for row in cur.fetchall()]
            if "spot_symbol" not in cols:
                cur.execute("ALTER TABLE signals ADD COLUMN spot_symbol TEXT")
                conn.commit()
                self.logger.info("DB migration: added signals.spot_symbol column")
            self._signals_has_symbol = True
        except Exception as e:
            self.logger.debug(f"signals spot_symbol detection failed: {e}")
            self._signals_has_symbol = False

        # Migration: add spot_symbol to trades if missing
        try:
            cur.execute("PRAGMA table_info(trades)")
            tcols = [row[1] for row in cur.fetchall()]
            if "spot_symbol" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN spot_symbol TEXT")
                conn.commit()
                self.logger.info("DB migration: added trades.spot_symbol column")
            if "stop_loss_price" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN stop_loss_price REAL")
                conn.commit()
                self.logger.info("DB migration: added trades.stop_loss_price column")
            if "tp_order_id" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN tp_order_id TEXT")
                conn.commit()
                self.logger.info("DB migration: added trades.tp_order_id column")
            if "close_order_id" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN close_order_id TEXT")
                conn.commit()
                self.logger.info("DB migration: added trades.close_order_id column")
            if "close_price" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN close_price REAL")
                conn.commit()
                self.logger.info("DB migration: added trades.close_price column")
            if "fee_entry" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN fee_entry REAL")
                conn.commit()
                self.logger.info("DB migration: added trades.fee_entry column")
            if "fee_exit" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN fee_exit REAL")
                conn.commit()
                self.logger.info("DB migration: added trades.fee_exit column")
            if "sl_order_id" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN sl_order_id TEXT")
                conn.commit()
                self.logger.info("DB migration: added trades.sl_order_id column")
            if "sl_price" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN sl_price REAL")
                conn.commit()
                self.logger.info("DB migration: added trades.sl_price column")
            if "is_oco_order" not in tcols:
                cur.execute("ALTER TABLE trades ADD COLUMN is_oco_order BOOLEAN DEFAULT 0")
                conn.commit()
                self.logger.info("DB migration: added trades.is_oco_order column")
            self._trades_has_symbol = True
        except Exception as e:
            self.logger.debug(f"trades columns detection failed: {e}")
            self._trades_has_symbol = False

        # Migration: add oi_value to oi_data if missing
        try:
            cur.execute("PRAGMA table_info(oi_data)")
            ocols = [row[1] for row in cur.fetchall()]
            if "bar_ts" not in ocols:
                cur.execute("ALTER TABLE oi_data ADD COLUMN bar_ts BIGINT")
                conn.commit()
                self.logger.info("DB migration: added oi_data.bar_ts column")
            # Create unique index if not exists and bar_ts column is present
            try:
                cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS oi_symbol_bar_ts_ux ON oi_data(symbol_id, bar_ts)")
                conn.commit()
            except Exception:
                pass
            if "oi_value" not in ocols:
                cur.execute("ALTER TABLE oi_data ADD COLUMN oi_value REAL")
                conn.commit()
                self.logger.info("DB migration: added oi_data.oi_value column")
            if "oi_tokens" not in ocols:
                cur.execute("ALTER TABLE oi_data ADD COLUMN oi_tokens REAL")
                conn.commit()
                self.logger.info("DB migration: added oi_data.oi_tokens column")
            if "oi_tokens_mark" not in ocols:
                cur.execute("ALTER TABLE oi_data ADD COLUMN oi_tokens_mark REAL")
                conn.commit()
                self.logger.info("DB migration: added oi_data.oi_tokens_mark column")
            if "noi_percent" not in ocols:
                cur.execute("ALTER TABLE oi_data ADD COLUMN noi_percent REAL")
                conn.commit()
                self.logger.info("DB migration: added oi_data.noi_percent column")
        except Exception as e:
            self.logger.debug(f"oi_data columns detection failed: {e}")

    # ---- Symbols ----
    def upsert_symbol(self, spot_symbol: str, futures_symbol: str, is_active: bool = True) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id FROM symbols WHERE spot_symbol = ? AND futures_symbol = ?
            """,
            (spot_symbol, futures_symbol),
        )
        row = cur.fetchone()
        if row:
            cur.execute(
                "UPDATE symbols SET is_active = ? WHERE id = ?",
                (1 if is_active else 0, row[0]),
            )
            conn.commit()
            return int(row[0])
        cur.execute(
            """
            INSERT INTO symbols (spot_symbol, futures_symbol, is_active) VALUES (?, ?, ?)
            """,
            (spot_symbol, futures_symbol, 1 if is_active else 0),
        )
        conn.commit()
        return int(cur.lastrowid)

    def set_symbol_active(self, spot_symbol: str, is_active: bool) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE symbols SET is_active = ? WHERE spot_symbol = ?",
            (1 if is_active else 0, spot_symbol),
        )
        conn.commit()

    def get_active_symbols(self) -> List[SymbolRecord]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT id, spot_symbol, futures_symbol, is_active FROM symbols WHERE is_active = 1"
        )
        rows = cur.fetchall()
        return [
            SymbolRecord(
                id=int(r["id"]),
                spot_symbol=str(r["spot_symbol"]),
                futures_symbol=str(r["futures_symbol"]),
                is_active=bool(r["is_active"]),
            )
            for r in rows
        ]

    def _get_spot_symbol_by_id(self, symbol_id: int) -> Optional[str]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT spot_symbol FROM symbols WHERE id = ?", (symbol_id,))
        row = cur.fetchone()
        return str(row["spot_symbol"]) if row else None

    def get_symbol_id(self, spot_symbol: str) -> Optional[int]:
        """Get symbol_id by spot_symbol."""
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id FROM symbols WHERE spot_symbol = ?", (spot_symbol,))
        row = cur.fetchone()
        return int(row["id"]) if row else None

    # ---- Inserts ----
    def insert_price(self, symbol_id: int, price: float, ts: Optional[datetime] = None) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO price_data (symbol_id, price, timestamp) VALUES (?, ?, ?)",
            (symbol_id, float(price), (ts or datetime.utcnow()).isoformat()),
        )
        conn.commit()

    def insert_oi(
        self,
        symbol_id: int,
        open_interest: float,
        ts: Optional[datetime] = None,
        oi_value: Optional[float] = None,
        oi_tokens: Optional[float] = None,
        oi_tokens_mark: Optional[float] = None,
        noi_percent: Optional[float] = None,
    ) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        try:
            cur.execute("PRAGMA table_info(oi_data)")
            cols = [row[1] for row in cur.fetchall()]
        except Exception:
            cols = ["symbol_id", "open_interest", "timestamp"]
        # compute 5m bar_ts (ms since epoch // (5*60*1000)) if column exists
        computed_ts = (ts or datetime.utcnow())
        try:
            epoch_ms = int(computed_ts.timestamp() * 1000)
            bar_ts = epoch_ms // (5 * 60_000)
        except Exception:
            bar_ts = None
        if "bar_ts" in cols and bar_ts is not None:
            try:
                cur.execute(
                    "INSERT OR IGNORE INTO oi_data (symbol_id, open_interest, oi_value, oi_tokens, oi_tokens_mark, noi_percent, timestamp, bar_ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        symbol_id,
                        float(open_interest),
                        None if oi_value is None else float(oi_value),
                        None if oi_tokens is None else float(oi_tokens),
                        None if oi_tokens_mark is None else float(oi_tokens_mark),
                        None if noi_percent is None else float(noi_percent),
                        computed_ts.isoformat(),
                        int(bar_ts),
                    ),
                )
                conn.commit()
                return
            except Exception:
                pass
        if "oi_value" in cols and "oi_tokens" in cols and "oi_tokens_mark" in cols and "noi_percent" in cols:
            cur.execute(
                "INSERT INTO oi_data (symbol_id, open_interest, oi_value, oi_tokens, oi_tokens_mark, noi_percent, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    symbol_id,
                    float(open_interest),
                    None if oi_value is None else float(oi_value),
                    None if oi_tokens is None else float(oi_tokens),
                    None if oi_tokens_mark is None else float(oi_tokens_mark),
                    None if noi_percent is None else float(noi_percent),
                    computed_ts.isoformat(),
                ),
            )
        elif "oi_value" in cols and "oi_tokens" in cols and "oi_tokens_mark" in cols:
            cur.execute(
                "INSERT INTO oi_data (symbol_id, open_interest, oi_value, oi_tokens, oi_tokens_mark, timestamp) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    symbol_id,
                    float(open_interest),
                    None if oi_value is None else float(oi_value),
                    None if oi_tokens is None else float(oi_tokens),
                    None if oi_tokens_mark is None else float(oi_tokens_mark),
                    computed_ts.isoformat(),
                ),
            )
        elif "oi_value" in cols:
            cur.execute(
                "INSERT INTO oi_data (symbol_id, open_interest, oi_value, timestamp) VALUES (?, ?, ?, ?)",
                (
                    symbol_id,
                    float(open_interest),
                    None if oi_value is None else float(oi_value),
                    computed_ts.isoformat(),
                ),
            )
        else:
            cur.execute(
                "INSERT INTO oi_data (symbol_id, open_interest, timestamp) VALUES (?, ?, ?)",
                (symbol_id, float(open_interest), computed_ts.isoformat()),
            )
        conn.commit()

    def insert_signal(self, symbol_id: int, price_change: float, oi_change: float, action_taken: str) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        spot_symbol = self._get_spot_symbol_by_id(symbol_id) if self._signals_has_symbol else None
        if self._signals_has_symbol and spot_symbol is not None:
            cur.execute(
                """
                INSERT INTO signals (symbol_id, spot_symbol, price_change_percent, oi_change_percent, action_taken)
                VALUES (?, ?, ?, ?, ?)
                """,
                (symbol_id, spot_symbol, float(price_change), float(oi_change), action_taken),
            )
        else:
            cur.execute(
                """
                INSERT INTO signals (symbol_id, price_change_percent, oi_change_percent, action_taken)
                VALUES (?, ?, ?, ?)
                """,
                (symbol_id, float(price_change), float(oi_change), action_taken),
            )
        conn.commit()
        return int(cur.lastrowid)

    # ---- Split mode (Spike Detector) helpers ----
    def insert_sp_signal(
        self,
        timestamp: datetime,
        symbol: str,
        signal_type: str,
        signal_strength: float,
        price: float,
        volume: float,
        rsi: float | None = None,
        macd: float | None = None,
        volume_spike_ratio: float | None = None,
        orderbook_imbalance: float | None = None,
        action_taken: str | None = None,
    ) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sp_signals (timestamp, symbol, signal_type, signal_strength, price, volume, rsi, macd, volume_spike_ratio, orderbook_imbalance, action_taken)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp.isoformat(),
                symbol,
                signal_type,
                float(signal_strength),
                float(price),
                float(volume),
                None if rsi is None else float(rsi),
                None if macd is None else float(macd),
                None if volume_spike_ratio is None else float(volume_spike_ratio),
                None if orderbook_imbalance is None else float(orderbook_imbalance),
                action_taken,
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    def insert_sp_order(
        self,
        signal_id: int,
        order_id: str,
        symbol: str,
        side: str,
        quantity: float,
        price: float,
        status: str,
    ) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO sp_orders (signal_id, order_id, symbol, side, quantity, price, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(signal_id),
                order_id,
                symbol,
                side,
                float(quantity),
                float(price),
                status,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()
        return int(cur.lastrowid)

    def count_open_sp_orders(self) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM sp_orders WHERE status IN ('placed','open')")
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def update_sp_order_filled(self, order_id: str, filled_at: Optional[datetime] = None) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE sp_orders SET status = 'filled', filled_at = ? WHERE order_id = ?",
            ((filled_at or datetime.utcnow()).isoformat(), order_id),
        )
        conn.commit()

    def get_last_sp_signal_time(self, symbol: str) -> Optional[datetime]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT timestamp FROM sp_signals WHERE symbol = ? ORDER BY id DESC LIMIT 1",
            (symbol,),
        )
        row = cur.fetchone()
        return datetime.fromisoformat(row["timestamp"]) if row else None

    def insert_trade(
        self,
        symbol_id: int,
        order_id: str,
        side: str,
        quantity: float,
        entry_price: float,
        take_profit_price: float,
        status: str,
        stop_loss_price: Optional[float] = None,
        tp_order_id: Optional[str] = None,
        is_oco_order: bool = False,
    ) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        spot_symbol = self._get_spot_symbol_by_id(symbol_id) if self._trades_has_symbol else None
        if self._trades_has_symbol and spot_symbol is not None:
            cur.execute(
                """
                INSERT INTO trades (symbol_id, spot_symbol, order_id, tp_order_id, side, quantity, entry_price, take_profit_price, status, stop_loss_price, is_oco_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol_id,
                    spot_symbol,
                    order_id,
                    tp_order_id,
                    side,
                    float(quantity),
                    float(entry_price),
                    float(take_profit_price),
                    status,
                    None if stop_loss_price is None else float(stop_loss_price),
                    1 if is_oco_order else 0,
                ),
            )
        else:
            cur.execute(
                """
                INSERT INTO trades (symbol_id, order_id, tp_order_id, side, quantity, entry_price, take_profit_price, status, stop_loss_price, is_oco_order)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol_id,
                    order_id,
                    tp_order_id,
                    side,
                    float(quantity),
                    float(entry_price),
                    float(take_profit_price),
                    status,
                    None if stop_loss_price is None else float(stop_loss_price),
                    1 if is_oco_order else 0,
                ),
            )
        conn.commit()
        return int(cur.lastrowid)

    def trade_exists(self, order_id: str) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM trades WHERE order_id = ? LIMIT 1", (order_id,))
        return cur.fetchone() is not None

    def close_trade(self, order_id: str, pnl: float) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE trades SET status = 'closed', closed_at = CURRENT_TIMESTAMP, pnl = ?
            WHERE order_id = ?
            """,
            (float(pnl), order_id),
        )
        conn.commit()

    def set_trade_status(self, order_id: str, status: str) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        if status == 'open':
            cur.execute(
                "UPDATE trades SET status = ?, closed_at = NULL WHERE order_id = ?",
                (status, order_id),
            )
        else:
            cur.execute(
                "UPDATE trades SET status = ?, closed_at = CURRENT_TIMESTAMP WHERE order_id = ?",
                (status, order_id),
            )
        conn.commit()

    # ---- Queries ----
    def get_recent_price_series(self, symbol_id: int, minutes: int) -> List[Tuple[datetime, float]]:
        conn = self._get_conn()
        cur = conn.cursor()
        since = datetime.utcnow() - timedelta(minutes=minutes)
        cur.execute(
            """
            SELECT timestamp, price FROM price_data
            WHERE symbol_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            """,
            (symbol_id, since.isoformat()),
        )
        rows = cur.fetchall()
        return [(datetime.fromisoformat(r["timestamp"]), float(r["price"])) for r in rows]

    def get_recent_oi_series(self, symbol_id: int, minutes: int) -> List[Tuple[datetime, float]]:
        conn = self._get_conn()
        cur = conn.cursor()
        since = datetime.utcnow() - timedelta(minutes=minutes)
        # Prefer monetary value if available, but always coalesce to avoid NULLs
        cur.execute(
            """
            SELECT timestamp, COALESCE(oi_value, open_interest) AS oi
            FROM oi_data
            WHERE symbol_id = ? AND timestamp >= ?
            ORDER BY timestamp ASC
            """,
            (symbol_id, since.isoformat()),
        )
        rows = cur.fetchall()
        out: List[Tuple[datetime, float]] = []
        for r in rows:
            ts = datetime.fromisoformat(r["timestamp"])
            val = r["oi"]
            try:
                out.append((ts, float(val)))
            except Exception:
                # skip rows with unparseable values
                continue
        return out

    def get_recent_noi_series(self, symbol_id: int, minutes: int) -> List[Tuple[datetime, float]]:
        """Return series of normalized OI percent (0..100) if available."""
        conn = self._get_conn()
        cur = conn.cursor()
        since = datetime.utcnow() - timedelta(minutes=minutes)
        try:
            cur.execute(
                """
                SELECT timestamp, noi_percent FROM oi_data
                WHERE symbol_id = ? AND timestamp >= ? AND noi_percent IS NOT NULL
                ORDER BY timestamp ASC
                """,
                (symbol_id, since.isoformat()),
            )
            rows = cur.fetchall()
            return [
                (datetime.fromisoformat(r["timestamp"]), float(r["noi_percent"])) for r in rows
            ]
        except Exception:
            return []

    # ---- Helpers for trading/protections ----
    def count_open_positions(self) -> int:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS c FROM trades WHERE status = 'open'")
        row = cur.fetchone()
        return int(row[0]) if row else 0

    def has_open_position(self, symbol_id: int) -> bool:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT 1 FROM trades WHERE symbol_id = ? AND status = 'open' LIMIT 1",
            (symbol_id,),
        )
        return cur.fetchone() is not None

    def get_open_trades(self) -> List[TradeRecord]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT order_id, symbol_id, side, quantity, entry_price, take_profit_price, status, tp_order_id, close_order_id, close_price, fee_entry, fee_exit, sl_order_id, sl_price, is_oco_order
            FROM trades WHERE status = 'open'
            """
        )
        rows = cur.fetchall()
        trades = [
            TradeRecord(
                order_id=str(r["order_id"]),
                symbol_id=int(r["symbol_id"]),
                side=str(r["side"]),
                quantity=float(r["quantity"]),
                entry_price=float(r["entry_price"]),
                take_profit_price=float(r["take_profit_price"]),
                status=str(r["status"]),
                tp_order_id=(str(r["tp_order_id"]) if r["tp_order_id"] is not None else None),
                close_order_id=(str(r["close_order_id"]) if r["close_order_id"] is not None else None),
                close_price=(float(r["close_price"]) if r["close_price"] is not None else None),
                fee_entry=(float(r["fee_entry"]) if r["fee_entry"] is not None else None),
                fee_exit=(float(r["fee_exit"]) if r["fee_exit"] is not None else None),
                sl_order_id=(str(r["sl_order_id"]) if r["sl_order_id"] is not None else None),
                sl_price=(float(r["sl_price"]) if r["sl_price"] is not None else None),
                is_oco_order=bool(r["is_oco_order"]),
            )
            for r in rows
        ]
        
        return trades

    def set_trade_tp_order(self, order_id: str, tp_order_id: str) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE trades SET tp_order_id = ? WHERE order_id = ?",
            (tp_order_id, order_id),
        )
        conn.commit()

    def update_trade_entry_qty(self, order_id: str, entry_price: float, quantity: float) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE trades SET entry_price = ?, quantity = ? WHERE order_id = ?",
            (float(entry_price), float(quantity), order_id),
        )
        conn.commit()

    def update_trade_fees(self, order_id: str, fee_entry: Optional[float] = None, fee_exit: Optional[float] = None) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        if fee_entry is not None and fee_exit is not None:
            cur.execute(
                "UPDATE trades SET fee_entry = ?, fee_exit = ? WHERE order_id = ?",
                (float(fee_entry), float(fee_exit), order_id),
            )
        elif fee_entry is not None:
            cur.execute(
                "UPDATE trades SET fee_entry = ? WHERE order_id = ?",
                (float(fee_entry), order_id),
            )
        elif fee_exit is not None:
            cur.execute(
                "UPDATE trades SET fee_exit = ? WHERE order_id = ?",
                (float(fee_exit), order_id),
            )
        conn.commit()

    def set_trade_close_info(self, order_id: str, close_order_id: str, close_price: float) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE trades SET close_order_id = ?, close_price = ? WHERE order_id = ?",
            (close_order_id, float(close_price), order_id),
        )
        conn.commit()

    def set_trade_sl_order(self, order_id: str, sl_order_id: str, sl_price: float) -> None:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "UPDATE trades SET sl_order_id = ?, sl_price = ? WHERE order_id = ?",
            (sl_order_id, float(sl_price), order_id),
        )
        conn.commit()

    def get_last_signal_time(self, symbol_id: int) -> Optional[datetime]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timestamp FROM signals WHERE symbol_id = ? ORDER BY timestamp DESC LIMIT 1
            """,
            (symbol_id,),
        )
        row = cur.fetchone()
        return datetime.fromisoformat(row["timestamp"]) if row else None

    def get_last_trade_time(self, symbol_id: int) -> Optional[datetime]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT created_at FROM trades WHERE symbol_id = ? ORDER BY id DESC LIMIT 1
            """,
            (symbol_id,),
        )
        row = cur.fetchone()
        return datetime.fromisoformat(row["created_at"]) if row and row["created_at"] else None

    def get_last_price(self, symbol_id: int) -> Optional[float]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            "SELECT price FROM price_data WHERE symbol_id = ? ORDER BY timestamp DESC LIMIT 1",
            (symbol_id,),
        )
        row = cur.fetchone()
        return float(row["price"]) if row else None

    def get_last_closed_pnls(self, limit: int) -> List[float]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT pnl FROM trades
            WHERE status = 'closed' AND pnl IS NOT NULL
            ORDER BY closed_at DESC
            LIMIT ?
            """,
            (int(limit),),
        )
        rows = cur.fetchall()
        return [float(r["pnl"]) for r in rows]

    def get_today_pnl(self) -> float:
        conn = self._get_conn()
        cur = conn.cursor()
        # Filter by UTC date of 'closed_at'
        cur.execute(
            """
            SELECT COALESCE(SUM(pnl), 0) AS d
            FROM trades
            WHERE status = 'closed' AND pnl IS NOT NULL AND DATE(closed_at) = DATE('now')
            """
        )
        row = cur.fetchone()
        return float(row["d"]) if row and row["d"] is not None else 0.0

    def get_last_signal_time_queued(self, symbol_id: int) -> Optional[datetime]:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timestamp FROM signals
            WHERE symbol_id = ? AND action_taken = 'queued'
            ORDER BY timestamp DESC LIMIT 1
            """,
            (symbol_id,),
        )
        row = cur.fetchone()
        return datetime.fromisoformat(row["timestamp"]) if row else None 

    def get_total_pnl(self) -> float:
        conn = self._get_conn()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT COALESCE(SUM(pnl), 0) AS total_pnl
            FROM trades
            WHERE status = 'closed' AND pnl IS NOT NULL
            """
        )
        row = cur.fetchone()
        return float(row["total_pnl"]) if row and row["total_pnl"] is not None else 0.0 