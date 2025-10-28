from __future__ import annotations

from typing import Dict, List, Set

from bybit_trading_bot.config.settings import Config
from bybit_trading_bot.utils.logger import get_logger
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.utils.http_client import RateLimitedHTTP

try:
    from pybit.unified_trading import HTTP
except Exception:  # pragma: no cover - allows running without pybit installed at dev time
    HTTP = None  # type: ignore


class SymbolMapper:
    """Сопоставление спот и фьючерсных пар."""

    def __init__(self, config: Config, db: DBManager) -> None:
        self.config = config
        self.db = db
        self.logger = get_logger(self.__class__.__name__)
        self._raw_http = None
        self._http = None
        if HTTP is not None:
            try:
                self._raw_http = HTTP(testnet=self.config.bybit_testnet)
                self._http = RateLimitedHTTP(self._raw_http, max_requests=90, per_seconds=3.0)
            except Exception as e:
                self.logger.warning(f"Failed to init Bybit HTTP client: {e}")

    def _fetch_all_symbols(self, category: str) -> Set[str]:
        if self._http is None:
            return set()
        symbols: Set[str] = set()
        cursor: str | None = None
        try:
            while True:
                args = {"category": category}
                if cursor:
                    args["cursor"] = cursor
                resp = self._http.request("get_instruments_info", **args)
                if not isinstance(resp, dict):
                    break
                result = resp.get("result", {})
                items = result.get("list", []) if isinstance(result, dict) else []
                for it in items:
                    sym = it.get("symbol")
                    if sym:
                        symbols.add(str(sym))
                cursor = result.get("nextPageCursor") if isinstance(result, dict) else None
                if not cursor:
                    break
        except Exception as e:
            self.logger.error(f"Failed to fetch instruments for {category}: {e}")
        return symbols

    def get_common_symbols(self) -> List[str]:
        spot = self._fetch_all_symbols("spot")
        linear = self._fetch_all_symbols("linear")
        common = sorted(list(spot & linear))
        self.logger.info(f"Common symbols found: {len(common)}")
        return common

    def create_symbol_mapping(self) -> int:
        common = set(self.get_common_symbols())
        count = 0
        for symbol in common:
            try:
                self.db.upsert_symbol(spot_symbol=symbol, futures_symbol=symbol, is_active=True)
                count += 1
            except Exception as e:
                self.logger.error(f"Failed to upsert symbol {symbol}: {e}")
        try:
            current = {rec.spot_symbol for rec in self.db.get_active_symbols()}
            missing = current - common
            for sym in missing:
                self.db.set_symbol_active(sym, False)
            if missing:
                self.logger.info(f"Deactivated {len(missing)} symbols no longer in common set")
        except Exception as e:
            self.logger.error(f"Failed to deactivate missing symbols: {e}")
        self.logger.info(f"Symbol mapping upserted: {count} records")
        return count

    def is_symbol_active(self, spot_symbol: str) -> bool:
        active = {rec.spot_symbol for rec in self.db.get_active_symbols()}
        return spot_symbol in active 