from __future__ import annotations

from bybit_trading_bot.config.settings import load_settings
from bybit_trading_bot.utils.db_manager import DBManager
from bybit_trading_bot.core.symbol_mapper import SymbolMapper


def test_symbol_mapper_instantiation(tmp_path):
    cfg = load_settings()
    db = DBManager(str(tmp_path / "db.sqlite"))
    mapper = SymbolMapper(cfg, db)
    # get_common_symbols may be empty offline; just ensure it returns a list
    syms = mapper.get_common_symbols()
    assert isinstance(syms, list) 