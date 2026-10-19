"""Tests: the raw store, and the dbt models run against sample API data (no internet needed)."""
import os
from datetime import UTC, datetime

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from lk_markets.warehouse import ROOT, save_raw

DBT_DIR = ROOT / "dbt"

SAT = int(datetime(2026, 10, 17, tzinfo=UTC).timestamp())  # a Saturday, 00:00 UTC
DAY = 86400


def test_save_raw_appends_rows(tmp_path):
    db = tmp_path / "w.duckdb"
    save_raw("cse_trade_summary", [("JKH.N0000", {"price": 17.7})], db)
    save_raw("cse_trade_summary", [("JKH.N0000", {"price": 17.9})], db)
    with duckdb.connect(str(db)) as con:
        prices = con.execute("select payload->>'price' from raw.cse_trade_summary").fetchall()
    assert prices == [("17.7",), ("17.9",)]


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    """Load sample API responses, run the real `dbt build` on them, return a connection."""
    tmp = tmp_path_factory.mktemp("wh")
    db = tmp / "test.duckdb"
    # Friday's exchange rates (published Fri 00:00 UTC)
    save_raw("fx_rates", [("USD", {"base_code": "USD", "time_last_update_unix": SAT - DAY,
                                   "rates": {"USD": 1, "LKR": 300.0, "GBP": 0.8, "ZAR": 18.0}})], db)
    # Binance candles for Sat + Sun, and a Finnhub quote for Monday
    save_raw("crypto_history", [("BINANCE:BTCUSDT", [
        [SAT * 1000, "100", "111", "99", "110", "1"],
        [(SAT + DAY) * 1000, "110", "121", "109", "120", "1"],
    ])], db)
    save_raw("crypto_quotes", [("BINANCE:BTCUSDT", {"c": 130.0, "t": SAT + 2 * DAY + 3600})], db)
    # Two days of CSE history for JKH, plus the snapshot for the second day
    save_raw("cse_price_history", [("JKH.N0000", [
        {"t": (SAT - 2 * DAY) * 1000 - 19800000, "p": 20.0, "q": 1000},   # Thu 00:00 Colombo
        {"t": (SAT - DAY) * 1000 - 19800000, "p": 21.0, "q": 1000},        # Fri 00:00 Colombo
    ])], db)
    save_raw("cse_trade_summary", [("JKH.N0000", {"name": "JOHN KEELLS HOLDINGS PLC", "closingPrice": 22.0,
                                                  "price": 22.0, "sharevolume": 5, "marketCap": 1,
                                                  "lastTradedTime": (SAT - DAY) * 1000 + 3600000})], db)

    os.environ["LK_WAREHOUSE"] = str(db)  # profiles.yml reads the database path from here
    try:
        result = dbtRunner().invoke(["build", "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR),
                                     "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs"),
                                     "--no-partial-parse", "--quiet"])
    finally:
        os.environ.pop("LK_WAREHOUSE")
    assert result.success, result.exception
    with duckdb.connect(str(db)) as con:  # same settings as dbt's open connection
        yield con


def test_lkr_rates(warehouse):
    rates = dict(warehouse.execute("select currency, lkr_per_unit from marts.fct_lkr_rates").fetchall())
    assert rates == {"USD": 300.0, "GBP": 375.0}  # ZAR isn't in the tracked list


def test_weekend_crypto_uses_fridays_rate(warehouse):
    rows = warehouse.execute(
        "select price_date::varchar, price_usdt, price_lkr, source from marts.fct_crypto_lkr order by price_date"
    ).fetchall()
    assert rows == [("2026-10-17", 110.0, 33000.0, "binance"),
                    ("2026-10-18", 120.0, 36000.0, "binance"),
                    ("2026-10-19", 130.0, 39000.0, "finnhub")]


def test_snapshot_wins_over_history_on_the_same_day(warehouse):
    rows = warehouse.execute(
        "select trade_date::varchar, close_price from marts.fct_stock_prices order by trade_date"
    ).fetchall()
    assert rows == [("2026-10-15", 20.0), ("2026-10-16", 22.0)]
