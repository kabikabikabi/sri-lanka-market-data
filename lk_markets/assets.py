"""Dagster assets: fetch -> raw tables -> dbt models -> Power BI files."""
import csv
import shutil
import sys
from pathlib import Path

import dagster_dbt
import duckdb
from dagster import AssetCheckResult, AssetExecutionContext, MaterializeResult, asset, asset_check
from dagster_dbt import DbtCliResource, DbtProject, dbt_assets, get_asset_key_for_model

from lk_markets import sources
from lk_markets.warehouse import DB_PATH, ROOT, query, save_raw

DBT_DIR = ROOT / "dbt"


def watchlist() -> list[str]:
    """Symbols from dbt/seeds/cse_watchlist.csv (one list shared by Python and dbt)."""
    with (DBT_DIR / "seeds" / "cse_watchlist.csv").open() as f:
        return [row["symbol"] for row in csv.DictReader(f)]


# ------------------------------------------------------------ EXTRACT + LOAD ----
@asset(key_prefix="raw", group_name="cse")
def cse_trade_summary() -> MaterializeResult:
    """Today's prices for every company on the Colombo Stock Exchange."""
    rows = sources.cse_trade_summary()
    n = save_raw("cse_trade_summary", [(r["symbol"], r) for r in rows])
    return MaterializeResult(metadata={"companies": n})


@asset_check(asset=cse_trade_summary, description="The CSE lists ~280 companies; far fewer means a broken response.")
def all_companies_received() -> AssetCheckResult:
    n = query("""select count(*) from raw.cse_trade_summary
                 where fetched_at_utc = (select max(fetched_at_utc) from raw.cse_trade_summary)""")[0][0]
    return AssetCheckResult(passed=n >= 200, metadata={"companies_in_latest_fetch": n})


@asset(key_prefix="raw", group_name="cse", deps=[cse_trade_summary])
def cse_price_history(context: AssetExecutionContext) -> MaterializeResult:
    """One year of daily prices for each watchlist company."""
    # The history endpoint needs the CSE's internal id, which the trade summary gives us.
    ids = dict(query("""select key, (payload->>'id')::int from raw.cse_trade_summary
                        qualify row_number() over (partition by key order by fetched_at_utc desc) = 1"""))
    records = []
    for symbol in watchlist():
        if symbol in ids:
            records.append((symbol, sources.cse_price_history(ids[symbol])))
        else:
            context.log.warning("%s is not listed any more", symbol)
    return MaterializeResult(metadata={"companies": save_raw("cse_price_history", records)})


@asset(key_prefix="raw", group_name="fx")
def fx_rates() -> MaterializeResult:
    """Today's exchange rates (per 1 USD), including LKR."""
    data = sources.usd_exchange_rates()
    save_raw("fx_rates", [("USD", data)])
    return MaterializeResult(metadata={"lkr_per_usd": data["rates"]["LKR"]})


@asset(key_prefix="raw", group_name="crypto")
def crypto_history() -> MaterializeResult:
    """One year of daily crypto prices from Binance."""
    records = [(s, sources.binance_daily_candles(s)) for s in sources.CRYPTO_SYMBOLS]
    return MaterializeResult(metadata={"coins": save_raw("crypto_history", records)})


@asset(key_prefix="raw", group_name="crypto")
def crypto_quotes(context: AssetExecutionContext) -> MaterializeResult:
    """Latest crypto prices from Finnhub (skipped if FINNHUB_API_KEY isn't set in .env)."""
    records = [(s, q) for s in sources.CRYPTO_SYMBOLS if (q := sources.finnhub_quote(s))]
    if not records:
        context.log.warning("No Finnhub key in .env - skipping live quotes")
    return MaterializeResult(metadata={"coins": save_raw("crypto_quotes", records)})


# ---------------------------------------------------------------- TRANSFORM ----
dbt_project = DbtProject(project_dir=DBT_DIR, profiles_dir=DBT_DIR)


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project)
def dbt_models(context: AssetExecutionContext, dbt: DbtCliResource):
    """Every dbt model becomes an asset; `dbt build` runs the models and their tests."""
    yield from dbt.cli(["build"], context=context).stream()


def find_dbt() -> str:
    """The dbt.exe in this virtual environment (works even when it isn't activated)."""
    venv = Path(dagster_dbt.__file__).resolve().parents[3]
    for exe in (venv / "Scripts" / "dbt.exe", venv / "bin" / "dbt", Path(sys.executable).with_name("dbt.exe")):
        if exe.exists():
            return str(exe)
    return shutil.which("dbt") or "dbt"


# ------------------------------------------------------------------- REPORT ----
MARTS = ["fct_stock_prices", "mart_watchlist_stats", "fct_lkr_rates", "fct_crypto_lkr"]


@asset(group_name="reporting", deps=[get_asset_key_for_model([dbt_models], m) for m in MARTS])
def powerbi_export() -> MaterializeResult:
    """Copy the final tables to Parquet files for Power BI (Get data > Parquet)."""
    out = ROOT / "data" / "powerbi"
    out.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(DB_PATH), read_only=True) as con:
        for table in MARTS:
            con.execute(f"copy marts.{table} to '{(out / table).as_posix()}.parquet' (format parquet)")
    return MaterializeResult(metadata={"files": len(MARTS), "folder": str(out)})
