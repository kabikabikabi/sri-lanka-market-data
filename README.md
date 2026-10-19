# Sri Lanka Market Data

A daily data pipeline for Sri Lankan market data.

Every day after the Colombo Stock Exchange closes it collects

- stock prices for all CSE companies plus one year of history for 10 companies
- LKR exchange rates for 9 currencies
- prices for 5 cryptocurrencies in US dollars and in rupees

It saves the data, cleans it with SQL and makes tables for Power BI.

## How it works

1. **Collect** Python calls four free APIs and saves each answer as it came into a DuckDB database
2. **Clean** dbt SQL models turn the raw data into clean tables with one row per day
3. **Build** four final tables
   - stock prices with the daily change
   - 1 month return, 1 year return and volatility for each watchlist company
   - LKR rates for each currency
   - crypto prices in LKR
4. **Test** dbt tests check for missing values and duplicate rows
5. **Export** the final tables are saved as Parquet files for Power BI

Dagster runs these steps in the right order and can run them every day at 3 pm.

## Data sources

| Data | Source | API key |
|---|---|---|
| CSE stock prices | cse.lk | not needed |
| LKR exchange rates | open.er-api.com | not needed |
| Crypto price history | Binance | not needed |
| Live crypto prices | Finnhub | needed |

The cse.lk API is the one the CSE website uses itself. It is not an official public API
so it can change. The code waits between calls to keep the load low.

## Tools

Python, Dagster, DuckDB, dbt and Power BI

## Run it

```
python -m venv .venv
.venv\Scripts\activate
pip install -e .
cd dbt
dbt parse
cd ..
dagster dev
```

Open http://localhost:3000 and click **Materialize all**.

For live crypto prices get a free key from finnhub.io and put it in a file called `.env`

```
FINNHUB_API_KEY=your_key
```

The project works without it. Only the live crypto prices are skipped.

To run the tests use `pip install -e ".[dev]"` and then `pytest`.

## Files

```
lk_markets/
  sources.py      calls the APIs
  warehouse.py    saves raw data in DuckDB
  assets.py       the pipeline steps
  definitions.py  connects the steps for Dagster
dbt/
  models/staging  clean the raw data
  models/marts    the final tables
  seeds           the list of watchlist companies
  tests           data tests
tests/            tests
```
