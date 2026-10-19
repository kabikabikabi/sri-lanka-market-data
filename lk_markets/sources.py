"""Every external API the platform calls, in one place.

| Data                         | API                                   | Key?            |
|------------------------------|---------------------------------------|-----------------|
| CSE stocks (today + 1 year)  | cse.lk (unofficial website endpoints) | no              |
| Latest crypto prices         | Finnhub                               | FINNHUB_API_KEY |
| Crypto price history         | Binance public market data            | no              |
| LKR exchange rates           | open.er-api.com                       | no              |
"""
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

CSE_API = "https://www.cse.lk/api/"
CRYPTO_SYMBOLS = ["BINANCE:BTCUSDT", "BINANCE:ETHUSDT", "BINANCE:SOLUSDT", "BINANCE:BNBUSDT", "BINANCE:XRPUSDT"]


def call_api(method: str, url: str, pause: float = 0.0, **kwargs):
    """Call an API and return its JSON. Retries up to 3 times if the server is busy (429/5xx)."""
    for attempt in range(4):
        response = requests.request(method, url, timeout=30, **kwargs)
        if response.status_code == 200:
            time.sleep(pause)  # be polite to free APIs
            return response.json()
        if response.status_code in (429, 500, 502, 503, 504) and attempt < 3:
            time.sleep(2 ** attempt)  # wait 1s, 2s, 4s
            continue
        response.raise_for_status()


# ---- Colombo Stock Exchange (POST with form data, like the cse.lk website does) ----
def cse_trade_summary() -> list[dict]:
    """Today's trading for every listed company (~283 rows)."""
    return call_api("POST", CSE_API + "tradeSummary", pause=1.5)["reqTradeSummery"]


def cse_price_history(security_id: int) -> list[dict]:
    """One year of daily prices: t = date (epoch ms), p = close, h = high, l = low, q = volume."""
    data = call_api("POST", CSE_API + "companyChartDataByStock", pause=1.5,
                    data={"stockId": security_id, "period": 5})
    return data["chartData"]


# ---- Crypto ----
def finnhub_quote(symbol: str) -> dict | None:
    """Latest price {c: current, h, l, o, t}. Returns None if no API key is set."""
    key = os.environ.get("FINNHUB_API_KEY")
    if not key:
        return None
    return call_api("GET", "https://finnhub.io/api/v1/quote", params={"symbol": symbol},
                    headers={"X-Finnhub-Token": key})  # key in a header, never in the URL


def binance_daily_candles(symbol: str, days: int = 365) -> list[list]:
    """[[open_time_ms, open, high, low, close, volume, ...], ...]"""
    pair = symbol.split(":")[-1]  # "BINANCE:BTCUSDT" -> "BTCUSDT"
    return call_api("GET", "https://api.binance.com/api/v3/klines",
                    params={"symbol": pair, "interval": "1d", "limit": days})


# ---- FX ----
def usd_exchange_rates() -> dict:
    """{"base_code": "USD", "time_last_update_unix": ..., "rates": {"LKR": 330.9, "GBP": 0.75, ...}}"""
    return call_api("GET", "https://open.er-api.com/v6/latest/USD")
