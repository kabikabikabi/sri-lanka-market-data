-- One daily price per coin, from two sources:
--   Binance history (candles):  [open_time_ms, open, high, low, close, volume, ...]
--   Finnhub live quote:         {"c": current price, "t": unix seconds, ...}
-- Binance wins when both have the same day; Finnhub fills in today.
with binance as (
    select key as symbol, fetched_at_utc, unnest(from_json(payload, '["json"]')) as candle
    from {{ source('raw', 'crypto_history') }}
),

unioned as (
    select
        symbol,
        cast(epoch_ms(json_extract(candle, '$[0]')::bigint) as date) as price_date,
        json_extract_string(candle, '$[4]')::double                   as price_usdt,
        'binance' as source, 1 as priority, fetched_at_utc
    from binance

    union all

    select
        key,
        cast(epoch_ms((payload->>'t')::bigint * 1000) as date),
        (payload->>'c')::double,
        'finnhub', 2, fetched_at_utc
    from {{ source('raw', 'crypto_quotes') }}
    where (payload->>'c')::double > 0
)

select
    symbol,
    replace(split_part(symbol, ':', 2), 'USDT', '') as coin,     -- BINANCE:BTCUSDT -> BTC
    price_date,
    price_usdt,
    source
from unioned
qualify row_number() over (partition by symbol, price_date order by priority, fetched_at_utc desc) = 1
