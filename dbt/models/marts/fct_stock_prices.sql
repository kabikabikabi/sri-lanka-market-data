-- Daily closing price per CSE company, with the % change from the previous trading day.
-- Combines the 1-year history (watchlist) with the daily snapshot (every company).
-- If both have the same day, the snapshot wins: it is taken after the market closes.
with both_sources as (
    select symbol, trade_date, close_price, share_volume, 1 as priority
    from {{ ref('stg_cse__trade_summary') }}
    union all
    select symbol, trade_date, close_price, share_volume, 2 as priority
    from {{ ref('stg_cse__price_history') }}
),

one_per_day as (
    select * from both_sources
    qualify row_number() over (partition by symbol, trade_date order by priority) = 1
)

select
    symbol,
    trade_date,
    close_price,
    share_volume,
    close_price / lag(close_price) over (partition by symbol order by trade_date) - 1 as daily_return
from one_per_day
where close_price > 0
