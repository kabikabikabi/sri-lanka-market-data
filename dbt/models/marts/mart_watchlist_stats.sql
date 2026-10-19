-- Simple performance and risk numbers for each watchlist company.
--   return_1m / return_1y : % change since the first trading day in the last 30 / 365 days
--   volatility            : how much the price swings in a year (std dev of daily returns x sqrt(242 trading days))
with prices as (
    select p.*
    from {{ ref('fct_stock_prices') }} p
    join {{ ref('cse_watchlist') }} using (symbol)
),

latest as (
    select symbol, max(trade_date) as latest_date from prices group by symbol
)

select
    w.symbol,
    w.company_short_name,
    w.sector,
    l.latest_date,
    arg_max(p.close_price, p.trade_date)                                                    as latest_close,
    arg_max(p.close_price, p.trade_date)
        / arg_min(p.close_price, p.trade_date) filter (where p.trade_date >= l.latest_date - 30) - 1  as return_1m,
    arg_max(p.close_price, p.trade_date)
        / arg_min(p.close_price, p.trade_date) filter (where p.trade_date >= l.latest_date - 365) - 1 as return_1y,
    stddev_samp(p.daily_return) * sqrt(242)                                                 as volatility
from prices p
join latest l using (symbol)
join {{ ref('cse_watchlist') }} w using (symbol)
group by all
order by return_1y desc
