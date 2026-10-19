-- fct_stock_prices must have exactly one row per company per day. Returns the rows that break this.
select symbol, trade_date, count(*) as copies
from {{ ref('fct_stock_prices') }}
group by symbol, trade_date
having count(*) > 1
