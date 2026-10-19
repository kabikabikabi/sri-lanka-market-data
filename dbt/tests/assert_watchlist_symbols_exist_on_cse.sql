{{ config(severity='warn') }}
-- Every watchlist company should appear in today's CSE data; if not, it was renamed or delisted.
select w.symbol
from {{ ref('cse_watchlist') }} w
where w.symbol not in (select symbol from {{ ref('stg_cse__trade_summary') }})
