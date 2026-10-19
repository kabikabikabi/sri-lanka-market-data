-- Crypto prices in USDT and in LKR (treating 1 USDT as 1 USD).
-- ASOF JOIN = "use the latest USD/LKR rate on or before this day", so weekend prices
-- get Friday's rate. Days before the first FX download have no LKR price yet.
with usd_lkr as (
    select rate_date, lkr_per_unit as usd_lkr
    from {{ ref('fct_lkr_rates') }}
    where currency = 'USD'
)

select
    c.coin,
    c.price_date,
    c.price_usdt,
    c.price_usdt * fx.usd_lkr as price_lkr,
    c.source
from {{ ref('stg_crypto__prices') }} c
asof left join usd_lkr fx on c.price_date >= fx.rate_date
