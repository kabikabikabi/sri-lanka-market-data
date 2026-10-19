-- How many rupees (LKR) one unit of each currency costs, per day.
-- Rates are quoted per 1 USD, so:  LKR per X = (LKR per USD) / (X per USD)
-- Example: 1 USD = 330 LKR and 1 USD = 0.75 GBP  ->  1 GBP = 330 / 0.75 = 440 LKR
with rates as (
    select * from {{ ref('stg_fx__rates') }}
),

lkr as (
    select rate_date, per_usd as lkr_per_usd from rates where currency = 'LKR'
)

select
    r.rate_date,
    r.currency,
    l.lkr_per_usd / r.per_usd as lkr_per_unit
from rates r
join lkr l using (rate_date)
where r.currency in ({{ "'" ~ var('fx_currencies') | join("', '") ~ "'" }})
