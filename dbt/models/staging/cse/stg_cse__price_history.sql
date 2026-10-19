-- One row per company per day from the 1-year history.
-- The history is downloaded again every day, so keep only the newest copy of each day.
with days as (
    select
        key as symbol,
        fetched_at_utc,
        unnest(from_json(payload, '["json"]')) as day     -- turn the JSON list into rows
    from {{ source('raw', 'cse_price_history') }}
)

select
    symbol,
    cast({{ epoch_ms_to_colombo("day->>'t'") }} as date) as trade_date,
    (day->>'p')::double as close_price,
    (day->>'q')::bigint as share_volume,
    fetched_at_utc
from days
where day->>'p' is not null
qualify row_number() over (partition by symbol, trade_date order by fetched_at_utc desc) = 1
