-- One row per currency per day: units of `currency` you get for 1 USD.
-- Payload: {"base_code": "USD", "time_last_update_unix": ..., "rates": {"LKR": 330.9, "GBP": 0.75, ...}}
with rates as (
    select
        cast({{ epoch_ms_to_colombo("(payload->>'time_last_update_unix')::bigint * 1000") }} as date) as rate_date,
        unnest(json_keys(payload->'rates')) as currency,          -- one row per currency
        payload->'rates'                    as rate_map,
        fetched_at_utc
    from {{ source('raw', 'fx_rates') }}
)

select
    rate_date,
    currency,
    json_extract(rate_map, '$.' || currency)::double as per_usd,
    fetched_at_utc
from rates
qualify row_number() over (partition by rate_date, currency order by fetched_at_utc desc) = 1
