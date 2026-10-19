-- One row per company per trading day from the daily all-market snapshot.
-- The trade date comes from the last trade time (a weekend fetch still describes Friday).
select
    key                                                           as symbol,
    payload->>'name'                                              as company_name,
    coalesce(nullif((payload->>'closingPrice')::double, 0),
             (payload->>'price')::double)                         as close_price,
    (payload->>'sharevolume')::bigint                             as share_volume,
    (payload->>'marketCap')::double                               as market_cap_lkr,
    cast({{ epoch_ms_to_colombo("payload->>'lastTradedTime'") }} as date) as trade_date,
    fetched_at_utc
from {{ source('raw', 'cse_trade_summary') }}
where payload->>'lastTradedTime' is not null
qualify row_number() over (partition by symbol, trade_date order by fetched_at_utc desc) = 1
