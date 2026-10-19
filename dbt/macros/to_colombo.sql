{#- APIs give times as epoch milliseconds in UTC. Sri Lanka is always UTC+05:30
    (no daylight saving), so adding 330 minutes gives Colombo time. -#}
{% macro epoch_ms_to_colombo(col) -%}
    (epoch_ms(({{ col }})::bigint) + interval 330 minute)
{%- endmacro %}
