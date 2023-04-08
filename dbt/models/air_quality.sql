{{config(materialized="view")}}

select 
    timestamp,
    city,
    pollutant,
    pollutant_value,
    unit
from {{ref('stg_hourly_air_quality')}}
left join {{ref('air_quality_indicators')}} ON pollutant_symbol = symbol
{% if var('is_test_run', default=true) %}
  limit 100
{% endif %}