{{config(materialized="view")}}

select 
    datetime,
    location,
    pollutant,
    pollutant_value
from {{ref('stg_hourly_air_quality')}}
left join {{ref('air_quality_indicators')}} ON pollutant_symbol = symbol
{% if var('test_run', default=true) %}
  limit 100
{% endif %}