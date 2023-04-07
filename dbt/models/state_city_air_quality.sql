{{config(materialized='view')}}

select
    city,
    state,
    timestamp,
    pollutant_value
from {{ref('air_quality')}}
left join {{ref('state_locations')}} on location = city
{% if var('is_test_run', default=true) %}
  limit 100
{% endif %}