{{config(materialized='view')}}

select
    w.datetime,
    temperature,
    feels_like_temperature,
    pressure,
    dew_point,
    relative_humidity,
    wind_speed,
    wind_chill,
    wind_direction_degree,
    wind_direction_dir,
    uv_index,
    clouds,
    visibility,
    day_indicator
from {{ref('weather')}} as w
-- inner join {{ref('air_quality')}} as aq ##### INNER JOIN isn't righttt
ON w.datetime = aq.datetime
{% if var('test_run', default=true) %}
  limit 100
{% endif %}