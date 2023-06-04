{{config(materialized='view')}}

select
    w.datetime as datetime,
    COALESCE(w.location, aq.location) as location,
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
    pollutant as air_pollutant,
    pollutant_value as air_pollutant_value,
    day_indicator
from {{ref('weather')}} as w
full join {{ref('air_quality')}} as aq
ON w.datetime = aq.datetime AND w.location = aq.location
{% if var('test_run', default=true) %}
  limit 100
{% endif %}