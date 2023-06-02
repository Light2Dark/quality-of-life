{{config(materialized='view')}}

select
    datetime,
    weather_station,
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
from {{ref('stg_hourly_weather')}}
{% if var('test_run', default=true) %}
  limit 100
{% endif %}