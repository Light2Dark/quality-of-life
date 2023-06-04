{{config(materialized='view')}}

select
    datetime,
    sl.identifying_location as location,
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
from {{ref('stg_hourly_weather')}} as stg_hw left join {{ref('state_locations')}} as sl
on stg_hw.weather_station = sl.ICAO
{% if var('test_run', default=true) %}
  limit 100
{% endif %}