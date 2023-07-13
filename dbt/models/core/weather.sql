{{config(materialized='incremental')}}

select distinct
    datetime,
    identifying_location as location,
    place,
    city,
    state,
    temperature,
    feels_like_temperature,
    pressure,
    dew_point,
    relative_humidity,
    gust,
    wind_speed,
    wind_chill,
    wind_direction_degree,
    wind_direction_dir,
    uv_index,
    clouds,
    visibility,
    day_indicator
from {{ref('stg_hourly_weather')}} as stg_hw left join {{ref('full_locations')}} as fl
on stg_hw.weather_station = fl.ICAO
{% if is_incremental() %}
    where datetime > max(datetime) from {{this}}
{% endif %}