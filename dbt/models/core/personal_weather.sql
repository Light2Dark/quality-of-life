select
    datetime,
    place,
    city,
    state,
    solar_radiation,
    uv_index,
    humidity,
    temperature,
    precipitation_rate,
    precipitation_total,
    pressure,
    pressure_trend,
    wind_speed,
    gust,
    dew_point,
    heat_index,
    wind_direction_degree,
    wind_chill
from {{ref('stg_hourly_pws_weather')}} w left join {{ref('full_locations')}} fl
ON w.weather_station = fl.PWStation
{% if var('test_run', default=true) %}
limit 100
{% endif %}