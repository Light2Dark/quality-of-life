{{config(materialized='view')}}

select
    COALESCE(w.datetime, aq.datetime) as datetime,
    COALESCE(w.city, aq.city) as city,
    AVG(temperature) as temperature,
    AVG(feels_like_temperature) as feels_like_temperature,
    AVG(pressure) as pressure,
    AVG(dew_point) as dew_point,
    AVG(relative_humidity) as relative_humidity,
    AVG(wind_speed) as wind_speed,
    AVG(gust) as gust,
    AVG(wind_chill) as wind_chill,
    AVG(uv_index) as uv_index,
    AVG(visibility) as visibility,
    AVG(pollutant_value) as air_pollutant_value
from {{ref('weather')}} as w
full join {{ref('air_quality')}} as aq
ON w.datetime = aq.datetime AND w.city = aq.city
GROUP BY datetime, city
{% if var('test_run', default=true) %}
  limit 100
{% endif %}