{{config(materialized='table')}}

select
    COALESCE(w.datetime, aq.datetime, pws.datetime) as datetime,
    COALESCE(w.place, aq.place, pws.place) as place,
    COALESCE(w.city, aq.city, pws.city) as city,
    COALESCE(w.state, aq.state, pws.state) as state,
    AVG(temperature) as temperature,
    AVG(feels_like_temperature) as feels_like_temperature,
    AVG(pressure) as pressure,
    AVG(dew_point) as dew_point,
    AVG(humidity) as humidity,
    AVG(relative_humidity) as relative_humidity,
    AVG(wind_speed) as wind_speed,
    AVG(gust) as gust,
    AVG(wind_chill) as wind_chill,
    AVG(solar_radiation) as solar_radiation,
    AVG(uv_index) as uv_index,
    AVG(visibility) as visibility,
    AVG(pollutant_value) as air_pollutant_value,
    AVG(precipitation_rate) as precipitation_rate,
    AVG(precipitation_total) as precipitation_total
from {{ref('weather')}} as w
full join {{ref('air_quality')}} as aq
ON w.datetime = aq.datetime AND w.city = aq.city
full join {{ref('personal_weather')}} as pws
ON pws.datetime = aq.datetime AND pws.place = aq.place
GROUP BY datetime, place, city, state
{% if var('test_run', default=true) %}
  limit 100
{% endif %}
    