{{config(materialized='table')}}

select
    COALESCE(w.datetime, aq.datetime, pws.datetime) as datetime,
    COALESCE(w.place, aq.place, pws.place) as place,
    COALESCE(w.city, aq.city, pws.city) as city,
    COALESCE(w.state, aq.state, pws.state) as state,
    AVG((pws.temperature + w.temperature) / 2) as temperature,
    AVG(feels_like_temperature) as feels_like_temperature,
    AVG((pws.pressure + w.pressure) / 2) as pressure,
    AVG((pws.dew_point + w.dew_point) / 2) as dew_point,
    AVG((humidity + relative_humidity) / 2) as humidity,
    AVG((pws.wind_speed + w.wind_speed) / 2) as wind_speed,
    AVG((pws.gust + w.gust) / 2) as gust,
    AVG((pws.wind_chill + w.wind_chill) / 2) as wind_chill,
    AVG(solar_radiation) as solar_radiation,
    AVG((pws.uv_index + w.uv_index) / 2) as uv_index,
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
    