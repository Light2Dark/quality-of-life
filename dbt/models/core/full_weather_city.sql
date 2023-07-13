{{config(materialized='incremental')}}

SELECT
    datetime,
    city,
    state,
    AVG(temperature) as temperature,
    AVG(pressure) as pressure,
    AVG(dew_point) as dew_point,
    AVG(humidity) as humidity,
    AVG(wind_speed) as wind_speed,
    AVG(gust) as gust,
    AVG(wind_chill) as wind_chill,
    AVG(uv_index) as uv_index,
    AVG(feels_like_temperature) as feels_like_temperature,
    AVG(visibility) as visibility,
    AVG(solar_radiation) as solar_radiation,
    AVG(pollutant_value) as pollutant_value,
    AVG(precipitation_rate) as precipitation_rate,
    AVG(precipitation_total) as precipitation_total
FROM {{ref('full_weather_places')}}
group by datetime, city, state