{{config(materialized='incremental')}}

WITH combined_data AS (
    select
        COALESCE(w.datetime, aq.datetime, pws.datetime) as datetime,
        COALESCE(w.place, aq.place, pws.place) as place,
        COALESCE(w.city, aq.city, pws.city) as city,
        COALESCE(w.state, aq.state, pws.state) as state,
        AVG(pws.temperature) AS pws_temperature,
        AVG(w.temperature) AS weather_temperature,
        AVG(feels_like_temperature) as feels_like_temperature,
        AVG(pws.pressure) AS pws_pressure,
        AVG(w.pressure) AS weather_pressure,
        AVG(pws.dew_point) AS pws_dew_point,
        AVG(w.dew_point) AS weather_dew_point,
        AVG(w.relative_humidity) AS weather_humidity,
        AVG(pws.humidity) AS pws_humidity,
        AVG(pws.gust) AS pws_gust,
        AVG(w.gust) AS weather_gust,
        AVG(pws.wind_speed) AS pws_wind_speed,
        AVG(w.wind_speed) AS weather_wind_speed,
        AVG(pws.wind_chill) AS pws_wind_chill,
        AVG(w.wind_chill) AS weather_wind_chill,
        AVG(pws.uv_index) AS pws_uv_index,
        AVG(w.uv_index) AS weather_uv_index,
        AVG(visibility) AS visibility,
        AVG(solar_radiation) AS solar_radiation,
        AVG(pollutant_value) AS pollutant_value,
        AVG(precipitation_rate) as precipitation_rate,
        AVG(precipitation_total) as precipitation_total
    from {{ref('weather')}} as w
    full join {{ref('air_quality')}} as aq
    ON w.datetime = aq.datetime AND w.city = aq.city
    full join {{ref('personal_weather')}} as pws
    ON pws.datetime = aq.datetime AND pws.city = aq.city
    {% if is_incremental() %}
        where datetime > max(datetime) from {{this}}
    {% endif %}
    GROUP BY datetime, place, city, state
)

-- Adding to null will return null
SELECT
    datetime,
    place,
    city,
    state,
    COALESCE((pws_temperature + weather_temperature) / 2, pws_temperature, weather_temperature) as temperature,
    COALESCE((pws_pressure + weather_pressure) / 2, pws_pressure, weather_pressure) as pressure,
    COALESCE((pws_dew_point + weather_dew_point) / 2, pws_dew_point, weather_dew_point) as dew_point,
    COALESCE((pws_humidity + weather_humidity) / 2, pws_humidity, weather_humidity) as humidity,
    COALESCE((pws_wind_speed + weather_wind_speed) / 2, pws_wind_speed, weather_wind_speed) as wind_speed,
    COALESCE((pws_gust + weather_gust) / 2, pws_gust, weather_gust) as gust,
    COALESCE((pws_wind_chill + weather_wind_chill) / 2, pws_wind_chill, weather_wind_chill) as wind_chill,
    COALESCE((pws_uv_index + weather_uv_index) / 2, pws_uv_index, weather_uv_index) as uv_index,
    feels_like_temperature,
    visibility,
    solar_radiation,
    pollutant_value,
    precipitation_rate,
    precipitation_total
FROM combined_data



    