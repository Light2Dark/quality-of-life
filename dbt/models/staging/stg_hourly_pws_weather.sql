{{config(materialized="view")}}

WITH formatted_hourly_weather AS 
(
    select
        DATETIME(datetime) as datetime,
        weather_station,
        SAFE_CAST(solar_radiation_high AS FLOAT64) as solar_radiation,
        SAFE_CAST(humidity AS FLOAT64) as humidity,
        SAFE_CAST(temperature AS FLOAT64) as temperature,
        SAFE_CAST(precipitation_rate AS FLOAT64) as precipitation_rate,
        SAFE_CAST(precipitation_total AS FLOAT64) as precipitation_total,
        SAFE_CAST(pressure_trend AS FLOAT64) as pressure_tend,
        SAFE_CAST(pressure AS FLOAT64) as pressure,
        SAFE_CAST(wind_speed AS FLOAT64) as wind_speed,
        SAFE_CAST(gust AS FLOAT64) as gust,
        SAFE_CAST(dew_point AS FLOAT64) as dew_point,
        SAFE_CAST(heat_index AS FLOAT64) as heat_index,
        SAFE_CAST(wind_direction AS INT) as wind_direction_degree,
        SAFE_CAST(wind_chill AS FLOAT64) as wind_chill,
        SAFE_CAST(SAFE_CAST(uv AS float64) AS INT) as uv_index,
    FROM
    {% if var('test_run', default=true) %}
        {{source('dev', 'hourly_pws_weather')}}
        LIMIT 1000
    {% else %}
        {{source('prod', 'hourly_pws_weather')}}
    {% endif %}
)

SELECT 
    datetime,
    weather_station,

    CASE
        WHEN solar_radiation < 0 THEN NULL
        ELSE solar_radiation
    END as solar_radiation,

    CASE
        WHEN humidity > 100 THEN NULL
        WHEN humidity < 0 THEN NULL
        ELSE humidity
    END as humidity,

    CASE
        WHEN dew_point <= 0 THEN NULL
        WHEN dew_point >= 50 THEN NULL
        ELSE dew_point
    END as dew_point,

    CASE
        WHEN temperature <= 0 THEN NULL
        WHEN temperature >= 50 THEN NULL
        ELSE temperature
    END as temperature,

    CASE
        WHEN pressure_tend < 0 THEN NULL
        ELSE pressure_tend
    END as pressure_tend,

    CASE
        WHEN pressure < 500 THEN NULL
        WHEN pressure > 1500 THEN NULL
        ELSE pressure
    END as pressure,

    CASE
        WHEN wind_speed < 0 THEN NULL
        ELSE wind_speed
    END as wind_speed,

    CASE 
        WHEN gust < 0 THEN NULL
        ELSE gust
    END as gust,

    CASE
        WHEN heat_index <= 0 THEN NULL
        WHEN heat_index >=50 THEN NULL
        ELSE heat_index
    END as heat_index,

    CASE
        WHEN wind_direction_degree < 0 THEN NULL
        WHEN wind_direction_degree >= 360 THEN NULL
        ELSE wind_direction_degree
    END as wind_direction_degree,

    CASE 
        WHEN wind_chill <= 0 THEN NULL
        WHEN wind_chill >=50 THEN NULL
        ELSE wind_chill
    END as wind_chill,

    CASE 
        WHEN uv_index < 0 THEN NULL
        ELSE uv_index
    END as uv_index,

    CASE
        WHEN precipitation_rate < 0 THEN NULL
        ELSE precipitation_rate
    END as precipitation_rate,

    CASE 
        WHEN precipitation_total < 0 THEN NULL
        ELSE precipitation_total
    END as precipitation_total,

FROM formatted_hourly_weather
WHERE 
    datetime IS NOT NULL AND
    weather_station != 'Unidentified'