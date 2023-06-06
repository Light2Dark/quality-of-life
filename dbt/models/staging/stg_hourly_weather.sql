{{config(materialized="table")}}

WITH formatted_hourly_weather AS 
(
    select DISTINCT
        DATETIME(datetime) as datetime,
        weather_station,
        SAFE_CAST(temperature as FLOAT64) as temperature,
        SAFE_CAST(pressure AS FLOAT64) as pressure,
        SAFE_CAST(wind_speed AS INT) as wind_speed,
        SAFE_CAST(dew_point AS FLOAT64) as dew_point,
        SAFE_CAST(relative_humidity AS INT) as relative_humidity,
        SAFE_CAST(heat_index AS INT) as heat_index,
        SAFE_CAST(feels_like_temperature AS FLOAT64) as feels_like_temperature,
        wind_direction_dir,
        SAFE_CAST(wind_direction_deg AS INT) as wind_direction_degree,
        SAFE_CAST(wind_chill AS FLOAT64) as wind_chill,
        SAFE_CAST(clouds AS STRING) as clouds,
        uv_description,
        SAFE_CAST(uv_index AS INT) as uv_index,
        SAFE_CAST(visibility AS INT) as visibility,
        day_indicator
    FROM
    {% if var('test_run', default=true) %}
        {{source('dev', 'hourly_weather')}}
    {% else %}
        {{source('prod', 'hourly_weather')}}
    {% endif %}

    {% if var('test_run', default=true) %}
    LIMIT 100
    {% endif %}
)

SELECT 
    datetime,
    weather_station,
    visibility,
    day_indicator,

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
        WHEN pressure < 500 THEN NULL
        WHEN pressure > 1500 THEN NULL
        ELSE pressure
    END as pressure,

    CASE
        WHEN wind_speed < 0 THEN NULL
        ELSE wind_speed
    END as wind_speed,

    CASE
        WHEN relative_humidity > 100 THEN NULL
        WHEN relative_humidity < 0 THEN NULL
        ELSE relative_humidity
    END as relative_humidity,

    CASE
        WHEN heat_index <= 0 THEN NULL
        WHEN heat_index >=50 THEN NULL
        ELSE heat_index
    END as heat_index,

    CASE
        WHEN feels_like_temperature <= 0 THEN NULL
        WHEN feels_like_temperature >=50 THEN NULL
        ELSE feels_like_temperature
    END as feels_like_temperature,

    CASE
        WHEN LOWER(wind_direction_dir) = 'none' THEN NULL
        ELSE wind_direction_dir
    END AS wind_direction_dir,

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
        WHEN LOWER(clouds) = 'none' THEN NULL
        ELSE clouds
    END as clouds,

    CASE 
        WHEN uv_index < 0 THEN NULL
        ELSE uv_index
    END as uv_index,

    CASE
        WHEN LOWER(uv_description) = 'none' THEN NULL
        ELSE uv_description
    END as uv_description,

FROM formatted_hourly_weather
WHERE 
    datetime IS NOT NULL