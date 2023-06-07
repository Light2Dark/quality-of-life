{{config(materialized="table")}}

WITH hourly_air_quality AS (
  SELECT
    datetime,
    location,
    REGEXP_REPLACE(value, r'[^0-9]', '') AS pollutant_value,
    REGEXP_REPLACE(value, r'[0-9]', '') AS pollutant_symbol
  
  FROM (
    SELECT DISTINCT datetime, location, value FROM
    {% if var('test_run', default=true) %}
        {{source('dev', 'hourly_air_quality')}}
    {% else %}
        {{source('prod', 'hourly_air_quality')}}
    {% endif %}
  )
  WHERE REGEXP_REPLACE(value, r'[^0-9]', '') IS NOT NULL AND REGEXP_REPLACE(value, r'[^0-9]', '') != ''
)

SELECT
    DATETIME(datetime) as datetime,
    location,
    CAST(pollutant_value as NUMERIC) as pollutant_value,
    pollutant_symbol
FROM hourly_air_quality

{% if var('test_run', default=true) %}
  LIMIT 100
{% endif %}