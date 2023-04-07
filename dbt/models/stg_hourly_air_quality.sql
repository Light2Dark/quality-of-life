WITH hourly_air_quality AS (
  SELECT
    timestamp,
    city,
    REGEXP_REPLACE(value, r'[^0-9]', '') AS pollutant_value,
    REGEXP_REPLACE(value, r'[0-9]', '') AS pollutant_symbol
  FROM (
    SELECT DISTINCT timestamp, city, value
    FROM {{source('dev', 'hourly_air_quality')}}
  )
)
SELECT *
FROM hourly_air_quality
WHERE pollutant_value IS NOT NULL AND pollutant_value != ''