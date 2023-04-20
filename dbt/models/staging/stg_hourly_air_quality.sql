WITH hourly_air_quality AS (
  SELECT
    timestamp,
    city,
    REGEXP_REPLACE(value, r'[^0-9]', '') AS pollutant_value,
    REGEXP_REPLACE(value, r'[0-9]', '') AS pollutant_symbol
  
  FROM (
    SELECT DISTINCT timestamp, city, value FROM
    {% if var('test_run', default=true) %}
        {{source('dev', 'hourly_air_quality')}}
    {% else %}
        {{source('prod', 'hourly_air_quality')}}
    {% endif %}
  )
)
SELECT *
FROM hourly_air_quality
WHERE pollutant_value IS NOT NULL AND pollutant_value != ''

{% if var('test_run', default=true) %}
  LIMIT 100
{% endif %}