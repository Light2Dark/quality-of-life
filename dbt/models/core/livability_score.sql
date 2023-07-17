{{config(materialized='view')}}

SELECT 
  extract(date from datetime) as day,
  city,
  state,
  avg(pollutant_value) as pollutant_value,
  avg(temperature) as temperature,
  max(uv_index) as uv_index
FROM {{ref('full_weather_places')}}
group by day,city, state