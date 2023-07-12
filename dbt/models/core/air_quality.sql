{{config(materialized="table")}}

WITH aq AS (
    select 
        datetime,
        location,
        pollutant,
        pollutant_value,
        CASE
            WHEN pollutant_value < 50 THEN 'Good'
            WHEN pollutant_value < 100 THEN 'Moderate'
            WHEN pollutant_value < 150 THEN 'Unhealthy for sensitive groups'
            WHEN pollutant_value < 200 THEN 'Unhealthy'
            WHEN pollutant_value < 300 THEN 'Very unhealthy'
            ELSE 'Hazardous'
        END as label,
    from {{ref('stg_hourly_air_quality')}}
    left join {{ref('air_quality_indicators')}} ON pollutant_symbol = symbol
)
select
    datetime,
    aq.location,
    place,
    city,
    state,
    aq.pollutant as pollutant,
    pollutant_value,
    aq.label as label,
    warning_message as message
FROM aq
LEFT JOIN {{ref('air_quality_warning')}} aqw ON aq.pollutant = aqw.pollutant AND aq.label = aqw.label
LEFT JOIN {{ref('full_locations')}} as fl ON fl.identifying_location = aq.location 