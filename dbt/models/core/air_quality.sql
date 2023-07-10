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
    {% if var('test_run', default=true) %}
    limit 100
    {% endif %}
)
select
    datetime,
    location,
    place,
    city,
    state,
    pollutant,
    pollutant_value,
    label,
    CASE
        WHEN (pollutant = 'PM2.5' or pollutant = 'PM10') AND label = 'Moderate' THEN 'Unusually sensitive people should consider reducing prolonged or heavy exertion'
        WHEN (pollutant = 'PM2.5' or pollutant = 'PM10') AND label = 'Unhealthy for sensitive groups' THEN 'People with heart or lung disease, older adults, and children should reduce prolonged or heavy exertion'
        WHEN (pollutant = 'PM2.5' or pollutant = 'PM10') AND label = 'Unhealthy' THEN 'People with heart or lung disease, older adults, and children should avoid prolonged or heavy exertion; everyone else should reduce prolonged or heavy exertion'
        WHEN (pollutant = 'PM2.5' or pollutant = 'PM10') AND label = 'Very unhealthy' THEN 'People with heart or lung disease, older adults, and children should avoid all physical activity outdoors. Everyone else should avoid prolonged or heavy exertion'
        WHEN (pollutant = 'PM2.5' or pollutant = 'PM10') AND label = 'Hazardous' THEN 'Everyone should avoid all physical activity outdoors; people with heart or lung disease, older adults, and children should remain indoors and keep activity level slow'

        WHEN pollutant = 'O3' AND label = 'Moderate' THEN 'Unusually sensitive people should consider reducing prolonged or heavy outdoor exertion'
        WHEN pollutant = 'O3' AND label = 'Unhealthy for sensitive groups' THEN 'People with lung disease, such as asthma, children, older adults, and outdoor workers should reduce prolonged or heavy outdoor exertion'
        WHEN pollutant = 'O3' AND label = 'Unhealthy' THEN 'People with lung disease, such as asthma, children, older adults, and outdoor workers should avoid prolonged or heavy outdoor exertion; everyone else should reduce prolonged or heavy outdoor exertion'
        WHEN pollutant = 'O3' AND label = 'Very unhealthy' THEN 'People with lung disease, such as asthma, children, older adults, and outdoor workers should avoid all outdoor exertion; everyone else should reduce outdoor exertion'
        WHEN pollutant = 'O3' AND label = 'Hazardous' THEN 'Everyone should avoid all outdoor exertion'

        WHEN pollutant = 'CO2' AND label = 'Unhealthy for sensitive groups' THEN 'People with heart disease, such as angina, should limit heavy exertion and avoid sources of CO2 such as heavy traffic'
        WHEN pollutant = 'CO2' AND label = 'Unhealthy' THEN 'People with heart disease, such as angina, should limit moderate exertion and avoid sources of CO2 such as heavy traffic'
        WHEN pollutant = 'CO2' AND label = 'Very unhealthy' THEN 'People with heart disease, such as angina, should avoid exertion and sources of CO2 such as heavy traffic'
        WHEN pollutant = 'CO2' AND label = 'Hazardous' THEN 'People with heart disease, such as angina, should avoid exertion and sources of CO, such as heavy traffic; everyone else should limit heavy exertion'

        WHEN pollutant = 'SO2' AND label = 'Unhealthy for sensitive groups' THEN 'People with asthma should consider limiting outdoor exertion'
        WHEN pollutant = 'SO2' AND label = 'Unhealthy' THEN 'Children, people with asthma, or other lung diseases, should limit outdoor exertion'
        WHEN pollutant = 'SO2' AND label = 'Very unhealthy' THEN 'Children, people with asthma, or other lung diseases should avoid outdoor exertion; everyone else should reduce outdoor exertion'
        WHEN pollutant = 'SO2' AND label = 'Hazardous' THEN 'Children, people with asthma, or other lung diseases, should remain indoors; everyone else should avoid outdoor exertion'

        WHEN pollutant = 'NO2' AND label = 'Moderate' THEN 'Unusually sensitive individuals should consider limiting prolonged exertion especially near busy roads'
        WHEN pollutant = 'NO2' AND label = 'Unhealthy for sensitive groups' THEN 'People with asthma, children and older adults should limit prolonged exertion especially near busy roads'
        WHEN pollutant = 'NO2' AND label = 'Unhealthy' THEN 'People with asthma, children and older adults should avoid prolonged exertion near roadways; everyone else should limit prolonged exertion especially near busy roads'
        WHEN pollutant = 'NO2' AND label = 'Very unhealthy' THEN 'People with asthma, children and older adults should avoid all outdoor exertion; everyone else should avoid prolonged exertion especially near busy roads'
        WHEN pollutant = 'NO2' AND label = 'Hazardous' THEN 'People with asthma, children and older adults should remain indoors; everyone else should avoid all outdoor exertion'

        WHEN pollutant IS NULL AND label = 'Moderate' THEN 'Unusually sensitive individuals should consider limiting prolonged exertion outdoors'
        WHEN pollutant IS NULL AND label = 'Unhealthy for sensitive groups' THEN 'People with asthma, children and older adults should limit prolonged exertion outdoors'
        WHEN pollutant IS NULL AND label = 'Unhealthy' THEN 'People with asthma, children and older adults should avoid prolonged exertion outdoors'
        WHEN pollutant IS NULL AND label = 'Very unhealthy' THEN 'People with heart or lung disease, older adults, and children should avoid all physical activity outdoors. Everyone else should avoid prolonged or heavy exertion'
        WHEN pollutant IS NULL AND label = 'Hazardous' THEN 'Everyone should avoid all outdoor exertion'

        ELSE 'No health effects reported'
    END as message
FROM aq
LEFT JOIN {{ref('full_locations')}} as fl ON fl.identifying_location = aq.location 