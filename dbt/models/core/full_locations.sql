{{config(materialized='table')}}

select
    identifying_location,
    sl.place as place,
    cp.city as city,
    state,
    latitude,
    longitude,
    ICAO,
    PWStation
from
    {{ref('state_locations')}} sl
left join {{ref('city_places')}} cp ON sl.place = cp.place
left join {{ref('city_states')}} st ON cp.city = st.city