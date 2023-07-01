{{config(materialized='view')}}

WITH uv_stg AS (
    select 
        datetime,
        location,
        uv_index,
        CASE
            WHEN uv_index >= 0 AND uv_index <= 2 THEN "low"
            WHEN uv_index > 2 AND uv_index <=5 THEN "moderate"
            WHEN uv_index > 5 AND uv_index <= 7 THEN "high"
            WHEN uv_index > 7 AND uv_index <= 10 THEN "very high"
            WHEN uv_index > 10 AND uv_index <= 20 THEN "extreme"
            ELSE null
        END as exposure_category
    from
        {{ref('weather')}}
    {% if var('test_run', default=true) %}
    limit 100
    {% endif %}
)

SELECT 
    datetime,
    location,
    uv_index,
    uv_stg.exposure_category as exposure_category,
    short_description,
    long_description
FROM uv_stg as uv_stg LEFT JOIN {{ref('uv_info')}} as uv_info
ON uv_stg.exposure_category = uv_info.exposure_category