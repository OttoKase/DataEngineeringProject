{{ config(
    materialized='incremental'
) }}


WITH building_keys AS (
    -- Assign one key per building
    SELECT
        row_number() OVER (ORDER BY building_name) AS building_key,
        building_name
    FROM (
        SELECT DISTINCT name AS building_name
        FROM {{ ref('stg_dim_building') }}
    )
)

SELECT
    k.building_key,
    b.timestamp,
    b.name AS building_name,
    b.out AS people_out,
    b.in AS people_in
FROM {{ ref('stg_dim_building') }} b
JOIN building_keys k
    ON b.name = k.building_name













