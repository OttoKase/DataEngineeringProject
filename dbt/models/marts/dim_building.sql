{{
  config(
    materialized='view'
  )
}}

SELECT
	BuildingKey,
	timestamp,
	name,
    out,
    in
FROM {{ ref('stg_dim_building') }}
-- FROM {{ source('peopletraffic', 'bronze_infrared') }}













