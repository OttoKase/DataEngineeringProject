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
FROM {{ ref('bronze_infrared') }}
-- FROM {{ source('peopletraffic', 'bronze_infrared') }}













