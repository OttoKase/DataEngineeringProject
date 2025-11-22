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
FROM {{ source('peopletraffic', 'bronze_infrared') }}













