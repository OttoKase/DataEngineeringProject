{{
  config(
    materialized='view'
  )
}}

SELECT
	TimeKey,
    FullTime,
	hour,
	minute,
	second
FROM {{ ref('bronze_infrared') }}
-- FROM {{ source('peopletraffic', 'bronze_infrared') }}








