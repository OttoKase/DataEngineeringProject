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
FROM {{ source('peopletraffic', 'bronze_infrared') }}








