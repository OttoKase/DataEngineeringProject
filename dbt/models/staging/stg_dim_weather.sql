{{
  config(
    materialized='view'
  )
}}

SELECT
	WeatherKey,
	timestamp,
	temp,
	prcp
FROM {{ ref('bronze_weather') }}
-- FROM {{ source('peopletraffic', 'bronze_weather') }}







