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
FROM {{ source('peopletraffic', 'bronze_weather') }}







