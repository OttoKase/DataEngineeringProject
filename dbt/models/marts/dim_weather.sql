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
FROM {{ ref('stg_dim_weather') }}
-- FROM {{ source('peopletraffic', 'bronze_weather') }}







