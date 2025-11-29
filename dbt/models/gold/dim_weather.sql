{{ config(
    materialized='incremental'
) }}



SELECT
    row_number() OVER () AS weather_key,
	time,
	temp,
	prcp
FROM {{ ref('stg_dim_weather') }}








