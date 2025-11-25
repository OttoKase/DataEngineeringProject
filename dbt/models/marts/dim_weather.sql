{{ config(materialized='incremental') }}


SELECT
	time,
	temp,
	prcp
FROM {{ ref('stg_dim_weather') }}








