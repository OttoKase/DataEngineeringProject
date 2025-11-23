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
FROM {{ ref('stg_dim_time') }}









