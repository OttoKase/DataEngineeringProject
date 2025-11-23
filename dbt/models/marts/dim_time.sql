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
-- FROM {{ source('peopletraffic', 'bronze_infrared') }}








