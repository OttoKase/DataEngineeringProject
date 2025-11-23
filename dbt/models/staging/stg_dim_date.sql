{{
  config(
    materialized='view'
  )
}}

SELECT
	DateKey,
    FullDate,
    Year,
    Month,
    Day,
    DayOfWeek
FROM {{ ref('bronze_infrared') }}
-- FROM {{ source('peopletraffic', 'bronze_infrared') }}








