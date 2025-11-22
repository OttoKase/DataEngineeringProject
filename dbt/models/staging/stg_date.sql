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
FROM {{ source('peopletraffic', 'bronze_infrared') }}








