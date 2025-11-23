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
FROM {{ ref('stg_dim_date') }}








