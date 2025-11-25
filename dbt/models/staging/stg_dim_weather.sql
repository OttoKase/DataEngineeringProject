{{
  config(
    materialized='view'
  )
}}

SELECT
	time,
	temp,
	prcp
FROM {{ source('peopletraffic', 'raw_Weather') }}
-- FROM file('/var/lib/clickhouse/user_files/bronze_weather.csv')







