{{
  config(
    materialized='view'
  )
}}



-- {{ config(schema='bronze') }}

SELECT
    time,
    temp,
    prcp
FROM {{ source('peopletraffic', 'raw_Weather') }}



/*

SELECT
	time,
	temp,
	prcp
FROM {{ source('peopletraffic', 'raw_Weather') }}
-- FROM file('/var/lib/clickhouse/user_files/bronze_weather.csv')
*/






