{{
  config(
    materialized='view'  )
}}



SELECT
    name,
    timestamp,
    in,
    out
FROM {{ source('peopletraffic', 'raw_Building') }}



/*
SELECT
	timestamp,
	name,
    out,
    in
FROM {{ source('peopletraffic', 'raw_Building') }}
-- FROM file('/var/lib/clickhouse/user_files/bronze_infrared.csv')




*/









