{{
  config(
    materialized='view'
  )
}}

SELECT
	timestamp,
	name,
    out,
    in
FROM {{ source('peopletraffic', 'raw_Building') }}
-- FROM file('/var/lib/clickhouse/user_files/bronze_infrared.csv')














