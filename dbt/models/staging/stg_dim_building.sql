SELECT
	BuildingKey,
	timestamp,
	name,
    out,
    in
FROM file('/var/lib/clickhouse/user_files/bronze_infrared.csv')














