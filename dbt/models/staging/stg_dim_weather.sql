SELECT
	WeatherKey,
	timestamp,
	temp,
	prcp
FROM file('/var/lib/clickhouse/user_files/bronze_weather.csv')







