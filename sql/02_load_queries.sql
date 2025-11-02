-- ========== Dimensions ==========


INSERT INTO peopletraffic_db.DimBuilding
SELECT
	timestamp AS timestamp_building,
	name AS building_name
FROM default.bronze_infrared;

INSERT INTO peopletraffic_db.DimWeather
SELECT
	time AS timestamp_weather,
	temp,
	prcp
FROM default.bronze_weather;


INSERT INTO peopletraffic_db.DimDate
SELECT DISTINCT
    toDate(timestamp) AS fullDate,
    toYear(timestamp) AS Year,
    toMonth(timestamp) AS Month,
    toDayOfMonth(timestamp) AS Date,
    toDayOfWeek(timestamp) AS DayOfWeek
FROM default.bronze_infrared;


INSERT INTO peopletraffic_db.DimTime
SELECT DISTINCT
    timestamp AS fullTime,
    toHour(timestamp) AS hour,
    toMinute(timestamp) AS minute,
    toSecond(timestamp) AS second
FROM default.bronze_infrared;

INSERT INTO peopletraffic_db.FactPeopleTraffic (
    building_name,
    timestamp_weather,
    fullDate,
    hour,
    people_in,
    people_out
)
SELECT
    name AS building_name,
    toStartOfHour(timestamp) AS timestamp_weather,
    toDate(toStartOfHour(timestamp)) AS fullDate,
    toHour(timestamp) AS hour,
    sum(`in`) AS people_in,
    sum(`out`) AS people_out
FROM default.bronze_infrared
GROUP BY
    name,
    toStartOfHour(timestamp),
    toHour(timestamp);