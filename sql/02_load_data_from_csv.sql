-- ========== Dimensions ==========

INSERT INTO peopletraffic_db.DimBuilding
SELECT
    toUInt32(BuildingKey),
    toDate(timestamp),
    toString(name),
    toUInt8(out),
    toUInt8(in)
FROM file('bronze_infrared.csv', 'CSVWithNames');


INSERT INTO peopletraffic_db.DimWeather
SELECT
	toUInt32(WeatherKey),
	toDate(timestamp) AS FullDate,
	toFloat32(temp),
	toFloat32(prcp)
FROM file('bronze_weather.csv', 'CSVWithNames');


INSERT INTO peopletraffic_db.DimDate
SELECT DISTINCT
    toUInt32(DateKey),
    toDate(timestamp) AS FullDate,
    toYear(timestamp) AS Year,
    toMonth(timestamp) AS Month,
    toDayOfMonth(timestamp) AS Date,
    toDayOfWeek(timestamp) AS DayOfWeek
FROM file('bronze_infrared.csv', 'CSVWithNames');
--FROM default.bronze_infrared;
    -- Year      UInt16,
    -- Month     UInt8,
    -- Day       UInt8,
    -- DayOfWeek String


INSERT INTO peopletraffic_db.DimTime
SELECT DISTINCT
    toUInt32(TimeKey),
    toTime(timestamp) AS FullTime,
    toHour(timestamp) AS hour,
    toMinute(timestamp) AS minute,
    toSecond(timestamp) AS second
FROM file('bronze_infrared.csv', 'CSVWithNames');
	-- hour	UInt8,
	-- minute	UInt8,
	-- second UInt8


-- FactPeopleTraffic
-- Mobility file is not needed here in the seed. This file upload was excluded in DAGs, i.e. dag_one.py
INSERT INTO peopletraffic_db.FactPeopleTraffic
SELECT
    toUInt64(PeopleTraffic),
    toUInt32(DateKey),
    toUInt32(TimeKey),
    toUInt32(BuildingKey),
    toUInt32(WeatherKey),
    toUInt16(people_in),
    toUInt16(people_out),
    toDecimal32(PeopleTrafficAmount),
    toDate(FullDate)
FROM file('bronze_*.csv', 'CSVWithNames');
-- default.bronze_infrared????



-- Quick verification
-- SELECT 'DimDate' AS table_name, count() AS rows FROM peopletraffic_db.DimDate
-- UNION ALL SELECT 'FactPeopleTraffic', count() FROM peopletraffic_db.FactPeopleTraffic;




