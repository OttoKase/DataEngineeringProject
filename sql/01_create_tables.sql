DROP DATABASE IF EXISTS peopletraffic_db;

CREATE DATABASE peopletraffic_db;

-- ========== Dimensions ==========


CREATE TABLE peopletraffic_db.DimBuilding (
	timestamp_building	DateTime,
	building_name	String
) ENGINE = MergeTree()
ORDER BY (timestamp_building);



CREATE TABLE peopletraffic_db.DimWeather (
	timestamp_weather	DateTime,
	temp	Float32,
	prcp Float32
) ENGINE = MergeTree ()
ORDER BY (timestamp_weather);


CREATE TABLE peopletraffic_db.DimDate (
	fullDate	Date,
	year	UInt16,
	month	UInt8,
	day	UInt8,
	dayOfWeek	UInt8
) ENGINE = MergeTree ()
ORDER BY (fullDate);

CREATE TABLE peopletraffic_db.DimTime (
    fullTime	DateTime,
	hour	UInt8,
	minute	UInt8,
	second UInt8
) ENGINE = MergeTree ()
ORDER BY (hour);

-- ========== Fact ==========
-- Denormalize FullDate onto fact for partitioning and fast time filtering.
CREATE TABLE peopletraffic_db.FactPeopleTraffic (
	building_name	String,
    timestamp_weather	DateTime,
    fullDate	Date,
    hour	UInt8,
	people_in	UInt16,
	people_out	UInt16
) ENGINE = MergeTree ()
PARTITION BY toYYYYMM(timestamp_weather)
ORDER BY (timestamp_weather);