DROP DATABASE IF EXISTS peopletraffic_db;

CREATE DATABASE peopletraffic_db;

-- ========== Dimensions ==========

CREATE TABLE peopletraffic_db.DimBuilding (
	BuildingKey   UInt32,
	timestamp	DateTime(),
	name	String,
    out    UInt8,
    in    UInt8
) ENGINE = MergeTree()
ORDER BY (BuildingKey);


CREATE TABLE peopletraffic_db.DimWeather (
	WeatherKey   UInt32,
	timestamp	DateTime(),
	temp	Float32,
	prcp Float32
) ENGINE = MergeTree()
ORDER BY (WeatherKey);


CREATE TABLE peopletraffic_db.DimDate (
    DateKey   UInt32,
    FullDate  DateTime(),
    Year      UInt16,
    Month     UInt8,
    Day       UInt8,
    DayOfWeek String
) ENGINE = MergeTree()
ORDER BY (DateKey);


CREATE TABLE peopletraffic_db.DimTime (
	TimeKey   UInt32,
    FullTime	Time,
	hour	UInt8,
	minute	UInt8,
	second UInt8
) ENGINE = MergeTree()
ORDER BY (TimeKey);


-- ========== Fact ==========
-- Denormalize FullDate onto fact for partitioning and fast time filtering.
CREATE TABLE peopletraffic_db.FactPeopleTraffic (
	PeopleTraffic	UInt64,
	DateKey        UInt32,
	TimeKey        UInt32,
	BuildingKey    UInt32,
	WeatherKey 	   UInt32,
	people_in	UInt16,
	people_out	UInt16
	PeopleTrafficAmount Decimal(10,2),
	FullDate DateTime()
) ENGINE = MergeTree ()
PARTITION BY toYYYYMM(FullDate)
ORDER BY (FullDate, BuildingKey);



















