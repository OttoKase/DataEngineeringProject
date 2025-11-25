DROP DATABASE IF EXISTS peopletraffic;
CREATE DATABASE peopletraffic;

DROP DATABASE IF EXISTS dataeng;
CREATE DATABASE dataeng;

CREATE TABLE IF NOT EXISTS peopletraffic.raw_Building (
	timestamp	DateTime64(3),
	name	String,
    out   UInt8,
    in    UInt8,
    loaded_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp);


CREATE TABLE IF NOT EXISTS peopletraffic.raw_Weather (
	time	DateTime64(3),
	temp	Float32,
	prcp    Float32,
	loaded_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree(loaded_at)
PARTITION BY toYYYYMM(time)
ORDER BY (time)


/*
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
ORDER BY (FullDate, BuildingKey);*/



















