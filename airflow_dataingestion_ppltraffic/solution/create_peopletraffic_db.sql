-- CREATE DATABASE IF NOT EXISTS peopletraffic_db;

CREATE TABLE IF NOT EXISTS DimBuilding (
BuildingKey SERIAL PRIMARY KEY,
timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
building_name VARCHAR(100),
people_out INT,
people_in INT,
-- ValidFrom DATE NOT NULL,
-- ValidTo DATE NOT NULL
);

CREATE TABLE IF NOT EXISTS DimWeather (
    WeatherKey SERIAL PRIMARY KEY,
	timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    temp FLOAT,
    prcp FLOAT
);

CREATE TABLE IF NOT EXISTS DimDate (
    DateKey SERIAL PRIMARY KEY,
    FullDate DATE NOT NULL,
    Year INT,
    Month INT,
    Day INT,
    DayOfWeek VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS DimTime (
    TimeKey SERIAL PRIMARY KEY,
    FullTime DATE NOT NULL,
    Hour INT,
    Minute INT
);


CREATE TABLE IF NOT EXISTS FactPeopleTraffic (
    PeopleTrafficID SERIAL PRIMARY KEY,
    BuildingKey INT REFERENCES DimBuilding(BuildingKey),
    WeatherKey INT REFERENCES DimWeather(WeatherKey),
    DateKey INT REFERENCES DimDate(DateKey),
    TimeKey INT REFERENCES DimTime(TimeKey),
    -- Quantity INT,
    PeopleTrafficAmount NUMERIC(10,2)
);

