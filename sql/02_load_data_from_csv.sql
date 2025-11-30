-- ========== Dimensions ==========

INSERT INTO peopletraffic.raw_Building
(
timestamp,
	name,
    out,
    in
)

SELECT
    toDateTime(timestamp),
    toString(name),
    toUInt8(out),
    toUInt8(in)
FROM file('bronze_infrared.csv', 'CSVWithNames');


INSERT INTO peopletraffic.raw_Weather
(
time,
temp,
prcp
)
SELECT
	toDateTime(time),
	toFloat32(temp),
	toFloat32(prcp)
FROM file('bronze_weather.csv', 'CSVWithNames');


