DROP DATABASE IF EXISTS peopletraffic;
CREATE DATABASE peopletraffic;


CREATE TABLE IF NOT EXISTS peopletraffic.raw_Building (
 timestamp DateTime64(3),
 name String,
    out   UInt8,
    in    UInt8,
    loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp);


CREATE TABLE IF NOT EXISTS peopletraffic.raw_Weather (
 time DateTime64(3),
 temp Float32,
 prcp    Float32,
 loaded_at DateTime DEFAULT now()
) ENGINE = MergeTree
PARTITION BY toYYYYMM(time)
ORDER BY (time)




