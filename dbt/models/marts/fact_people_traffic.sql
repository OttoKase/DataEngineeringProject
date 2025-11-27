{{ config(materialized='incremental') }}

WITH f AS (
    SELECT
        row_number() OVER () AS building_key,
        weather_key,
        datekey,
        timekey,
        PeopleTrafficAmount,
        FullDate
    FROM {{ ref('stg_dim_building') }}
)

SELECT
    row_number() OVER () AS peoplefact_key,

    -- Building
    b.building_key,
    b.name AS building_name,
    b.out AS people_out,
    b.in AS people_in,

    -- Weather
    w.weather_key,
    w.temp,
    w.prcr,

    -- Measures
    SUM(f.PeopleTrafficAmount) AS total_people,
    MAX(f.FullDate) AS last_date

FROM f
LEFT JOIN {{ ref('dim_building') }} AS b
    ON f.building_key = b.building_key

LEFT JOIN {{ ref('dim_weather') }} AS w
    ON f.weather_key = w.weather_key

LEFT JOIN {{ ref('dim_date') }} AS d
    ON f.datekey = d.datekey

LEFT JOIN {{ ref('dim_time') }} AS t
    ON f.timekey = t.timekey

GROUP BY
    b.building_key,
    b.name,
    b.out,
    b.in,
    w.weather_key,
    w.temp,
    w.prcr


-- {{ config(materialized='incremental') }}
--
--
-- SELECT
--     row_number() OVER () AS peoplefact_key,
--     b.building_key,
--     b.name AS building_name,
--     b.out AS people_out,
--     b.in AS people_in,
--     w.weather_key,
--     w.temp,
--     w.prcr,
--     SUM(f.PeopleTrafficAmount) AS TotalPeople,
--     MAX(f.FullDate) AS LastDate
-- FROM {{ ref('stg_dim_building') }} AS f
-- LEFT JOIN {{ ref('dim_building') }} AS b
--     ON f.building_key = b.building_key
-- LEFT JOIN {{ ref('dim_weather') }} AS w
--     ON f.weather_key = w.weather_key
-- LEFT JOIN {{ ref('dim_date') }} AS d
--     ON f.DateKey = d.DateKey
-- LEFT JOIN {{ ref('dim_time') }} AS t
--     ON f.TimeKey = t.TimeKey


-- GROUP BY
--     b.BuildingKey,
--     b.name --,
--     -- c.LastName,
--     -- c.City,
--     -- p.ProductKey,
--     -- p.ProductName,
--     -- s.StoreKey,
--     -- s.StoreName



