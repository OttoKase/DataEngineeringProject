{{ config(materialized='incremental') }}


SELECT
    -- b.BuildingKey,
    b.name,
    b.out,
    b.in,
    -- w.WeatherKey,
    w.temp,
    w.prcr,
    SUM(f.PeopleTrafficAmount) AS TotalPeople,
    MAX(f.FullDate) AS LastDate
FROM {{ ref('stg_dim_weather', 'stg_dim_building') }} AS f
LEFT JOIN {{ ref('dim_building') }} AS b
    ON f.BuildingKey = b.BuildingKey
LEFT JOIN {{ ref('dim_weather') }} AS w
    ON f.WeatherKey = w.WeatherKey
LEFT JOIN {{ ref('dim_date') }} AS d
    ON f.DateKey = d.DateKey
LEFT JOIN {{ ref('dim_time') }} AS t
    ON f.TimeKey = t.TimeKey


GROUP BY
    -- b.BuildingKey,
    b.name --,
    -- c.LastName,
    -- c.City,
    -- p.ProductKey,
    -- p.ProductName,
    -- s.StoreKey,
    -- s.StoreName



