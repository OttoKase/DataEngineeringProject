{{ config(materialized='incremental') }}

WITH building_hourly AS (
    SELECT
        building_key,
        toStartOfHour(timestamp) AS timestamp_hour,
        any(building_name) AS building_name,
        SUM(people_out) AS people_out,
        SUM(people_in) AS people_in
    FROM {{ ref('dim_building') }}
    {% if is_incremental() %}
        WHERE timestamp > (SELECT max(timestamp_hour) FROM {{ this }})
    {% endif %}
    GROUP BY
        building_key,
        timestamp_hour
),

weather_ordered AS (
    SELECT
        row_number() OVER (ORDER BY time) - 1 AS weather_key,  -- start from 0
        time AS weather_time,
        temp,
        prcp
    FROM {{ ref('dim_weather') }}
),

joined AS (
    SELECT
        b.building_key,
        b.building_name,
        b.people_out,
        b.people_in,
        w.weather_key,
        w.temp,
        w.prcp,
        b.people_out + b.people_in AS TotalPeople,
        b.timestamp_hour AS join_timestamp
    FROM building_hourly AS b
    LEFT JOIN weather_ordered AS w
        ON b.timestamp_hour = w.weather_time
)

SELECT
    row_number() OVER (ORDER BY building_key, weather_key) - 1 AS peoplefact_key,
    building_key,
    weather_key,
    join_timestamp,
    building_name,
    people_out,
    people_in,
    temp,
    prcp,
    TotalPeople
FROM joined
