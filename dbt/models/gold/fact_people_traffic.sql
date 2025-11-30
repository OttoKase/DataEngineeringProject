{{ config(
    materialized='incremental'
) }}

<<<<<<< HEAD:dbt/models/marts/fact_people_traffic.sql
-- 1️⃣ BUILDING DATA ROLLED UP TO HOURLY GRAIN
WITH building_hourly AS (
    SELECT
        any(building_key) AS building_key,      -- pick a representative key
=======
-- 1️⃣ BUILDING DATA ROLLED UP TO HOURLY GRAIN (summarized by building_name)
WITH building_hourly AS (
    SELECT
        building_key,
>>>>>>> 0639af7ea235b3551331be25f92f19fa1f76616c:dbt/models/gold/fact_people_traffic.sql
        building_name,
        toStartOfHour(timestamp) AS timestamp_hour,
        SUM(people_out) AS people_out,
        SUM(people_in) AS people_in
    FROM {{ ref('dim_building') }}
    {% if is_incremental() %}
        WHERE timestamp > (SELECT max(join_timestamp) FROM {{ this }})
    {% endif %}
    GROUP BY
        building_name,
<<<<<<< HEAD:dbt/models/marts/fact_people_traffic.sql
=======
        building_key,
>>>>>>> 0639af7ea235b3551331be25f92f19fa1f76616c:dbt/models/gold/fact_people_traffic.sql
        timestamp_hour
),

-- 2️⃣ WEATHER ORDERED + HOURLY
weather_ordered AS (
    SELECT
<<<<<<< HEAD:dbt/models/marts/fact_people_traffic.sql
        row_number() OVER (ORDER BY time ASC) - 1 AS weather_key,
        time AS weather_time,
        temp,
        prcp
=======
        row_number() OVER (ORDER BY toStartOfHour(time) ASC) - 1 AS weather_key,
        toStartOfHour(time) AS weather_time,
        round(AVG(temp), 2) AS temp,
        round(SUM(prcp), 2) AS prcp
>>>>>>> 0639af7ea235b3551331be25f92f19fa1f76616c:dbt/models/gold/fact_people_traffic.sql
    FROM {{ ref('dim_weather') }}
    GROUP BY toStartOfHour(time)
),

-- 3️⃣ JOIN
joined AS (
    SELECT
        b.building_key,
        b.building_name,
        b.people_out,
        b.people_in,
        w.weather_key,
        w.temp,
        w.prcp,
<<<<<<< HEAD:dbt/models/marts/fact_people_traffic.sql
        (b.people_in) AS total_peopleIN,
=======
>>>>>>> 0639af7ea235b3551331be25f92f19fa1f76616c:dbt/models/gold/fact_people_traffic.sql
        b.timestamp_hour AS join_timestamp
    FROM building_hourly b
    LEFT JOIN weather_ordered w
        ON b.timestamp_hour = w.weather_time
)

-- 4️⃣ FINAL OUTPUT
SELECT
    row_number() OVER (ORDER BY join_timestamp, building_name) - 1 AS peoplefact_key,
    building_key,
    join_timestamp,
    building_name,
    people_out,
    people_in,
    temp,
    prcp,
<<<<<<< HEAD:dbt/models/marts/fact_people_traffic.sql
    total_peopleIN
=======
    toHour(join_timestamp) AS hour  -- ✅ hour as integer
>>>>>>> 0639af7ea235b3551331be25f92f19fa1f76616c:dbt/models/gold/fact_people_traffic.sql
FROM joined
ORDER BY join_timestamp, building_name
