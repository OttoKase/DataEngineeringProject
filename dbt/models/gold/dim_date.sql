{{ config(
    materialized='incremental'
) }}



SELECT
    toUInt32(toYYYYMMDD(timestamp)) AS date_key,
    toDate(timestamp) AS full_date,
    CASE toDayOfWeek(timestamp)
        WHEN 1 THEN 'Monday'
        WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday'
        WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'
        WHEN 6 THEN 'Saturday'
        WHEN 7 THEN 'Sunday'
    END AS day_of_week,
    toDayOfWeek(timestamp) AS day_of_week_num,
    (
        toDayOfWeek(timestamp) IN (6, 7)
        OR formatDateTime(timestamp, '%m-%d') IN (
            '01-01', '06-19', '07-04', '09-11', '10-31', '11-11', '12-25', '12-31'
        )
        OR (toMonth(timestamp) = 1 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 15 AND 21)
        OR (toMonth(timestamp) = 2 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 15 AND 21)
        OR (toMonth(timestamp) = 5 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 25 AND 31)
        OR (toMonth(timestamp) = 9 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 1 AND 7)
        OR (toMonth(timestamp) = 10 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 8 AND 14)
        OR (toMonth(timestamp) = 11 AND toDayOfWeek(timestamp) = 2 AND toDayOfMonth(timestamp) BETWEEN 1 AND 7)
        OR (toMonth(timestamp) = 11 AND toDayOfWeek(timestamp) = 4 AND toDayOfMonth(timestamp) BETWEEN 22 AND 28)
    ) AS is_holiday,
    CASE
        WHEN toMonth(timestamp) IN (12, 1, 2)started_at THEN 'Winter'
        WHEN toMonth(timestamp) IN (3, 4, 5) THEN 'Spring'
        WHEN toMonth(timestamp) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END AS season,
    toMonth(timestamp) AS month,
    toYear(timestamp) AS year
FROM {{ ref('stg_dim_building') }}
GROUP BY
    toUInt32(toYYYYMMDD(timestamp)),
    toDate(timestamp),
    toDayOfWeek(timestamp),
    toMonth(timestamp),
    toYear(timestamp),
    (
        toDayOfWeek(timestamp) IN (6, 7)
        OR formatDateTime(timestamp, '%m-%d') IN (
            '01-01', '06-19', '07-04', '09-11', '10-31', '11-11', '12-25', '12-31'
        )
        OR (toMonth(timestamp) = 1 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 15 AND 21)
        OR (toMonth(timestamp) = 2 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 15 AND 21)
        OR (toMonth(timestamp) = 5 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 25 AND 31)
        OR (toMonth(timestamp) = 9 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 1 AND 7)
        OR (toMonth(timestamp) = 10 AND toDayOfWeek(timestamp) = 1 AND toDayOfMonth(timestamp) BETWEEN 8 AND 14)
        OR (toMonth(timestamp) = 11 AND toDayOfWeek(timestamp) = 2 AND toDayOfMonth(timestamp) BETWEEN 1 AND 7)
        OR (toMonth(timestamp) = 11 AND toDayOfWeek(timestamp) = 4 AND toDayOfMonth(timestamp) BETWEEN 22 AND 28)
    ),
    CASE
        WHEN toMonth(timestamp) IN (12, 1, 2) THEN 'Winter'
        WHEN toMonth(timestamp) IN (3, 4, 5) THEN 'Spring'
        WHEN toMonth(timestamp) IN (6, 7, 8) THEN 'Summer'
        ELSE 'Fall'
    END








