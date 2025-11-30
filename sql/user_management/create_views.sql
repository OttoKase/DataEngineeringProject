-- create view which looks at every buildings daily count of people coming in and out.
CREATE OR REPLACE VIEW v_daily_traffic_full 
AS SELECT
    b.building_name AS `Building name`,
    d.full_date AS `Full date`,
    d.day_of_week AS `Day of week`,
    d.is_holiday AS `Is holiday?`,
    d.season,
    sum(f.people_in) AS `Total people in`,
    sum(f.people_out) AS `Total people out`,
    avg(f.temp) AS `Average temp.`,
    avg(f.prcp) AS `Average prcp.`
FROM default.fact_people_traffic AS f
INNER JOIN default.dim_building AS b ON f.building_key = b.building_key
INNER JOIN default.dim_date AS d ON d.date_key = toUInt32(formatDateTime(join_timestamp, '%Y%m%d'))
GROUP BY
    b.building_name,
    d.full_date,
    d.day_of_week,
    d.is_holiday,
    d.season;

-- create limited view which looks at every buildings daily count of people coming in and out. Most of the columns have been masked to hide information
CREATE OR REPLACE VIEW v_daily_traffic_limited
AS SELECT
    replaceRegexpOne(b.building_name, '^(.{18}).*$', '\\1***') AS `Building name`,
    toStartOfWeek(d.full_date) AS `Week`,
    'Classified' AS `Day of week`,
    d.is_holiday AS `Is holiday?`,
    d.season,
    concat(
        toString(intDiv(toUInt64(sum(f.people_in)), 1000) * 1000),
        '–',
        toString(intDiv(toUInt64(sum(f.people_in)), 1000) * 1000 + 999)
    ) AS `Total people in`,
    concat(
        toString(intDiv(toUInt64(sum(f.people_out)), 1000) * 1000),
        '–',
        toString(intDiv(toUInt64(sum(f.people_out)), 1000) * 1000 + 999)
    ) AS `Total people out`,
    round(avg(f.temp)) AS `Average temp.`,
    round(avg(f.prcp)) AS `Average prcp.`
FROM default.fact_people_traffic AS f
INNER JOIN default.dim_building AS b ON f.building_key = b.building_key
INNER JOIN default.dim_date AS d ON d.date_key = toUInt32(formatDateTime(join_timestamp, '%Y%m%d'))
GROUP BY
    b.building_name,
    d.full_date,
    d.day_of_week,
    d.is_holiday,
    d.season;

--- create view which looks at every buildings summarized count of people coming in and out.
CREATE VIEW v_summarized_traffic_full
AS SELECT
    b.building_name AS `Building name`,
    count(*) AS `Number of rows`,
    sum(f.people_in) AS `Total people in`,
    sum(f.people_out) AS `Total people out`,
    avg(f.temp) AS `Average temp.`,
    avg(f.prcp) AS `Average prcp.`
FROM default.fact_people_traffic AS f
INNER JOIN default.dim_building AS b ON f.building_key = b.building_key
INNER JOIN default.dim_date AS d ON d.date_key = toUInt32(formatDateTime(join_timestamp, '%Y%m%d'))
GROUP BY b.building_name
ORDER BY count(*) DESC

--- create limited view which looks at every buildings summarized count of people coming in and out. Most of the columns have been masked to hide information
CREATE VIEW v_summarized_traffic_limited
AS SELECT
    concat('Building ', ROW_NUMBER() OVER (ORDER BY b.building_name)) `Building name`,
    COUNT(*) AS `Number of rows`,
    concat(
        toString(intDiv(toUInt64(sum(f.people_in)), 1000) * 1000),
        '–',
        toString(intDiv(toUInt64(sum(f.people_in)), 1000) * 1000 + 999)
    ) AS `Total people in`,
    concat(
        toString(intDiv(toUInt64(sum(f.people_out)), 1000) * 1000),
        '–',
        toString(intDiv(toUInt64(sum(f.people_out)), 1000) * 1000 + 999)
    ) AS `Total people out`,
    round(avg(f.temp)) AS `Average temp.`,
    round(avg(f.prcp), 1) AS `Average prcp.`
FROM default.fact_people_traffic AS f
INNER JOIN default.dim_building AS b ON f.building_key = b.building_key
INNER JOIN default.dim_date AS d ON d.date_key = toUInt32(formatDateTime(join_timestamp, '%Y%m%d'))
GROUP BY
    b.building_name
ORDER BY
    count(*) desc

-- grant user access on the views
GRANT SELECT ON v_daily_traffic_limited TO analyst_limited;
GRANT SELECT ON v_summarized_traffic_limited TO analyst_limited;