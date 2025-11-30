-- create two users
CREATE USER IF NOT EXISTS analyst_full IDENTIFIED BY 'analystHasD1fficul7Pa55sowrd';
CREATE USER IF NOT EXISTS analyst_limited IDENTIFIED BY 'password';

-- create two roles, one with full access and the other one with limited access
CREATE ROLE IF NOT EXISTS full;
CREATE ROLE IF NOT EXISTS limited;

-- give roles to users
GRANT full TO analyst_full;
GRANT limited TO analyst_limited;

-- grant user access on the tables
GRANT SELECT ON default.* TO full;
GRANT SELECT(people_in, people_out, temp, prcp, building_key, join_timestamp) ON default.fact_people_traffic to limited;
GRANT SELECT(building_name, building_key) ON default.dim_building TO limited;
GRANT SELECT(full_date, is_holiday, season, date_key, day_of_week) ON default.dim_date TO limited;