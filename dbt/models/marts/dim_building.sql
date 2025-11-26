{{ config(materialized='incremental') }}


SELECT
    row_number() OVER () AS building_key,
	timestamp,
	name AS building_name,
    out AS people_out,
    in AS people_in
    -- toDate('2020-01-01') AS valid_from,
    -- toDate('2099-12-31') AS valid_to
FROM {{ ref('stg_dim_building') }}














