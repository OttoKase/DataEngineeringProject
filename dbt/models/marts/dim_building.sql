{{ config(materialized='incremental') }}


SELECT
	timestamp,
	name,
    out,
    in
    -- toDate('2020-01-01') AS valid_from,
    -- toDate('2099-12-31') AS valid_to
FROM {{ ref('stg_dim_building') }}
GROUP BY name













