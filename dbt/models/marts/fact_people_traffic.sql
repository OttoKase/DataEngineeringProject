{{
  config(
    materialized='view'
  )
}}

SELECT
 WeatherKey,
 BuildingKey,
 DateKey,
 TimeKey,
 out,
 in
FROM {{ ref('stg_dim_weather', 'stg_dim_building') }}



