{{ config(materialized='table') }}


SELECT
    position_id,
    position_name,
    etl_datetime
FROM {{ ref('user_positions') }}