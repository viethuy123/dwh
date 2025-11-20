{{ config(materialized='table') }}


SELECT
    position_id,
    position_name
FROM {{ source('dwh', 'user_positions') }}