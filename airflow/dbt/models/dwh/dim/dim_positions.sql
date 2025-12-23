{{ config(materialized='table') }}


SELECT
    position_id,
    position_name
FROM {{ ref('user_positions') }}