{{ config(materialized='table', tags=['dim']) }}


SELECT
    position_id,
    position_name,
    etl_datetime
FROM {{ ref('user_positions') }}