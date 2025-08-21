{{ config(materialized='table') }}

SELECT
    project_id,
    user_id as member_id,
    joined_at,
    left_at
FROM {{ source('dwh', 'project_members') }}