{{ config(materialized='table') }}


SELECT
    "ID" as priority_id,
    pname as priority_name,
    "DESCRIPTION" as priority_description,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_priority') }}