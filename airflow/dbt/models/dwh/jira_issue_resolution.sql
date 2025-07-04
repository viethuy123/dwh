{{ config(materialized='table') }}


SELECT
    "ID" as resolution_id,
    pname as resolution_name,
    "DESCRIPTION" as resolution_description,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_resolution') }}