{{ config(materialized='table') }}


SELECT
    "ID" as status_id,
    pname as status_name,
    "DESCRIPTION" as status_description,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_issuestatus') }}