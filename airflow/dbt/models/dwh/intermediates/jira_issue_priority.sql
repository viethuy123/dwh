{{ config(materialized='table') }}


SELECT
    "ID" as priority_id,
    pname as priority_name,
    "DESCRIPTION" as priority_description,
    etl_datetime
FROM {{ source('jira', 'stg_jira_priority') }}