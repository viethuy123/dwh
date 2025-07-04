{{ config(materialized='table') }}


SELECT
    "ID" as type_id,
    pname as type_name,
    "DESCRIPTION" as type_description,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_issuetype') }}