{{ config(materialized='table') }}


SELECT
    "ID" as type_id,
    pname as type_name,
    "DESCRIPTION" as type_description,
    etl_datetime
FROM {{ source('jira', 'stg_jira_issuetype') }}