{{ config(materialized='table') }}


SELECT
    "ID" as resolution_id,
    pname as resolution_name,
    "DESCRIPTION" as resolution_description,
    etl_datetime
FROM {{ source('jira', 'stg_jira_resolution') }}