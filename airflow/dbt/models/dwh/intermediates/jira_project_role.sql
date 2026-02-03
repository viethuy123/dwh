{{ config(materialized='table') }}


SELECT
    "ID" as id,
    "NAME" as role_name,
    "DESCRIPTION" as description,
    etl_datetime
FROM {{ source('jira', 'stg_jira_projectrole') }}
