{{ config(materialized='table') }}


SELECT
    "ID" as id,
    "NAME" as role_name,
    "DESCRIPTION" as description,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_projectrole') }}
