{{ config(materialized='table') }}


SELECT
    "ID" as id,
    "pname" as project_name,
    "LEAD" as  project_lead,
    "pkey" as project_key,
    "pcounter" as project_counter,
    "ASSIGNEETYPE" as assign_type,
    "ORIGINALKEY" as original_key,
    "PROJECTTYPE" as project_type,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_project') }}
