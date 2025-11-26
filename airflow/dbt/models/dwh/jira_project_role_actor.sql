{{ config(materialized='table') }}


SELECT
    "ID" as id,
    "PID" as project_id,
    "PROJECTROLEID" as project_role_id,
    "ROLETYPEPARAMETER" as user_email,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_projectroleactor') }}
