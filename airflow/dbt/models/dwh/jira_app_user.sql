{{ config(materialized='table') }}


SELECT
    "ID" as id,
    "user_key" as user_key,
    "lower_user_name" as lower_user_name,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_app_user') }}
