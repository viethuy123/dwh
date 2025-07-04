{{ config(materialized='table') }}


SELECT
    "ID" as worklog_id,
    issueid as issue_id,
    "AUTHOR" as worklog_author,
    worklogbody as worklog_description,
    "STARTDATE" as start_time,
    timeworked as time_worked,
    "UPDATEAUTHOR"as update_author,
    "CREATED" as created_time,
    "UPDATED" as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_worklog') }}