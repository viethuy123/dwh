{{ config(materialized='table') }}


SELECT
    "ID" as issue_id,
    issuenum as issue_number,
    "PROJECT" as jira_project_id,
    issuetype as issue_type,
    "REPORTER" as issue_reporter,
    "ASSIGNEE" as issue_assignee,
    "CREATOR" as issue_creator,
    "SUMMARY" as issue_summary,
    "DESCRIPTION" as issue_description,
    "PRIORITY" as issue_priority,
    "RESOLUTION" as issue_resolution,
    "RESOLUTIONDATE" as resolution_date,
    issuestatus as issue_status,
    "DUEDATE" as due_date,	  
    "TIMEORIGINALESTIMATE" as time_original_estimate,
    "TIMEESTIMATE" as time_estimate,
    "TIMESPENT" as time_spent,
    "ENVIRONMENT" as environment,
    "CREATED" as created_time,
    "UPDATED" as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_jiraissue') }}