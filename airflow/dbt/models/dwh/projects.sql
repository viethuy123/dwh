{{ config(materialized='table') }}


Select 
    a._id as project_id,
    a."jiraProjectId" as jira_project_id,
    a."projectName" as project_name,
    a."projectJiraUrl" as project_jira_url,
    a."projectDescription" as project_description,
    b."LEAD" as project_lead,
    a."projectCode" as project_code,
    a."projectStatus" as project_status,
    a."projectType" as project_type,
    a."isDeleted" as is_deleted,
    c.name_pm as name_pm,
    c.name_br_se as name_brse,
    c.location as location,
    c.scope as scope,
    a.type as type,
    c.point_css as point_css,
    c.point_comment as point_comment,
    c.summary as summary,
    c.size as size,
    c.period as period,
    c.project_rank as project_rank,
    c.team_size as team_size,
    c."startDate" as start_date,
    c."endDate" as end_date,
    c.updated_at as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_projects') }} a
LEFT JOIN {{ source('jira', 'stg_project') }} b
ON a."jiraProjectId" = b."ID"
LEFT JOIN {{ source('jisseki', 'stg_projects') }} c
ON a."projectCode" = c.code
