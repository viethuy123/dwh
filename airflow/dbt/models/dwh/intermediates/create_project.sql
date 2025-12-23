{{ config(materialized='table') }}


SELECT
    "_id" as id,
    "projectName" as project_name,
    "projectCode" as project_code,
    "projectStatus" as project_status,
    "projectCategoryObjId" as project_category,
    "projectDescription" as project_description,
    "projectLeadObjId" as project_lead,
    "jiraProjectId" as jira_project_id,
    "jiraProjectKey" as jira_project_key,
    "projectType" as project_type,
    note,
    status,
    type,
    "projectCustomerCode" as project_customer_code,
    "projectObjective" as project_objective,
    "projectRank" as project_rank,
    "projectScope" as project_scope,
    "endAt" as end_time,
    "startAt" as start_time,
    "releaseAt" as release_time,
    "projectDiv" as project_div,
    "createdAt" as create_time,
    "isDeleted" as is_deleted,
    "updatedAt" as update_time
FROM {{ source('create', 'stg_create_projects') }}
