{{ config(materialized='table') }}


SELECT
    "_id" as id,
    "projectName" as project_name,
    "projectCode" as project_code,
    "projectStatus" as project_status,
    "projectCategoryObjId" as project_category,
    "projectDescription" as project_description,
    "projectLeadObjId" as project_lead_id,
    "jiraProjectId"::NUMERIC as jira_project_id,
    "jiraProjectKey" as jira_project_key,
    "projectType" as project_type,
    note,
    status,
    type,
    "projectCustomerCode" as project_customer_code,
    "projectObjective" as project_objective,
    "projectRank" as project_rank,
    "projectScope" as project_scope,
    {{ safe_parse_timestamp('"endAt"') }} as end_time,
    {{ safe_parse_timestamp('"startAt"') }} as start_time,
    {{ safe_parse_timestamp('"releaseAt"') }} as release_time,
    "projectDiv" as project_div,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    "isDeleted" as is_deleted,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    etl_datetime
FROM {{ source('create', 'stg_create_projects') }}
