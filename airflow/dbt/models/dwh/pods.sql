{{ config(materialized='table') }}

SELECT
    _id as pod_id,
    "projectCode" as project_code,
    "projectName" as project_name,
    "projectType" as project_type,
    "projectSize" as project_size,
    "projectRank" as project_rank,
    "projectOverview" as project_overview,
    "projectCategory" as project_category,
    "warrantyCondition" as warranty_condition,
    "developmentModel" as development_model,
    "departmentObjId" as department_id,
    "jiraUrl" as jira_url,
    TO_TIMESTAMP(nullif("startDate",''),'DD-MM-YYYY HH24:MI:SS') as start_date,
    TO_TIMESTAMP(nullif("planUATDate",''),'DD-MM-YYYY HH24:MI:SS') as plan_uat_date,
    TO_TIMESTAMP(nullif("planReleaseDate",''),'DD-MM-YYYY HH24:MI:SS') as plan_release_date,
    TO_TIMESTAMP(nullif("finalReleaseDate",''),'DD-MM-YYYY HH24:MI:SS') as final_release_date,
    "statusPOD" as pod_status,
    status,
    "isDeleted" as is_deleted,
    TO_TIMESTAMP("createdAt",'YYYY-MM-DD HH24:MI:SS') as created_time,
    TO_TIMESTAMP("updatedAt",'YYYY-MM-DD HH24:MI:SS') as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_pods') }}
