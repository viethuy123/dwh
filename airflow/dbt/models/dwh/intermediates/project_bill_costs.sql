{{ config(materialized='table') }}

SELECT
    _id as id,
    "departmentObjId" as department_id,
    "projectObjId" as project_id,
    "billEffortMenDay" as bill_effort_men_day,
    "billEffortMenMonth" as bill_effort_men_month,
    "status" as status,
    "isDeleted" as is_deleted,
    "closingMonthObjId" as closing_month_id,
    "reason" as reason,
    "billKPI" as bill_kpi,
    "billInternal" as bill_internal,
    "feedback" as feedback,
    "projectType" as project_type,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_create_project_bill_costs') }}