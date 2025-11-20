{{ config(materialized='table') }}


SELECT
    _id as branch_id,
    "branchCode" as branch_code,
    "branchName" as branch_name,
    "branchAddress" as branch_address,
    "isHidden" as is_hidden,
    status,
    "isDeleted" as is_deleted,
    -- TO_TIMESTAMP("createdAt",'YYYY-MM-DD HH24:MI:SS') as created_time,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('create', 'stg_branches') }}
