{{ config(materialized='table') }}


SELECT
    _id as id,
    "customerId" as customer_id,
    "customerCompanyId" as customer_company_id,
    "customerName" as customer_name,
    status,
    "isDeleted" as is_deleted,
    {{ safe_parse_timestamp('"createdAt"') }} as created_time,
    {{ safe_parse_timestamp('"updatedAt"') }} as updated_time,
    etl_datetime
FROM {{ source('create', 'stg_create_customers') }}
