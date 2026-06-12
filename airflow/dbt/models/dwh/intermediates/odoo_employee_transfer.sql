{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_employee_transfer') }}
)

select
    cast(id as integer)              as id,
    cast(employee_id as integer)     as member_id,
    cast(transfer_type_id as integer)      as transfer_type_id,
    trim(name)                       as transfer_name,
    cast(z_employee_code as integer)            as member_code,
    date_trunc('month', date)::date as transfer_start_date,
    (date_trunc('month', received_date) + INTERVAL '1 month - 1 day')::date as transfer_end_date, 
    type        as transfer_type,
    CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
from source