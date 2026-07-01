{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_contract') }}
),

renamed AS (
    SELECT
        -- Primary Key & Foreign Keys (Nên đổi tên cho rõ ràng)
        id AS contract_id,
        employee_id as member_id, -- Đổi tên để rõ ràng hơn, vì Odoo có thể dùng employee_id cho cả nhân viên và ứng viên
        contract_type_id,
        z_contract_type as contract_type, 
        -- cast(z_employee_code AS INTEGER) AS member_code,
        CASE
            WHEN TRIM(z_employee_code) ~ '^[0-9]+$'
            THEN z_employee_code::BIGINT
            ELSE NULL
        END AS member_code,

        -- Info (Giữ nguyên nếu kiểu dữ liệu đã đúng)
        name,
        state,
        date_start,
        date_end,
        resign_date,
        etl_datetime
   
    FROM source
)

SELECT * FROM renamed