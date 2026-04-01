{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_contract') }}
),

renamed AS (
    SELECT
        -- Primary Key & Foreign Keys (Nên đổi tên cho rõ ràng)
        id AS contract_id,
        employee_id,
        contract_type_id,
        z_contract_type_id as contract_type, 
        cast(z_employee_code AS INTEGER) AS employee_code,

        -- Info (Giữ nguyên nếu kiểu dữ liệu đã đúng)
        name,
        state,
        date_start,
        date_end,
        resign_date, -- Cột quan trọng bạn tìm thấy lúc nãy
   
    FROM source
)

SELECT * FROM renamed