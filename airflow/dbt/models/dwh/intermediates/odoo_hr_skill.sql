{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_skill') }}
),

renamed AS (
    SELECT
        -- Primary Key
        CAST(id AS INTEGER) AS skill_id,
        
        -- Foreign Keys
        CAST(skill_type_id AS INTEGER) AS skill_type_id,
        CAST(group_skill_id AS INTEGER) AS group_skill_id,
        
        -- Fields
        CAST(sequence AS INTEGER) AS sequence_order,
        
        -- Xử lý JSONB: Lấy giá trị mặc định nếu cần (ví dụ: name->>'en_US')
        -- Hoặc để nguyên để xử lý ở layer sau
        -- name AS name_json,
        COALESCE(
            {{ parse_python_json('name') }}->>'vi_VN',
            {{ parse_python_json('name') }}->>'en_US',
            'unknown'
        ) AS skill_name,
        
        CAST(allow_select AS BOOLEAN) AS is_selectable,
        
        -- Audit Fields
        create_uid,
        write_uid,
        CAST(create_date AS TIMESTAMP) AS created_at,
        CAST(write_date AS TIMESTAMP) AS updated_at,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
        
    FROM source
)

SELECT * FROM renamed