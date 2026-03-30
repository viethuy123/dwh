{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_employee') }} -- Thay đổi source name cho đúng với schema của bạn
),

renamed AS (
    SELECT
        -- Định dạng: CAST(column_name AS data_type) AS column_name
        
        -- IDs & Foreign Keys (BigInt/Double Precision)
        CAST(id AS BIGINT) AS employee_id,
        CAST(resource_id AS BIGINT) AS resource_id,
        CAST(company_id AS BIGINT) AS company_id,
        CAST(resource_calendar_id AS BIGINT) AS resource_calendar_id,
        CAST(department_id AS BIGINT) AS department_id,
        CAST(address_id AS BIGINT) AS address_id,
        CAST(job_id AS DOUBLE PRECISION) AS job_id,
        CAST(user_id AS DOUBLE PRECISION) AS user_id,
        CAST(parent_id AS DOUBLE PRECISION) AS parent_id,
        CAST(coach_id AS DOUBLE PRECISION) AS coach_id,

        -- Thông tin cơ bản (Text)
        CAST(name AS TEXT) AS name,
        CAST(job_title AS TEXT) AS job_title,
        CAST(work_phone AS TEXT) AS work_phone,
        CAST(mobile_phone AS TEXT) AS mobile_phone,
        CAST(work_email AS TEXT) AS work_email,
        CAST(employee_type AS TEXT) AS employee_type,

        -- Thông tin cá nhân & Địa chỉ (Text)
        CAST(gender AS TEXT) AS gender,
        CAST(marital AS TEXT) AS marital,
        CAST(private_street AS TEXT) AS private_street,
        CAST(private_city AS TEXT) AS private_city,
        CAST(private_zip AS TEXT) AS private_zip,
        CAST(private_phone AS TEXT) AS private_phone,
        CAST(private_email AS TEXT) AS private_email,
        CAST(lang AS TEXT) AS language,
        CAST(identification_id AS TEXT) AS identification_id,
        CAST(passport_id AS TEXT) AS passport_id,
        CAST(ssnid AS TEXT) AS ssnid,
        CAST(sinid AS TEXT) AS sinid,

        -- Ngày tháng (Date / Timestamp)
        CAST(birthday AS DATE) AS birthday,
        CAST(joining_date AS DATE) AS joining_date,
        CAST(start_working_date AS DATE) AS start_working_date,
        CAST(probation_start_date AS DATE) AS probation_start_date,
        CAST(traineeship_start_date AS DATE) AS traineeship_start_date,
        CAST(departure_date AS DATE) AS departure_date,
        CAST(resign_date AS DATE) AS resign_date,
        CAST(create_date AS TIMESTAMP) AS create_date,
        CAST(write_date AS TIMESTAMP) AS write_date,
        CAST(last_check_in AS TIMESTAMP) AS last_check_in,
        CAST(last_check_out AS TIMESTAMP) AS last_check_out,

        -- Các trường Boolean (Trạng thái)
        CAST(active AS BOOLEAN) AS is_active,
        CAST(is_flexible AS BOOLEAN) AS is_flexible,
        CAST(is_fully_flexible AS BOOLEAN) AS is_fully_flexible,
        CAST(disabled AS BOOLEAN) AS is_disabled,
        CAST(is_party_member AS BOOLEAN) AS is_party_member,
        CAST(is_union_member AS BOOLEAN) AS is_union_member,

        -- Các trường tùy chỉnh (Custom Fields z_ / x_)
        CAST(z_employee_code AS DOUBLE PRECISION) AS employee_code,
        CAST(z_rank_id AS DOUBLE PRECISION) AS rank_id,
        CAST(z_academic_level_id AS DOUBLE PRECISION) AS academic_level_id,
        CAST(z_qualification_id AS TEXT) AS qualification_id,
        CAST(z_level AS TEXT) AS level,
        CAST(z_type_employee_id AS DOUBLE PRECISION) AS type_employee_id,
        CAST(x_id_card_supply_date AS DATE) AS id_card_supply_date,
        CAST(x_id_card_supply_address AS TEXT) AS id_card_supply_address,

        -- Log & Hệ thống
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime

        -- Lưu ý: Bạn có thể thêm đầy đủ ~100 cột còn lại theo cấu trúc CAST này
        -- dựa vào kiểu dữ liệu ghi bên cạnh tên cột trong ảnh.

    FROM source
)

SELECT * FROM renamed