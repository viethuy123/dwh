{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_hr_job') }}
),

transformed AS (
    SELECT
        -- 1. Primary & Foreign Keys
        CAST(id AS INTEGER) AS job_id,
        CAST(department_id AS INTEGER) AS department_id,
        CAST(company_id AS INTEGER) AS company_id,
        CAST(contract_type_id AS INTEGER) AS contract_type_id,
        CAST(manager_id AS INTEGER) AS manager_id,
        CAST(user_id AS INTEGER) AS user_id,
        CAST(address_id AS INTEGER) AS address_id,
        CAST(alias_id AS INTEGER) AS alias_id,
        CAST(industry_id AS INTEGER) AS industry_id,
        CAST(website_id AS INTEGER) AS website_id,
        CAST(group_role_id AS INTEGER) AS group_role_id,
        CAST(group_job_id AS INTEGER) AS group_job_id,
        CAST(division_id AS INTEGER) AS division_id,

        -- 2. Core Job Information
        CAST(job_code AS VARCHAR) AS job_code,
        name AS name_json, -- JSONB
        description AS description_json, -- JSONB
        CAST(requirements AS TEXT) AS requirements,
        CAST(no_of_employee AS INTEGER) AS current_member_count,
        CAST(expected_employees AS INTEGER) AS expected_member_count,
        CAST(no_of_recruitment AS INTEGER) AS recruitment_target,
        CAST(no_of_hired_employee AS INTEGER) AS hired_member_count,
        CAST(sequence AS INTEGER) AS sequence_order,
        CAST(color AS INTEGER) AS color_index,
        CAST(active AS BOOLEAN) AS is_active,
        
        -- 3. Additional Codes/Properties
        CAST(pre_code AS VARCHAR) AS pre_code,
        CAST(mid_code AS VARCHAR) AS mid_code,
        CAST(secondary_job AS VARCHAR) AS secondary_job_title,
        CAST(group_job AS VARCHAR) AS group_job_name,
        job_properties, -- JSONB
        job_details, -- JSONB
        
        -- 4. Dates & Planning
        CAST(date_from AS DATE) AS start_date,
        CAST(date_to AS DATE) AS end_date,
        CAST(published_date AS DATE) AS published_date,
        
        -- 5. Website & SEO Metadata
        CAST(is_published AS BOOLEAN) AS is_published_on_web,
        CAST(is_delivery AS BOOLEAN) AS is_delivery_role,
        CAST(is_security AS BOOLEAN) AS is_security_role,
        website_description AS website_description_json,
        website_meta_title AS seo_title_json,
        website_meta_description AS seo_description_json,
        website_meta_keywords AS seo_keywords_json,
        CAST(website_meta_og_img AS VARCHAR) AS seo_og_image_url,
        seo_name AS seo_slug_json,
        applicant_properties_definition, -- JSONB

        -- 6. Audit Fields
        CAST(create_uid AS INTEGER) AS created_by_user_id,
        CAST(write_uid AS INTEGER) AS updated_by_user_id,
        CAST(create_date AS TIMESTAMP) AS created_at,
        CAST(write_date AS TIMESTAMP) AS updated_at,
        CAST(etl_datetime AS TIMESTAMP) AS etl_datetime

    FROM source
)

SELECT * FROM transformed