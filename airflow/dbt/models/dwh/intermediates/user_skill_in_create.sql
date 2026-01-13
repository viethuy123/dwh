{{ config(materialized='table') }}


WITH source_data AS (
    SELECT
        _id AS id,
        "userObjId" AS user_id,
        "userSkills" AS skills_json,
        {{ safe_parse_timestamp('"createdAt"') }} AS created_time,
        {{ safe_parse_timestamp('"updatedAt"') }} AS updated_time
    FROM {{ source('create', 'stg_create_user_skills') }}
),

flattened_data AS (
    SELECT
        sd.id,
        sd.user_id,
        sd.created_time,
        sd.updated_time,
        item->>'skill' AS skill_level,
        item->>'role' AS job_role,
        item->>'userPositionObjId' AS user_position_id,
        CURRENT_TIMESTAMP AS etl_datetime
    FROM source_data sd,
    LATERAL jsonb_array_elements(sd.skills_json::jsonb) AS item
)

SELECT * FROM flattened_data
