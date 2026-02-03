{{ config(materialized='table') }}


SELECT
    _id as position_id,
    "positionCode" as position_code,
    "positionName" as position_name,
    "positionLevel" as position_level,
    "positionDescription" as position_description,
    "positionStatus" as position_status,
    type as position_type,
    status as status,
    "isDeleted"as is_deleted,
    "updatedAt" as updated_time,
    etl_datetime
FROM {{ source('create', 'stg_create_user_positions') }}
