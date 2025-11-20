{{ config(materialized='table') }}


SELECT
    "ID" as id,
    "ISSUE" as issue_id,
    "CUSTOMFIELD" as custom_field,
    "UPDATED" as updated_time,
    "PARENTKEY" as parent_key,
    "STRINGVALUE" as string_value,
    "NUMBERVALUE" as number_value,
    "TEXTVALUE" as text_value,
    "DATEVALUE" as date_value,
    "VALUETYPE" as value_type,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_customfieldvalue') }}
