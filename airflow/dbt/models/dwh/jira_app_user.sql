{{ config(materialized='table') }}


SELECT
    "ID" as id,
    "CUSTOMFIELD" as custom_field,
    "CUSTOMFIELDCONFIG" as custom_field_config,
    "PARENTOPTIONID" as parent_option_id,
    "SEQUENCE" as sequence,
    "customvalue" as custom_value,
    "optiontype" as option_type,
    "disabled" as disabled,
    CURRENT_TIMESTAMP as etl_datetime
FROM {{ source('jira', 'stg_customfieldoption') }}
