{{ config(materialized='table') }}

WITH source AS (
    SELECT * FROM {{ source('odoo', 'stg_odoo_z_type_employee') }}
)

select
    cast(id as integer)              as member_type_id,
    cast(company_id as integer)      as company_id,
    cast(create_uid as integer)      as create_uid,
    cast(write_uid as integer)       as write_uid,

    trim(name)                       as member_type_name,
    trim(note)                       as note,
    trim(other_type)                 as other_type,

    trim(status)                     as status,

    cast(create_date as timestamp)   as created_at,
    cast(write_date as timestamp)    as updated_at,

    cast(sequence as integer)        as sequence,
    CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
from source