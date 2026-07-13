{{ config(materialized='table') }}

select 
    CAST(id AS BIGINT) AS country_id,
    name AS json_name,
    code AS country_code,
    currency_id AS country_currency_id,
    phone_code,
    CAST(etl_datetime AS TIMESTAMP) AS etl_datetime
from {{ source('odoo', 'stg_odoo_res_country')}}

