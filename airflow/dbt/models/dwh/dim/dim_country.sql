{{ config(materialized='table') }}

select 
    country_id,
    coalesce(
        {{ parse_python_json("regexp_replace(json_name, '([a-zA-ZÀ-ỹ])\"([a-zA-ZÀ-ỹ])', '\\1''\\2', 'g')") }}->>'vi_VN',
        {{ parse_python_json("regexp_replace(json_name, '([a-zA-ZÀ-ỹ])\"([a-zA-ZÀ-ỹ])', '\\1''\\2', 'g')") }}->>'en_US',
        'unknown'
    ) as country_name,
    country_code,
    country_currency_id,
    phone_code,
    etl_datetime
from {{ ref('odoo_res_country') }}