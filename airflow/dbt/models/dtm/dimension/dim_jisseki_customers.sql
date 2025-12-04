{{ config(
    materialized='table',
) }}

WITH customer_snapshot AS (
    SELECT
        customer_id,
        company_name,
        customer_name,
        sale_person,
        status,
        type,
        country_id,
        dbt_valid_from,
        dbt_valid_to 
    FROM {{ ref('customer_snapshot') }}
),
country_dim AS (
    SELECT 
        country_id, 
        country_name, 
        country_code 
    FROM {{ source('dwh', 'jisseki_countries') }} 
)
SELECT
    {{ generate_sk(['cs.customer_id', 'cs.dbt_valid_from']) }} AS customer_key,
    
    cs.customer_id,
    cs.company_name,
    cs.customer_name,
    cs.sale_person,
    cs.status,
    cs.type,
    cs.dbt_valid_from,
    cs.dbt_valid_to,
    cd.country_name,
    cd.country_code

FROM customer_snapshot cs
LEFT JOIN country_dim cd
    ON cs.country_id = cd.country_id

