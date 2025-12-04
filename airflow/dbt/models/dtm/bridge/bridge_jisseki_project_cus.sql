{{ config(
    materialized='table',
) }}
WITH raw_bridge AS (
    SELECT
        project_id,
        customer_id
    FROM {{ source('dwh', 'jisseki_project_customers') }}
),
project_keys AS (
    SELECT project_key, project_id FROM {{ ref('fct_jisseki_projects') }} 
),
customer_keys AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_jisseki_customers') }} 
)
SELECT
    -- Khóa chính của Bridge: Cặp Project_Key và Customer_Key
    {{ generate_sk(['pk.project_key', 'ck.customer_key']) }} AS bridge_key,
    pk.project_key,
    ck.customer_key
FROM raw_bridge rb
INNER JOIN project_keys pk
  ON rb.project_id = pk.project_id
INNER JOIN customer_keys ck
  ON rb.customer_id = ck.customer_id