
{{ config(
    materialized='table',
) }}
WITH raw_bridge AS (
    SELECT
        project_id,
        category_id
    FROM {{ source('dwh', 'jisseki_project_categories') }} -- Giả định có bảng trung gian này
),
project_keys AS (
    -- Chỉ cần Project_Key và Project_ID từ Fact
    SELECT project_key, project_id FROM {{ ref('fct_jisseki_projects') }} 
),
category_keys AS (
    -- Chỉ cần Category_Key và Category_ID từ Dim
    SELECT category_key, category_id FROM {{ ref('dim_jisseki_categories') }} 
)
SELECT
    -- SK cho Bridge Table (Cặp khóa)
    {{ generate_sk(['pk.project_key', 'ck.category_key']) }} AS bridge_key, 
    
    pk.project_key,
    ck.category_key
FROM raw_bridge rb
INNER JOIN project_keys pk
  ON rb.project_id = pk.project_id
INNER JOIN category_keys ck
  ON rb.category_id = ck.category_id