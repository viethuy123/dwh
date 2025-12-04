{{
    config(
        materialized='table',
    )
}}

WITH category AS (
    SELECT
        category_id,
        category_name
    FROM {{ source('dwh', 'jisseki_categories') }} a
)
SELECT
    {{ generate_sk(['category.category_id']) }} AS category_key, 
    category_id,
    category_name
FROM category