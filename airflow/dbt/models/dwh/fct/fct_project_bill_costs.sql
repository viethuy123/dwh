{{ config(materialized='table') }}

SELECT
    *
FROM {{ ref('project_bill_costs') }} 