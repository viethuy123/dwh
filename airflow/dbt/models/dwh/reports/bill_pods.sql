{{ config(materialized='table', enabled=false) }}

select *, etl_datetime from {{ ref('dim_pods') }}
where is_deleted != 'Yes'