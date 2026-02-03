{{ config(materialized='table') }}

select *, etl_datetime from {{ ref('dim_pods') }}
where is_deleted != 'Yes'