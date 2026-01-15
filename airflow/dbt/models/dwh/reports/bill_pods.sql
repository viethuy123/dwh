{{ config(materialized='table') }}

select * from {{ ref('dim_pods') }}
where is_deleted != 'Yes'