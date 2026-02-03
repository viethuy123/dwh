{{ config(materialized='table') }}

WITH 
create_jira_cate as (
    select 
    id::TEXT as category_id,
    category_name,
    etl_datetime
    from {{ ref('create_project_cate') }}
),
jisseki_cate as (
    select 
    id::TEXT as category_id,
    category_name,
    etl_datetime
    from {{ ref('jisseki_categories') }}
)
select * from create_jira_cate
union
select * from jisseki_cate