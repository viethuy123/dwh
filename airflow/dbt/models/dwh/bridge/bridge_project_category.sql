{{ config(materialized='table') }}

WITH
create_jira_project as (
    select 
    cp.id::TEXT as project_id,
    cp.project_category_id::TEXT as category_id
from {{ ref('create_project') }} cp
),

jisseki_project as (
    select 
    project_id::TEXT as project_id,
    category_id::TEXT as category_id
from {{ ref('jisseki_project_cate') }}
)

select * from create_jira_project
union
select * from jisseki_project