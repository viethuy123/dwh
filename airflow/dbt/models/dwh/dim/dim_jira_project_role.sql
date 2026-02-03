{{ config(materialized='table') }}

select 
    id ,
    role_name ,
    description,
    etl_datetime
from {{ ref('jira_project_role') }}