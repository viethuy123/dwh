{{ config(materialized='table') }}

select 
    id ,
    role_name ,
    description
from {{ ref('jira_project_role') }}