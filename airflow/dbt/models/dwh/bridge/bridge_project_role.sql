{{ config(materialized='table') }}
select
    id,
    project_id,
    user_email,
    project_role_id
from {{ ref('jira_project_role_actor') }}