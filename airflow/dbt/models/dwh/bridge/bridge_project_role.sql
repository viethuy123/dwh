
    {{ config(enabled=false) }}
select
    id,
    project_id,
    user_email,
    project_role_id,
    etl_datetime
from {{ ref('jira_project_role_actor') }}
