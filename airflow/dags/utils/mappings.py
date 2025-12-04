warehouse_mapping ={
    'branches': 'src_create.stg_branches',
    'departments': 'src_create.stg_company_departments',
    'jira_issue_resolution': 'src_jira.stg_resolution',
    'jira_issue_status': 'src_jira.stg_issuestatus',
    'jira_issue_types': 'src_jira.stg_issuetype',
    'jira_issues': 'src_jira.stg_jiraissue',
    'jira_app_user': 'src_jira.stg_app_user',
    'jira_worklog': 'src_jira.stg_worklog',
    'jira_issue_priority': 'src_jira.stg_priority',
    'jira_customfield_option': 'src_jira.stg_customfieldoption',
    'jira_customfield_value': 'src_jira.stg_customfieldvalue',
    'jira_project_role': 'src_jira.stg_projectrole',
    'jira_project_role_actor': 'src_jira.stg_projectroleactor',
    'jira_project': 'src_jira.stg_project',
    'project_members': 'src_create.stg_project_members',
    'project_profit_loss': 'src_create.stg_profit_loss_project_expenses',
    'projects': [
        'src_create.stg_projects',
        'src_jisseki.stg_projects',
        'src_jira.project'
    ],
    'user_positions': 'src_create.stg_user_positions',
    'users': [
        'src_create.stg_users',
        'src_create.stg_user_infos'
    ],
    'salaries': 'src_create.stg_salaries',
    'staff_attendances': 'src_create.stg_staff_attendances',
    'staff_attendances_types': 'src_create.stg_staff_attendance_types',
    'pods': 'src_create.stg_pods',
    'billable_efforts_approveds': 'src_create.stg_billable_efforts_approveds',
    'jisseki_customers': 'src_jisseki.stg_customers',
    'jisseki_countries': 'src_jisseki.stg_countries',
    'jisseki_categories': 'src_jisseki.stg_categories',
    'jisseki_project_customers': 'src_jisseki.stg_project_customers',
    'jisseki_project_categories': 'src_jisseki.stg_project_categories',
    'jisseki_projects': 'src_jisseki.stg_projects'

    
}

hr_dtm_mapping = {
    "dim_branches": "branches",
    "dim_departments": "departments",
    "dim_month_year": "time_series",
    "dim_positions": "user_positions",
    "fct_hrm_employees": ["time_series", "users", "staff_attendances"]
}

jira_dtm_mapping = {
    "dim_members": ["users", "branches", "departments", "user_positions"],
    "dim_projects": "projects",
    "dim_time_series": "time_series",
    "dim_jira_issues":["jira_issues", "jira_issue_types", "jira_issue_resolution", "jira_issue_status", "jira_issue_priority"],
    "fct_worklog": ["jira_worklog","jira_issues"],
    "fct_project_members": "project_members",
    "dim_pods": "pods",
    "fct_pod_member_efforts": "billable_efforts_approveds"
}

public_dtm_mapping = {
    "dim_users_public": ["users", "branches", "departments", "user_positions"],
    # "dim_projects": "projects",
    # "dim_time_series": "time_series",
    "dim_branches_public": "branches",
    "dim_departments_public": "departments",
    "dim_positions_public": "user_positions",
    "dim_jira_issues_public":["jira_issues", "jira_issue_types", "jira_issue_resolution", "jira_issue_status", "jira_issue_priority"],
}

bi_dtm = {
    "user_join_project": "user_join_project",
    "detect_resource": "detect_resource"
}

jira_mviews = ["mview_member_free_effort"]

dimension_dtm_mapping = {
    'dim_jisseki_customers': [
        'customer_snapshot',
        'dwh.jisseki_countries'
    ],
    'dim_jisseki_categories': [
        'dwh.jisseki_categories'
    ],
}

fact_dtm_mapping = {
    'fct_jisseki_projects': [
        'dwh.jisseki_projects'
    ],
}

bridge_dtm_mapping = {
    'bridge_jisseki_project_cate': [
        'dtm.fct_jisseki_projects',
        'dtm.dim_jisseki_categories',
        'dwh.jisseki_project_categories'
    ],
    'bridge_jisseki_project_cus': [
        'dwh.jisseki_project_customers',
        'dtm.fct_jisseki_projects',
        'dtm.dim_jisseki_customers'
    ],
}

snapshot_dtm_mapping = {
        'customer_snapshot' : None,
    }
