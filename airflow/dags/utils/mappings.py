warehouse_mapping ={
    'branches': 'src_create.stg_branches',
    'departments': 'src_create.stg_company_departments',
    'jira_issue_resolution': 'src_jira.stg_resolution',
    'jira_issue_status': 'src_jira.stg_issuestatus',
    'jira_issue_types': 'src_jira.stg_issuetype',
    'jira_issues': 'src_jira.stg_jiraissue',
    'jira_worklog': 'src_jira.stg_worklog',
    'jira_issue_priority': 'src_jira.stg_priority',
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
    'billable_efforts_approveds': 'src_create.stg_billable_efforts_approveds'
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
    "fct_issues":["jira_issues", "jira_issue_types", "jira_issue_resolution", "jira_issue_status", "jira_issue_priority"],
    "fct_worklog": ["jira_worklog","jira_issues"],
    "fct_project_members": "project_members",
    "dim_pods": "pods",
    "fct_pod_member_efforts": "billable_efforts_approveds"
}