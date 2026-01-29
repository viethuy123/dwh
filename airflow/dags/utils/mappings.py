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
    'billable_efforts_approveds': 'stg_create_billable_efforts_approveds',
    'create_project': 'stg_create_projects',
    'jisseki_project': 'stg_jisseki_projects',
    # New intermediates

    'jisseki_categories': 'src_jisseki.stg_project_categories',
    'jisseki_countries': 'src_jisseki.stg_countries',
    'jisseki_customers': 'src_jisseki.stg_project_customer',
    'jisseki_project_cate': 'src_jisseki.stg_project_categories',
    'jisseki_project_cus': 'src_jisseki.stg_project_customer',
    'skill_members' : 'stg_skill_members',
    'create_project_customer' : 'stg_create_project_customer',
    'create_project_cate' : 'stg_create_project_categories',
    'staff_log_works' : 'stg_create_staff_log_works',
    'staff_log_work_jira_deletes' : 'stg_create_staff_log_work_jira_deletes',
    'staff_log_work_jira_updates' : 'stg_create_staff_log_work_jira_updates',

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
    "dim_pods": "pods",
    "fct_pod_member_efforts": "billable_efforts_approveds"
}


bi_dtm = {
    "user_join_project": "user_join_project",
    "detect_resource": "detect_resource"
}

jira_mviews = ["mview_member_free_effort"]

report_mapping = {
    "detect_resources": ["dim_members", "dim_projects", "dim_time_series", "fct_worklog", "fct_project_members", "dim_pods", "fct_pod_member_efforts"],
    "mview_member_free_efforts": ["dim_members", "dim_time_series", "fct_worklog","dim_skill_members"],
    "user_join_projects": ["dim_jira_issues", "dim_members"]
}
dim_mapping = {
    "dim_jira_issues": "jira_issues",
    "dim_members": "",
    "dim_pods": "",
    "dim_projects": "",
    "dim_members_scd": "",
    "dim_branches": "branches",
    "dim_departments": "departments",
    "dim_positions": "positions",
    "dim_project_members": "project_members",
    "dim_member_email_effective": "users",
    # New dims
    "dim_jira_project_role": "jira_project_role",
    # "dim_members_scd": "users",
    "dim_project_cate": "create_project_cate, jisseki_project_cate",
    "dim_project_cus": "create_project_customer, jisseki_project_cus",
    "dim_skill_members" : "skill_members",
    "dim_projects" : "create_projects, jisseki_projects, pods"
}

fct_mapping = {
    "fct_worklogs": "jira_worklog",
    "fct_pod_member_efforts": "billable_efforts_approveds",
    "fct_project": "jisseki_projects, pods"
}

bridge_mapping = {
    "bridge_project_customer": "create_project_customer, jisseki_project_cus",
    "bridge_project_category": "create_project_cate, jisseki_project_cate",
    "bridge_project_role": "create_project_role, jisseki_project_role"
}

snapshot_mapping = {
    "members_snapshot": "users",
}