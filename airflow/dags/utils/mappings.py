intermediate_mapping ={
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
    'users_infos': 'src_create.stg_user_infos',
    'users_infos_tranforms': 'dwh.intermediates.users_infos_tranforms',
    'salaries': 'src_create.stg_salaries',
    'create_attendances': 'src_create.stg_staff_attendances',
    'create_attendance_types': 'src_create.stg_staff_attendance_types',
    'member_attendance_daily': 'stg_create_staff_attendances',
    'pods': 'src_create.stg_pods',
    'billable_efforts_approveds': 'stg_create_billable_efforts_approveds',
    'create_project': 'stg_create_projects',
    # 'jisseki_project': 'stg_jisseki_projects',
    # # New intermediates

    # 'jisseki_categories': 'src_jisseki.stg_project_categories',
    # 'jisseki_countries': 'src_jisseki.stg_countries',
    # 'jisseki_customers': 'src_jisseki.stg_project_customer',
    # 'jisseki_project_cate': 'src_jisseki.stg_project_categories',
    # 'jisseki_project_cus': 'src_jisseki.stg_project_customer',
    'skill_members' : 'stg_skill_members',
    'create_project_customer' : 'stg_create_project_customer',
    'create_project_cate' : 'stg_create_project_categories',
    'create_staff_overtimes' : 'src_create.stg_create_staff_overtimes',
    'create_staff_overtime_details' : 'src_create.stg_create_staff_overtime_details',
    'create_closing_months' : 'src_create.stg_create_closing_months',
    # 'staff_log_works' : 'stg_create_staff_log_works',
    # 'staff_log_work_jira_deletes' : 'stg_create_staff_log_work_jira_deletes',
    # 'staff_log_work_jira_updates' : 'stg_create_staff_log_work_jira_updates',
    'odoo_hr_member': 'src_odoo.stg_odoo_hr_employee',
    'odoo_hr_member_education': 'src_odoo.stg_odoo_hr_employee_education',
    'odoo_hr_member_school': 'src_odoo.stg_odoo_hr_employee_school',
    'odoo_hr_graduation_rank': 'src_odoo.stg_odoo_hr_graduation_rank',
    'odoo_hr_job': 'src_odoo.stg_odoo_hr_job',
    'odoo_hr_rank': 'src_odoo.stg_odoo_hr_rank',
    'odoo_hr_skill': 'src_odoo.stg_odoo_hr_skill',
    'odoo_hr_skill_level': 'src_odoo.stg_odoo_hr_skill_level',
    'odoo_hr_skill_type': 'src_odoo.stg_odoo_hr_skill_type',
    'odoo_hr_member_skill': 'src_odoo.stg_odoo_hr_employee_skill',
    'odoo_hr_member_skill_log': 'src_odoo.stg_odoo_hr_employee_skill_log',
    'odoo_z_academic_level': 'src_odoo.stg_odoo_z_academic_level',
    'odoo_z_qualification': 'src_odoo.stg_odoo_z_qualification',
    'odoo_hr_contract': 'src_odoo.stg_odoo_hr_contract',
    'odoo_branch': 'src_odoo.stg_res_company',
    'odoo_division': 'src_odoo.stg_hr_department',
    'odoo_branch': 'src_odoo.stg_res_company',
    'odoo_division': 'src_odoo.stg_hr_department',
    'odoo_z_type_employee': 'src_odoo.stg_odoo_z_type_employee',
    'odoo_employee_transfer': 'src_odoo.stg_odoo_employee_transfer',
    # 'odoo_hr_contract_type': 'src_odoo.stg_odoo_hr_contract_type'
    'odoo_res_country': 'src_odoo.stg_res_country'
    
    

}

# hr_dtm_mapping = {
#     "dim_branches": "branches",
#     "dim_departments": "departments",
#     "dim_month_year": "time_series",
#     "dim_positions": "user_positions",
#     "fct_hrm_employees": ["time_series", "users", "staff_attendances"]
# }

# jira_dtm_mapping = {
#     "dim_members": ["users", "branches", "departments", "user_positions"],
#     "dim_projects": "projects",
#     "dim_time_series": "time_series",
#     "dim_jira_issues":["jira_issues", "jira_issue_types", "jira_issue_resolution", "jira_issue_status", "jira_issue_priority"],
#     "fct_worklog": ["jira_worklog","jira_issues"],
#     "dim_pods": "pods",
#     "fct_pod_member_efforts": "billable_efforts_approveds"
# }


report_mapping = {
    "detect_resources": ["dim_members_scd", "dim_projects", "dim_time_series", "fct_worklog", "fct_project_members", "dim_pods", "fct_pod_member_efforts"],
    # "mview_member_free_efforts": ["dim_members", "dim_time_series", "fct_worklog","dim_skill_members"],
    # "user_join_projects": ["fct_jira_issues", "dim_members_scd", "dim_projects", "dim_time_series", "fct_worklog", "fct_project_members"],
    # "hr_data_user": ["dim_members_new"],
    "hr_attendance": ["member_attendance_daily", "dim_attendance_types", "dim_odoo_members"],
    "hr_skill_members": ["dim_odoo_members", "dim_skill_odoo", "dim_skill_level", "fct_member_skill"],
    "hr_data_user_new": ["dim_odoo_members", "fct_member_education", "dim_odoo_branch", "dim_odoo_division"],
    "hr_data_user_snapshot": ["dim_hc_snapshot_month"],
    "hr_data_user_snapshot_scd": ["dim_hc_snapshot_month_scd"]
}
dim_mapping = {
    # "dim_jira_issues": "jira_issues",
    "dim_members": "",
    "dim_pods": "",
    "dim_projects": "",
    # "dim_members_scd": "",
    "dim_members_new": "",
    "dim_branches": "branches",
    "dim_departments": "departments",
    "dim_positions": "positions",
    "dim_project_members": "project_members",
    # "dim_member_email_effective": "users",
    # New dims
    "dim_jira_project_role": "jira_project_role",
    # "dim_members_scd": "users",
    "dim_project_cate": "create_project_cate, jisseki_project_cate",
    "dim_project_cus": "create_project_customer, jisseki_project_cus",
    "dim_skill_members" : "skill_members",
    'dim_odoo_skill' : 'odoo_hr_skill',
    'dim_skill_level' : 'odoo_hr_skill_level',
    "dim_projects" : "create_projects, jisseki_projects, pods",
    "dim_date":"",
    # "dim_member_education": "odoo_hr_member_education",
    "dim_odoo_members": "odoo_hr_member",
    "dim_attendance_types": "create_attendance_types",
    "dim_odoo_branch": "odoo_branch",
    "dim_odoo_division": "odoo_division",
    "dim_odoo_job": "odoo_hr_job",
    "dim_odoo_members_scd": "odoo_members_snapshot",
    "dim_hc_snapshot_month": "dim_odoo_members",
    "dim_hc_snapshot_month_scd": "dim_odoo_members_scd",
    "dim_member_status": "dim_odoo_members",
    "dim_closing_month": "create_closing_months",
    "dim_country": "odoo_res_country",
    # "dim_seniority": ""
}

fct_mapping = {
    "fct_worklogs": "jira_worklog",
    "fct_pod_member_efforts": "billable_efforts_approveds",
    "fct_project": "jisseki_projects, pods",
    "fct_member_monthly_snapshot": "dim_members_new",
    'fct_member_skill': 'odoo_hr_member_skill',
    'fct_attendance_daily': 'member_attendance_daily',
    'fct_jira_issues': 'jira_issues',
    'fct_member_education': 'odoo_hr_member_education',
    # 'fct_hc_snapshot': 'dim_odoo_members'
    'fct_member_overtime': 'create_staff_overtime_detail'

}

bridge_mapping = {
    "bridge_project_customer": "create_project_customer, jisseki_project_cus",
    "bridge_project_category": "create_project_cate, jisseki_project_cate",
    "bridge_project_role": "create_project_role, jisseki_project_role",
    "bridge_member_create_with_odoo": "dim_odoo_members, dim_members_new"
}

snapshot_mapping = {
    "odoo_members_snapshot": "dim_odoo_members",
    "members_snapshot_full": "users",
    "members_snapshot_some_col": "users"
}
