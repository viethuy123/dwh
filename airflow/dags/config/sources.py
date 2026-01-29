"""Source system configurations (Jira, Create, Jisseki)"""
from config.databases import get_mysql_uri_builder, get_mongo_uri_builder

SOURCES = {
    'jira': {
        'restore': {
            'enabled': True,
            'dag_id': 'dag_restore_jira',
            'source': 'dropbox',
            'db_type': 'mysql',
            'db_name': 'jira8db',
            'db_password_var': 'jira_password',
            'backup_filename_template': 'jira8db_bk_{date}_22.zip',
            'restore_script': '/opt/airflow/scripts/restore_jira8db.sh',
            'unzip_script': '/opt/airflow/py/unzip_backup_file.py',
            'schedule': '0 2 * * *',
            'timeout_minutes': 60,
        },
        
        'ingestion': {
            'enabled': True,
            'dag_id': 'dag-jira_to_staging',
            'source_type': 'mysql',
            'source_db': 'jira8db',
            'source_uri_fn': get_mysql_uri_builder('jira8db', 'jira'),
            'target_schema': 'stg',
            'schedule': '0 3 * * *',
            'timeout_minutes': 120,
            'wait_for_dag': 'dag_restore_jira',
            'tables': [
                {'name': 'project', 'type': 'light', 'chunksize': None},
                {'name': 'issuestatus', 'type': 'light', 'chunksize': None},
                {'name': 'projectrole', 'type': 'light', 'chunksize': None},
                {'name': 'projectroleactor', 'type': 'light', 'chunksize': None},
                {'name': 'resolution', 'type': 'light', 'chunksize': None},
                {'name': 'priority', 'type': 'light', 'chunksize': None},
                {'name': 'issuetype', 'type': 'light', 'chunksize': None},
                {'name': 'customfieldoption', 'type': 'light', 'chunksize': None},
                {'name': 'app_user', 'type': 'light', 'chunksize': None},
                {'name': 'worklog', 'type': 'heavy', 'chunksize': 50000},
                {'name': 'jiraissue', 'type': 'heavy', 'chunksize': 50000},
                {'name': 'customfieldvalue', 'type': 'heavy', 'chunksize': 50000},
            ],

        }
    },
    
    'create': {
        'restore': {
            'enabled': False,
        },
        
        'ingestion': {
            'enabled': True,
            'dag_id': 'dag-create_to_staging',
            'source_type': 'mongodb',
            'source_db': 'portal',
            'source_uri_fn': get_mongo_uri_builder('portal', 'create'),
            'target_schema': 'stg',
            'schedule': '0 20 * * *',
            'timeout_minutes': 60,
            'wait_for_dag': None,
            'tables': [
                {'name': 'projects', 'type': 'light', 'chunksize': None},
                {'name': 'project_categories', 'type': 'light', 'chunksize': None},
                {'name': 'pods', 'type': 'light', 'chunksize': None},
                {'name': 'project_members', 'type': 'light', 'chunksize': None},
                {'name': 'user_skills', 'type': 'light', 'chunksize': None},
                {'name': 'billable_efforts_approveds', 'type': 'light', 'chunksize': None},
                {'name': 'users', 'type': 'light', 'chunksize': None},
                {'name': 'branches', 'type': 'light', 'chunksize': None},
                {'name': 'user_positions', 'type': 'light', 'chunksize': None},
                {'name': 'company_departments', 'type': 'light', 'chunksize': None},
                {'name': 'user_infos', 'type': 'light', 'chunksize': None},
                {'name': 'profit_loss_expenses', 'type': 'light', 'chunksize': None},
                {'name': 'profit_loss_project_expenses', 'type': 'light', 'chunksize': None},
                {'name': 'staff_attendances', 'type': 'light', 'chunksize': None},
                {'name': 'staff_attendance_types', 'type': 'light', 'chunksize': None},
                {'name': 'salaries', 'type': 'light', 'chunksize': None},
                {'name': 'project_bill_costs', 'type': 'light', 'chunksize': None},
                {'name': 'customers', 'type': 'light', 'chunksize': None},
                {'name': 'markets', 'type': 'light', 'chunksize': None},
                {'name': 'project_reports', 'type': 'light', 'chunksize': None},
                {'name': 'project_report_details', 'type': 'light', 'chunksize': None},
                {'name': 'staff_log_works', 'type': 'heavy', 'chunksize': 50000},
                {'name': 'staff_log_work_jira_deletes', 'type': 'light', 'chunksize': None},
                {'name': 'staff_log_work_jira_updates', 'type': 'light', 'chunksize': None},
            ],

        }
    },
    
    'jisseki': {
        'restore': {
            'enabled': False,
        },
        
        'ingestion': {
            'enabled': True,
            'dag_id': 'dag-jisseki_to_staging',
            'source_type': 'mysql',
            'source_db': 'go_jisseki',
            'source_uri_fn': get_mysql_uri_builder('go_jisseki', 'jisseki'),
            'target_schema': 'stg',
            'schedule': '0 20 * * *',
            'timeout_minutes': 60,
            'wait_for_dag': None,
            'tables': [
                {'name': 'projects', 'type': 'light', 'chunksize': None},
                {'name': 'project_customer', 'type': 'light', 'chunksize': None},
                {'name': 'project_categories', 'type': 'light', 'chunksize': None},
                {'name': 'customers', 'type': 'light', 'chunksize': None},
                {'name': 'countries', 'type': 'light', 'chunksize': None},
                {'name': 'categories', 'type': 'light', 'chunksize': None},
            ],
        }
    }
}