from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.sdk import Variable


def connect_pg_db(uri_type: str):
    # Postgres
    pg_user = Variable.get("pg_user")
    pg_pwd = Variable.get("pg_password")
    pg_host = Variable.get("pg_host")
    pg_port = Variable.get("pg_port")
    return "postgresql+psycopg2://{}:{}@{}:{}/{}".format(pg_user, pg_pwd, pg_host, pg_port, uri_type) 

# ==================== COMMON CONFIG ====================
DEFAULT_ARGS = {
    'owner': 'huynnx',
    'start_date': datetime.today() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False,
}

SLACK_CONFIG = {
    'bot_token': Variable.get('slack-bot_token'),
    'chat_id': Variable.get('slack-chat_id')
}

DB_URIS = {
    'staging': connect_pg_db("stg"),
    'warehouse': connect_pg_db("dwh"),
    'monitoring': connect_pg_db("monitoring")
}


# ==================== JIRA CONFIG ====================
def connect_jira_db(uri_type: str):
    # Jira
    jira_user = Variable.get("jira_user")
    jira_pwd = Variable.get("jira_password") 
    jira_host = Variable.get("jira_host")
    jira_port = Variable.get("jira_port")
    return "mysql+pymysql://{}:{}@{}:{}/{}".format(jira_user, jira_pwd, jira_host, jira_port, uri_type)

JIRA_CONFIG = {
    'source_name': 'jira',
    'target_schema': 'src_jira',
    'source_uri': connect_jira_db("jira8db"),
    'external_dag_id': 'dag-jira8db_restore',
    
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
        {'name': 'customfieldvalue', 'type': 'heavy', 'chunksize': 100000},
    ],
    
    'fdw_sync': {
        'tgt_schema': 'jira_fdw',
        'src_schema': 'src_jira',
        'server_name': 'staging_server'
    }
}


# ==================== CREATE CONFIG ====================
def get_create_source_uri():
    """Tạo MongoDB URI"""
    user = Variable.get("create_user")
    pwd = Variable.get("create_password")
    host = Variable.get("create_host")
    port = Variable.get("create_port")
    return f"mongodb://{user}:{pwd}@{host}:{port}/portal"


CREATE_CONFIG = {
    'source_name': 'create',
    'target_schema': 'src_create',
    'source_uri': get_create_source_uri(),
    'database_name': 'portal',
    
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
    ],
    
    'fdw_sync': {
        'tgt_schema': 'create_fdw',
        'src_schema': 'src_create',
        'server_name': 'staging_server'
    }
}


# ==================== HELPER FUNCTIONS ====================
def get_pool_name(table_type: str) -> str:
    """Trả về pool name dựa vào table type"""
    return 'heavy_task_pool' if table_type == 'heavy' else 'default_pool'


def get_target_table_name(source_table: str) -> str:
    """Tạo tên bảng đích từ tên bảng nguồn"""
    return f"stg_{source_table}"




