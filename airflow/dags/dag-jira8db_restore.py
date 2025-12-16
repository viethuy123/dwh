# dags/dag_jira8db_restore.py
"""
Restore Jira DB từ Dropbox backup (chạy 7 PM hàng ngày)
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta
from config import SOURCES, DEFAULT_ARGS
from factories.restore_factory import create_restore_task_group

# Lấy config
jira_config = SOURCES['jira']
restore_config = jira_config['restore']

# Tạo DAG
dag = DAG(
    dag_id=restore_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=restore_config['schedule'],  # 7 PM (0 19 * * *)
    catchup=False,
    dagrun_timeout=timedelta(minutes=restore_config['timeout_minutes']),
    description='Restore Jira8 DB from Dropbox backup',
    tags=['jira', 'restore', 'backup', 'dropbox']
)

with dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='all_done')
    
    # Restore tasks
    restore_group = create_restore_task_group(dag, 'jira', restore_config)
    
    # Dependencies
    start >> restore_group >> end