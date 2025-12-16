# dags/dag_create_to_staging.py
"""
Extract & Load CREATE data từ MongoDB vào Staging PostgreSQL
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta
from config import SOURCES , DEFAULT_ARGS
from factories.ingestion_factory import create_ingestion_task_group

# Lấy config
jira_config = SOURCES['jira']
ingestion_config = jira_config['ingestion']

# Tạo DAG
dag = DAG(
    dag_id=ingestion_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=ingestion_config['schedule'],  # 8 PM
    catchup=False,
    dagrun_timeout=timedelta(minutes=ingestion_config['timeout_minutes']),
    description='Extract JIRA data from MongoDB and load to Staging',
    tags=['jira', 'mongodb', 'ingestion', 'staging']
)

with dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='all_done')
    
    # Ingestion tasks
    ingestion_group = create_ingestion_task_group(dag, 'jira', ingestion_config)
    
    # Dependencies
    start >> ingestion_group >> end