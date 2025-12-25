# dags/dag_create_to_staging.py
"""
Extract & Load CREATE data từ MongoDB vào Staging PostgreSQL
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import SOURCES , DEFAULT_ARGS
from factories.ingestion_factory import create_ingestion_task_group

# Lấy config
create_config = SOURCES['create']
ingestion_config = create_config['ingestion']

# Tạo DAG
dag = DAG(
    dag_id=ingestion_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('jira_staging_completed')],  # Trigger by datasets
    catchup=False,
    dagrun_timeout=timedelta(minutes=ingestion_config['timeout_minutes']),
    description='Extract CREATE data from MongoDB and load to Staging',
    tags=['create', 'mongodb', 'ingestion', 'staging']
)

with dag:
    start = EmptyOperator(task_id='start')
    ingestion_group = create_ingestion_task_group(dag, 'create', ingestion_config)
    end = EmptyOperator(task_id='end', outlets=[Dataset('create_staging_completed')], trigger_rule='all_success')
    
    # Dependencies
    start >> ingestion_group >> end