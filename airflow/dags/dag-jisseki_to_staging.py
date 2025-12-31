# dags/dag_jisseki_to_staging.py
"""
Extract & Load JISSEKI data từ MySQL vào Staging PostgreSQL
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import SOURCES, DEFAULT_ARGS,  DEFAULT_CHECK_DAG
from factories.ingestion_factory import create_ingestion_task_group

# Lấy config
jisseki_config = SOURCES['jisseki']
ingestion_config = jisseki_config['ingestion']

# Tạo DAG
dag = DAG(
    dag_id=ingestion_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('create_staging_completed')],  # Trigger by datasets
    catchup=False,
    dagrun_timeout=timedelta(minutes=ingestion_config['timeout_minutes']),
    description='Extract JISSEKI data from MySQL and load to Staging',
    tags=['jisseki', 'mysql', 'ingestion', 'staging']
)

with dag:
    start = EmptyOperator(task_id='start')
    
    ingestion_group = create_ingestion_task_group(dag, 'jisseki', ingestion_config)
    
    end = EmptyOperator(task_id='end', outlets=[Dataset('jisseki_staging_completed')], trigger_rule= DEFAULT_CHECK_DAG['trigger_rule'])
    
    # Dependencies
    start >> ingestion_group >> end