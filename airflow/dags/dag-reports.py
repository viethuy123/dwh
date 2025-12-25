# dags/dag_staging_to_dwh.py
"""
DBT Transformation: Staging → Data Warehouse
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import DBT_PIPELINES, DEFAULT_ARGS
from factories.dbt_factory import create_dbt_transformation_task_group

# Lấy config
pipeline_config = DBT_PIPELINES['reports']

# Tạo DAG
dag = DAG(
    dag_id=pipeline_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('fct_data_completed')],  # Trigger by datasets
    catchup=False,
    dagrun_timeout=timedelta(minutes=pipeline_config['timeout_minutes']),
    description='DBT transformation from Staging to Data Warehouse',
    tags=['dbt', 'transformation', 'staging', 'warehouse']
)

with dag:
    start = EmptyOperator(task_id='start')
    transformation_group = create_dbt_transformation_task_group(dag,'dwh', pipeline_config)
    end = EmptyOperator(task_id='end', outlets=[Dataset('reports_completed')], trigger_rule='all_success')
    
    # Dependencies
    start >> transformation_group >> end