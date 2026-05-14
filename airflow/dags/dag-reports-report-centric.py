# dags/dag_staging_to_dwh.py
"""
DBT Transformation: Staging → Data Warehouse
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import DBT_PIPELINES, DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories.dbt_factory_report_centric import (
    create_dbt_transformation_task_group_report_centric,
    create_dbt_deps_task,
)

# Lấy config
pipeline_config = DBT_PIPELINES['reports']

# Tạo DAG
dag = DAG(
    dag_id='dag_reports_report_centric',
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('staging_to_dwh_completed')],  # Trigger by datasets
    catchup=False,
    dagrun_timeout=timedelta(minutes=pipeline_config['timeout_minutes']),
    description='DBT transformation from Staging to Data Warehouse',
    tags=['dbt', 'transformation', 'staging', 'warehouse']
)

with dag:
    start = EmptyOperator(task_id='start')
    dbt_deps = create_dbt_deps_task(dag) 
    # DBT transformation tasks
    transformation_group = create_dbt_transformation_task_group_report_centric(dag,'dwh', pipeline_config)
    end = EmptyOperator(task_id='end', outlets=[Dataset('reports_completed')], trigger_rule= DEFAULT_CHECK_DAG['trigger_rule'])
    
    # Dependencies
    start >> dbt_deps >> transformation_group >> end
