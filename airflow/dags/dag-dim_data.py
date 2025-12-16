# dags/dag_staging_to_dwh.py
"""
DBT Transformation: Staging → Data Warehouse
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import timedelta
from config import DBT_PIPELINES, DEFAULT_ARGS
from factories.dbt_factory import create_dbt_transformation_task_group

# Lấy config
pipeline_config = DBT_PIPELINES['dim_data']

# Tạo DAG
dag = DAG(
    dag_id=pipeline_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=pipeline_config['schedule'],  # 8:30 PM
    catchup=False,
    dagrun_timeout=timedelta(minutes=pipeline_config['timeout_minutes']),
    description='DBT transformation from Staging to Data Warehouse',
    tags=['dbt', 'transformation', 'staging', 'warehouse']
)

with dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end', trigger_rule='all_done')
    
    # DBT transformation tasks
    transformation_group = create_dbt_transformation_task_group(dag,'dwh', pipeline_config)
    
    # Dependencies

    start >> transformation_group >> end