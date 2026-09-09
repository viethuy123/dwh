# dags/dag_staging_to_dwh.py
"""
DBT Transformation: Staging → Data Warehouse
Dùng Cosmos để tự động resolve ref() dependency.
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import DBT_PIPELINES, DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories.cosmos_factory import build_layer_task_group

# Lấy config
pipeline_config = DBT_PIPELINES['intermediate_mapping']

# Tạo DAG
dag = DAG(
    dag_id=pipeline_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('odoo_staging_completed')],  # Trigger by datasets
    catchup=False,
    dagrun_timeout=timedelta(minutes=pipeline_config['timeout_minutes']),
    description='DBT transformation from Staging to Data Warehouse',
    tags=['dbt', 'transformation', 'staging', 'warehouse', 'cosmos']
)

with dag:
    start = EmptyOperator(task_id='start')
    # Cosmos tự handle dbt deps + resolve ref() dependency
    transformation_group = build_layer_task_group(
        "intermediates", "models/dwh/intermediates", install_deps=True
    )
    end = EmptyOperator(task_id='end', outlets=[Dataset('staging_to_dwh_completed')], trigger_rule=DEFAULT_CHECK_DAG['trigger_rule'])

    # Dependencies
    start >> transformation_group >> end