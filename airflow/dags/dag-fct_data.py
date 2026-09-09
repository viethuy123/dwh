# dags/dag_fct_data.py
"""
DBT Transformation: Fact layer
Dùng Cosmos để tự động resolve ref() dependency.
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import DBT_PIPELINES, DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories.cosmos_factory import build_layer_task_group

# Lấy config
pipeline_config = DBT_PIPELINES['fct_data']

# Tạo DAG
dag = DAG(
    dag_id=pipeline_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('dim_data_completed')],  # Trigger by datasets
    catchup=False,
    dagrun_timeout=timedelta(minutes=pipeline_config['timeout_minutes']),
    description='DBT transformation for fct layer - Cosmos auto dependency',
    tags=['dbt', 'transformation', 'fct', 'warehouse', 'cosmos']
)

with dag:
    start = EmptyOperator(task_id='start')
    transformation_group = build_layer_task_group("fct", "models/dwh/fct")
    end = EmptyOperator(task_id='end', outlets=[Dataset('fct_data_completed')], trigger_rule=DEFAULT_CHECK_DAG['trigger_rule'])

    # Dependencies
    start >> transformation_group >> end
