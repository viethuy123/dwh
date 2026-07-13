# dags/dag-bridge_data.py
"""
DBT Transformation: Data Warehouse Bridge Models
"""
from datetime import timedelta

from airflow.datasets import Dataset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from config import DBT_PIPELINES, DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories.dbt_factory import create_dbt_transformation_task_group


# Lấy config
pipeline_config = DBT_PIPELINES['bridge_data']


# Tạo DAG
dag = DAG(
    dag_id=pipeline_config['dag_id'],
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('staging_to_dwh_completed')],
    catchup=False,
    dagrun_timeout=timedelta(minutes=pipeline_config['timeout_minutes']),
    description='DBT transformation for bridge models in Data Warehouse',
    tags=['dbt', 'transformation', 'bridge', 'warehouse'],
)


with dag:
    start = EmptyOperator(task_id='start')
    transformation_group = create_dbt_transformation_task_group(dag, 'dwh', pipeline_config)
    end = EmptyOperator(
        task_id='end',
        outlets=[Dataset('bridge_data_completed')],
        trigger_rule=DEFAULT_CHECK_DAG['trigger_rule'],
    )

    start >> transformation_group >> end
