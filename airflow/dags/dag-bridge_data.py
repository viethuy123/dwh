# dags/dag-bridge_data.py
"""
DBT Transformation: Bridge Layer (Cosmos)

Dùng Cosmos với danh sách model từ bridge_mapping.
"""
from datetime import timedelta

from airflow.datasets import Dataset
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from config import DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories import build_cosmos_layer_group
from utils.mappings import bridge_mapping

dag = DAG(
    dag_id='dag_bridge_data',
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('staging_to_dwh_completed')],
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    description='Cosmos dbt transformation for bridge models in Data Warehouse',
    tags=['dbt', 'cosmos', 'bridge', 'warehouse'],
)

with dag:
    start = EmptyOperator(task_id='start')

    bridge_layer = build_cosmos_layer_group(
        layer_name='bridge',
        select_models=list(bridge_mapping.keys()),
    )

    end = EmptyOperator(
        task_id='end',
        outlets=[Dataset('bridge_data_completed')],
        trigger_rule=DEFAULT_CHECK_DAG['trigger_rule'],
    )

    start >> bridge_layer >> end
