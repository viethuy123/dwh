# dags/dag-fct_data.py
"""
DBT Transformation: Fact Layer (Cosmos)

Dùng Cosmos với danh sách model từ fct_mapping.
Cosmos tự xây graph dependency nội bộ giữa các fct model.
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories import build_cosmos_layer_group
from utils.mappings import fct_mapping

dag = DAG(
    dag_id='dag_fct_data',
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('dim_data_completed')],
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    description='Cosmos dbt transformation for fct layer - auto dependency resolution',
    tags=['dbt', 'cosmos', 'fct', 'warehouse'],
)

with dag:
    start = EmptyOperator(task_id='start')

    fct_layer = build_cosmos_layer_group(
        layer_name='fct',
        select_models=list(fct_mapping.keys()),
    )

    end = EmptyOperator(
        task_id='end',
        outlets=[Dataset('fct_data_completed')],
        trigger_rule=DEFAULT_CHECK_DAG['trigger_rule'],
    )

    start >> fct_layer >> end
