# dags/dag-dim_data.py
"""
DBT Transformation: Dim Layer (Cosmos)

Dùng Cosmos với danh sách model từ dim_mapping để:
 - Chỉ chạy đúng các model đang active (không bị broken/disabled).
 - Cosmos tự đọc ref() giữa các model đó và xếp thứ tự chạy đúng.
   Ví dụ: dim_odoo_members sẽ chạy trước dim_hc_snapshot_month.
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories import build_cosmos_layer_group
from utils.mappings import dim_mapping

dag = DAG(
    dag_id='dag_dim_data',
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('bridge_data_completed')],
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    description='Cosmos dbt transformation for dim layer - auto dependency resolution',
    tags=['dbt', 'cosmos', 'dim', 'warehouse'],
)

with dag:
    start = EmptyOperator(task_id='start')

    # Truyền đúng danh sách model từ dim_mapping
    # Cosmos vẫn tự xây dependency graph giữa các model này dựa trên ref()
    dim_layer = build_cosmos_layer_group(
        layer_name='dim',
        select_models=list(dim_mapping.keys()),
    )

    end = EmptyOperator(
        task_id='end',
        outlets=[Dataset('dim_data_completed')],
        trigger_rule=DEFAULT_CHECK_DAG['trigger_rule'],
    )

    start >> dim_layer >> end
