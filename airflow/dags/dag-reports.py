# dags/dag-reports.py
"""
DBT Transformation: Reports Layer (Cosmos)

Chỉ chạy các model trong report_mapping - KHÔNG kéo upstream (không dùng dấu +).
Cosmos tự xây dependency graph giữa các report model nếu có ref() lẫn nhau.
Các tầng dim/fct đã được trigger qua Airflow Dataset trước đó.
"""
from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.datasets import Dataset
from datetime import timedelta
from config import DEFAULT_ARGS, DEFAULT_CHECK_DAG
from factories import build_cosmos_layer_group
from utils.mappings import report_mapping

dag = DAG(
    dag_id='dag_reports',
    default_args=DEFAULT_ARGS,
    schedule=[Dataset('fct_data_completed')],
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    description='Cosmos dbt transformation for reports layer only (no upstream rerun)',
    tags=['dbt', 'cosmos', 'reports', 'warehouse'],
)

with dag:
    start = EmptyOperator(task_id='start')

    # Chỉ chạy đúng các report trong report_mapping
    # Không có dấu + nên Cosmos không kéo dim/fct upstream vào graph
    reports_layer = build_cosmos_layer_group(
        layer_name='reports',
        select_models=list(report_mapping.keys()),
    )

    end = EmptyOperator(
        task_id='end',
        outlets=[Dataset('reports_completed')],
        trigger_rule=DEFAULT_CHECK_DAG['trigger_rule'],
    )

    start >> reports_layer >> end
